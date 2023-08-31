/*
 *  This file is part of Peredvizhnikov Engine
 *  Copyright (C) 2023 Eduard Permyakov 
 *
 *  Peredvizhnikov Engine is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Peredvizhnikov Engine is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

export module atomic_work;

import concurrency;
import shared_ptr;
import assert;
import hazard_ptr;
import logger;
import platform;
import meta;
import lockfree_iterable_list;

import <atomic>;
import <optional>;
import <vector>;
import <ranges>;
import <optional>;
import <any>;
import <span>;
import <array>;

namespace pe{

/*****************************************************************************/
/* LOCKFREE FUNCTIONAL SERIAL WORK                                           */
/*****************************************************************************/

export
template <typename State>
requires requires {
    requires (std::is_trivially_copyable_v<State>);
    requires (std::is_default_constructible_v<State>);
}
struct AtomicFunctionalSerialWork
{
private:

    struct alignas(16) ControlBlock
    {
        State   *m_prev_state;
        uint64_t m_version;

        bool operator==(const ControlBlock& rhs) const
        {
            return (m_prev_state == rhs.m_prev_state)
                && (m_version == rhs.m_version);
        }
    };

    using AtomicControlBlock = DoubleQuadWordAtomic<ControlBlock>;

    AtomicControlBlock     m_ctrl;
    HPContext<State, 1, 1> m_hp;

public:

    AtomicFunctionalSerialWork(State state)
        : m_ctrl{}
        , m_hp{}
    {
        State *copy = new State{state};
        m_ctrl.Store({copy, 0}, std::memory_order_release);
    }

    ~AtomicFunctionalSerialWork()
    {
        auto curr = m_ctrl.Load(std::memory_order_acquire);
        m_hp.RetireHazard(curr.m_prev_state);
    }

    template <typename... Args>
    State PerformSerially(void (*critical_section)(State&, Args...), Args... args)
    {
        ControlBlock curr = m_ctrl.Load(std::memory_order_acquire);
        State ret;

        while(true) {

            State *old_state = curr.m_prev_state;
            auto old_hazard = m_hp.AddHazard(0, old_state);
            if(m_ctrl.Load(std::memory_order_relaxed) != curr) {
                curr = m_ctrl.Load(std::memory_order_acquire);
                continue;
            }

            AnnotateHappensAfter(__FILE__, __LINE__, &m_ctrl);

            State *copy = new State{*old_state};
            critical_section(*copy, std::forward<Args>(args)...);
            ret = *copy;

            AnnotateHappensBefore(__FILE__, __LINE__, &m_ctrl);

            if(m_ctrl.CompareExchange(curr, {copy, curr.m_version + 1},
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
                m_hp.RetireHazard(old_state);
                break;
            }else{
                delete copy;
            }
        }
        return ret;
    }

    State GetResult()
    {
    retry:

        auto state = m_ctrl.Load(std::memory_order_acquire);
        State *last_state = state.m_prev_state;
        auto last_hazard = m_hp.AddHazard(0, last_state);
        if(m_ctrl.Load(std::memory_order_relaxed) != state)
            goto retry;

        State ret = *last_state;
        std::atomic_thread_fence(std::memory_order_release);
        return ret;
    }
};

/*****************************************************************************/
/* LOCKFREE STATEFUL SERIAL WORK                                             */
/*****************************************************************************/

export 
template <typename RequestDescriptor>
requires requires (RequestDescriptor desc) {
    {desc.Version()} -> std::same_as<uint32_t>;
}
struct AtomicStatefulSerialWork
{
private:

    struct alignas(16) Request
    {
        uint32_t           m_seqnum;
        uint32_t           m_version;
        RequestDescriptor *m_request;

        bool operator==(const Request& rhs) const
        {
            return (m_seqnum == rhs.m_seqnum)
                && (m_version == rhs.m_version)
                && (m_request == rhs.m_request);
        }
    };

    using AtomicRequest = DoubleQuadWordAtomic<Request>;

    AtomicRequest                      m_request;
    HPContext<RequestDescriptor, 1, 1> m_hp;

    bool seqnum_passed(uint32_t a, uint32_t b)
    {
        return (static_cast<int32_t>((b) - (a)) < 0);
    }

    using HazardPtr = decltype(m_hp)::hazard_ptr_type;

    std::optional<std::tuple<HazardPtr, uint32_t, uint32_t>>
    try_push_request(RequestDescriptor *desired, uint32_t version, 
        std::optional<uint32_t> seqnum) noexcept
    {
    retry:
        auto expected = m_request.Load(std::memory_order_relaxed);

        while(!expected.m_request) {

            uint32_t next_seqnum = expected.m_seqnum + 1;
            if(seqnum.has_value()) {
                next_seqnum = seqnum.value();
            }

            if(seqnum_passed(expected.m_seqnum, next_seqnum))
                return std::nullopt;

            Request newval{next_seqnum, version, desired};
            AnnotateHappensBefore(__FILE__, __LINE__, &m_request);

            auto ret = m_hp.AddHazard(0, desired);
            auto curr = m_request.Load(std::memory_order_relaxed);
            if(curr != expected)
                goto retry;

            if(m_request.CompareExchange(expected, newval,
                std::memory_order_acq_rel, std::memory_order_acquire)) {

                AnnotateHappensAfter(__FILE__, __LINE__, &m_request);
                return std::tuple{std::move(ret), newval.m_version, newval.m_seqnum};
            }
        }

        auto ret = m_hp.AddHazard(0, expected.m_request);
        auto curr = m_request.Load(std::memory_order_relaxed);
        if(curr != expected)
            goto retry;

        AnnotateHappensAfter(__FILE__, __LINE__, &m_request);
        return std::tuple{std::move(ret), expected.m_version, expected.m_seqnum};
    }

    void try_release_request(HazardPtr request, uint32_t seqnum, uint32_t version)
    {
        Request expected{seqnum, version, *request};
        m_request.CompareExchange(expected, {seqnum, 0, nullptr},
            std::memory_order_release, std::memory_order_relaxed);
    }

public:

    AtomicStatefulSerialWork()
        : m_request{}
        , m_hp{}
    {
        m_request.Store({0, 0, nullptr}, std::memory_order_release);
    }

    template <typename RestartableCriticalSection>
    requires requires (RestartableCriticalSection cs, RequestDescriptor *request, uint64_t seqnum){
        {cs(request, seqnum)} -> std::same_as<void>;
    }
    void PerformSerially(std::unique_ptr<RequestDescriptor> request, 
        RestartableCriticalSection critical_section, std::optional<uint32_t> seqnum = std::nullopt)
    {
        HazardPtr curr{m_hp};
        uint32_t version = request->Version();
        uint32_t serviced_seqnum, serviced_version;
        RequestDescriptor *serviced;
        do{
            auto req = try_push_request(request.get(), version, seqnum);
            if(!req.has_value())
                break;
            std::tie(curr, serviced_version, serviced_seqnum) = std::move(req.value());
            serviced = *curr;
            critical_section(*curr, serviced_seqnum);
            try_release_request(std::move(curr), serviced_seqnum, serviced_version);
        }while((serviced != request.get())
            || (serviced_version != version));

        m_hp.RetireHazard(request.release());
    }
};

/*****************************************************************************/
/* LOCKFREE PARALLEL WORK                                                    */
/*****************************************************************************/

template <typename T>
struct work_traits;

export
template <typename WorkItem, LockfreeIterableSetItem Result, typename SharedState>
requires (std::is_default_constructible_v<WorkItem>
       && std::is_copy_assignable_v<WorkItem>)
struct AtomicParallelWork
{
private:

    template <typename T>
    friend struct work_traits;

    enum class WorkItemState : uint64_t
    {
        eFree,
        eAllocated,
        eCommitted,
    };

    using AtomicControlBlock = std::atomic<WorkItemState>;

    using NonVoidState = std::conditional_t<std::is_void_v<SharedState>,
        std::monostate,
        SharedState>;

    using RestartableWorkFunc = std::conditional_t<std::is_void_v<SharedState>,
        std::optional<Result>(*)(uint64_t, const WorkItem&),
        std::optional<Result>(*)(uint64_t, const WorkItem&, NonVoidState&)>;

    template <typename T>
    using OptionalRef = std::conditional_t<std::is_void_v<T>,
        std::nullopt_t,
        std::optional<std::reference_wrapper<T>>>;

    struct alignas(kCacheLineSize) WorkItemDescriptor
    {
        AtomicControlBlock m_ctrl;
        uint32_t           m_id;
        WorkItem           m_work;
    };

    std::vector<WorkItemDescriptor> m_work_descs;
    RestartableWorkFunc             m_workfunc;
    OptionalRef<SharedState>        m_shared_state;
    std::atomic_int                 m_min_completed;
    LockfreeIterableSet<Result>     m_results;

    WorkItemDescriptor *allocate_free_work()
    {
        if(m_min_completed.load(std::memory_order_relaxed) == m_work_descs.size())
            return nullptr;

        for(WorkItemDescriptor& desc : m_work_descs) {
            WorkItemState curr = desc.m_ctrl.load(std::memory_order_relaxed);
            if(curr != WorkItemState::eFree)
                continue;
            if(desc.m_ctrl.compare_exchange_strong(curr, WorkItemState::eAllocated,
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
                return &desc;
            }
        }
        return nullptr;
    }

    WorkItemDescriptor *steal_incomplete_work()
    {
        if(m_min_completed.load(std::memory_order_relaxed) == m_work_descs.size())
            return nullptr;

        for(WorkItemDescriptor& desc : m_work_descs) {
            WorkItemState curr = desc.m_ctrl.load(std::memory_order_relaxed);
            if(curr == WorkItemState::eAllocated) {
                return &desc;
            }
        }
        return nullptr;
    }

    void commit_work(WorkItemDescriptor *item)
    {
        auto state = item->m_ctrl.load(std::memory_order_relaxed);
        if(state == WorkItemState::eCommitted)
            return;
        if(item->m_ctrl.compare_exchange_strong(state, WorkItemState::eCommitted,
            std::memory_order_release, std::memory_order_relaxed)) {
            m_min_completed.fetch_add(1, std::memory_order_acquire);
        }
    }
    
    template <std::ranges::input_range Range>
    AtomicParallelWork(Range items, OptionalRef<SharedState> state, RestartableWorkFunc workfunc)
        : m_work_descs{std::ranges::size(items)}
        , m_workfunc{workfunc}
        , m_shared_state{state}
        , m_min_completed{}
        , m_results{}
    {
        uint32_t i = 0;
        for(const auto& item : items) {
            m_work_descs[i].m_id = i;
            m_work_descs[i].m_work = item;
            m_work_descs[i].m_ctrl.store(WorkItemState::eFree, std::memory_order_release);
            i++;
        }
        m_min_completed.store(0, std::memory_order_release);
    }

public:

    template <std::ranges::range Range, typename State = SharedState>
    requires (!std::is_void_v<State>)
    AtomicParallelWork(Range items, State& state, RestartableWorkFunc workfunc)
        : AtomicParallelWork(items, std::ref(state), workfunc)
    {}

    template <std::ranges::range Range>
    AtomicParallelWork(Range items, RestartableWorkFunc workfunc)
        : AtomicParallelWork(items, std::nullopt, workfunc)
    {}

    AtomicParallelWork(AtomicParallelWork const&) = delete;
    AtomicParallelWork& operator=(AtomicParallelWork const&) = delete;
    AtomicParallelWork& operator=(AtomicParallelWork&&) = delete;

    AtomicParallelWork(AtomicParallelWork&& other)
        : m_work_descs{std::move(other.m_work_descs)}
        , m_workfunc{std::move(other.m_workfunc)}
        , m_shared_state{std::move(other.m_shared_state)}
        , m_min_completed{}
        , m_results{}
    {
        m_min_completed.store(0, std::memory_order_release);
    }

    void Complete(uint64_t seqnum)
    {
        while(m_min_completed.load(std::memory_order_relaxed) < m_work_descs.size()) {

            WorkItemDescriptor *curr = allocate_free_work();
            if(!curr)
                curr = steal_incomplete_work();
            if(!curr)
                break;

            std::optional<Result> result{};
            if constexpr (std::is_void_v<SharedState>) {
                result = m_workfunc(seqnum, curr->m_work);
            }else{
                result = m_workfunc(seqnum, curr->m_work, m_shared_state.value().get());
            }

            if(result.has_value() && !m_results.Find(curr->m_id)) {
                m_results.Insert(curr->m_id, result.value());
            }
            commit_work(curr);
        }
        std::atomic_thread_fence(std::memory_order_release);
    }

    std::vector<Result> GetResult(uint64_t seqnum)
    {
        Complete(seqnum);

        std::vector<std::pair<uint64_t, Result>> results = m_results.TakeSnapshot();
        std::vector<Result> ret{};
        ret.resize(results.size());
        std::transform(std::begin(results), std::end(results), std::begin(ret), 
            [](auto& r){ return r.second; });
        return ret;
    }
};

template <typename SharedState, typename RestartableWorkFunc, std::ranges::range Range>
AtomicParallelWork(Range range, SharedState& state, RestartableWorkFunc func) -> 
    AtomicParallelWork<std::remove_pointer_t<decltype(std::ranges::data(range))>, 
                         typename function_traits<RestartableWorkFunc>::return_type::value_type,
                         SharedState>;

template <typename RestartableWorkFunc, std::ranges::range Range>
AtomicParallelWork(Range range, RestartableWorkFunc func) ->
    AtomicParallelWork<std::ranges::range_value_t<Range>, 
                         typename function_traits<RestartableWorkFunc>::return_type::value_type,
                         void>;

/*****************************************************************************/
/* LOCKFREE WORK PIPELINE                                                    */
/*****************************************************************************/

template <typename T, typename U>
concept not_same_as = not std::same_as<T, U>;

template <typename... Args>
struct work_traits<AtomicParallelWork<Args...>>
{
    using input_type = std::tuple_element_t<0, std::tuple<Args...>>;
    using output_type = std::tuple_element_t<1, std::tuple<Args...>>;
    using workfunc_type = typename AtomicParallelWork<Args...>::RestartableWorkFunc;
};

template <typename S, typename F>
struct is_compatible_workfunc;

template <typename... Args, typename F>
struct is_compatible_workfunc<AtomicParallelWork<Args...>, F>
{
    constexpr static bool value = std::is_convertible_v<
        F, typename work_traits<AtomicParallelWork<Args...>>::workfunc_type>;
};

template <typename S, typename F>
inline constexpr bool is_compatible_workfunc_v = is_compatible_workfunc<S, F>::value;

template <typename A, typename B>
inline constexpr bool are_compatible_stages_v = std::is_convertible_v<
    typename work_traits<A>::output_type,
    typename work_traits<B>::input_type
>;

export
template <typename SharedState, typename... Stages>
requires (sizeof...(Stages) > 0)
struct AtomicWorkPipeline
{
private:

    struct TypeErasedPipelineStage
    {
        struct PipelineStageInterface
        {
            virtual std::span<const std::byte> GetResult(uint64_t) = 0;
            virtual ~PipelineStageInterface() = default;
        };

        PipelineStageInterface *m_stage;

        template <typename Work>
        requires requires (Work work, uint64_t seqnum){
            {work.GetResult(seqnum)} -> not_same_as<void>;
        }
        struct Wrapped : PipelineStageInterface
        {
            using func_type = decltype(&std::remove_reference_t<Work>::GetResult);
            using result_type = typename function_traits<func_type>::return_type;

            Work                               m_work;
            pe::atomic_shared_ptr<result_type> m_result;

            Wrapped(Work&& work)
                : m_work{std::forward<Work>(work)}
                , m_result{}
            {
                m_result.store(nullptr, std::memory_order_release);
            }

            ~Wrapped()
            {
                m_result.store(nullptr, std::memory_order_release);
            }

            virtual std::span<const std::byte> GetResult(uint64_t seqnum)
            {
                auto result = m_result.load(std::memory_order_acquire);
                if(result) {
                    return std::as_bytes(std::span{*result});
                }
                auto computed = pe::make_shared<result_type>(m_work.GetResult(seqnum));
                if(m_result.compare_exchange_strong(result, computed,
                    std::memory_order_release, std::memory_order_acquire)){
                    return std::as_bytes(std::span{*computed});
                }
                return std::as_bytes(std::span{*result});
            }
        };

        std::span<const std::byte> GetResult(uint64_t seqnum)
        {
            return m_stage->GetResult(seqnum);
        }

        TypeErasedPipelineStage(TypeErasedPipelineStage&&) = delete;
        TypeErasedPipelineStage(TypeErasedPipelineStage const&) = delete;
        TypeErasedPipelineStage& operator=(TypeErasedPipelineStage&&) = delete;
        TypeErasedPipelineStage& operator=(TypeErasedPipelineStage const&) = delete;

        template <typename Work>
        explicit TypeErasedPipelineStage(Work&& work)
            : m_stage{new Wrapped<Work>(std::forward<Work>(work))}
        {}

        ~TypeErasedPipelineStage()
        {
            delete m_stage;
        }
    };

    template <typename T>
    using OptionalRef = std::conditional_t<std::is_void_v<T>,
        std::nullopt_t,
        std::optional<std::reference_wrapper<T>>>;

    OptionalRef<SharedState>                m_shared_state;
    std::array<std::any, sizeof...(Stages)> m_funcs;
    std::array<pe::atomic_shared_ptr<TypeErasedPipelineStage>, sizeof...(Stages)> m_memo;

    template <std::size_t I>
    std::optional<std::reference_wrapper<TypeErasedPipelineStage>> 
    try_get_memo()
    {
        auto stage = m_memo[I].load(std::memory_order_acquire);
        if(!stage)
            return std::nullopt;
        return *stage;
    }

    template <std::size_t I>
    TypeErasedPipelineStage& try_set_memo(std::ranges::range auto&& input)
    {
        using work_type = std::tuple_element_t<I, std::tuple<Stages...>>;
        using func_type = typename work_traits<work_type>::workfunc_type;

        auto stage = m_memo[I].load(std::memory_order_acquire);
        if(!stage) {
            pe::shared_ptr<TypeErasedPipelineStage> newstage{nullptr};
            if constexpr (!std::is_void_v<SharedState>) {
                work_type work{input, m_shared_state.value().get(), 
                    std::any_cast<func_type>(m_funcs[I])};
                newstage = pe::make_shared<TypeErasedPipelineStage>(std::move(work));
            }else{
                work_type work{input, std::any_cast<func_type>(m_funcs[I])};
                newstage = pe::make_shared<TypeErasedPipelineStage>(std::move(work));
            }
            if(m_memo[I].compare_exchange_strong(stage, newstage,
                std::memory_order_release, std::memory_order_acquire)) {
                return *newstage;
            }
        }
        return *stage;
    }

    template <std::size_t I>
    static consteval bool compatible_with_prior()
    {
        return are_compatible_stages_v<
            std::tuple_element_t<I-1, std::tuple<Stages...>>,
            std::tuple_element_t<I+0, std::tuple<Stages...>>
        >;
    };

    template <>
    static consteval bool compatible_with_prior<0>()
    {
        return true;
    };

    template <std::size_t I>
    requires (compatible_with_prior<I>())
    std::ranges::range decltype(auto) lazy_eval(uint64_t seqnum)
    {
        using work_type = std::tuple_element_t<I, std::tuple<Stages...>>;
        using output_type = typename work_traits<work_type>::output_type;

        std::span<const std::byte> result;
        if(auto memo = try_get_memo<I>()) {
            result = memo.value().get().GetResult(seqnum);
        }else{
            if constexpr (I > 0) {
                auto input = lazy_eval<I-1>(seqnum);
                auto& stage = try_set_memo<I>(input);
                result = stage.GetResult(seqnum);
            }
        }

        return std::span<const output_type>{
            reinterpret_cast<const output_type*>(result.data()),
            result.size_bytes() / sizeof(output_type)
        };
    }

    template <std::ranges::input_range Input, typename... Funcs>
    requires requires (Funcs... funcs){
        requires (sizeof...(Funcs) == sizeof...(Stages));
        requires ([](){
            return is_compatible_workfunc_v<Stages, Funcs>;
        }() && ...);
    }
    explicit AtomicWorkPipeline(OptionalRef<SharedState> state, Input input, Funcs... funcs)
        : m_shared_state{state}
        , m_funcs{funcs...}
        , m_memo{}
    {
        try_set_memo<0>(input);
    }

public:

    template <std::ranges::input_range Input, typename... Funcs>
    AtomicWorkPipeline(Input input, Funcs... funcs)
        : AtomicWorkPipeline(std::nullopt, input, funcs...)
    {}

    template <std::ranges::input_range Input, typename State = SharedState, typename... Funcs>
    requires (!std::is_void_v<State>)
    AtomicWorkPipeline(Input input, State& state, Funcs... funcs)
        : AtomicWorkPipeline(std::optional{std::ref(state)}, input, funcs...)
    {}

    ~AtomicWorkPipeline()
    {
        for(int i = 0; i < m_memo.size(); i++) {
            m_memo[i].store(nullptr, std::memory_order_relaxed);
        }
    }

    void Complete(uint64_t seqnum)
    {
        lazy_eval<sizeof...(Stages)-1>(seqnum);
    }

    std::ranges::range decltype(auto) GetResult(uint64_t seqnum)
    {
        return lazy_eval<sizeof...(Stages)-1>(seqnum);
    }
};

} //namespace pe

