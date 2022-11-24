export module lockfree_work;

import concurrency;
import shared_ptr;
import assert;
import hazard_ptr;
import logger;
import platform;
import meta;
import iterable_lockfree_list;

import <atomic>;
import <optional>;
import <vector>;
import <ranges>;
import <optional>;
import <any>;
import <span>;

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
struct LockfreeFunctionalSerialWork
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

    LockfreeFunctionalSerialWork(State state)
        : m_ctrl{}
        , m_hp{}
    {
        State *copy = new State;
        std::memcpy(copy, &state, sizeof(State));
        m_ctrl.Store({copy, 0}, std::memory_order_release);
    }

    ~LockfreeFunctionalSerialWork()
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

            State *copy = new State;
            std::memcpy(copy, old_state, sizeof(State));
            critical_section(*copy, std::forward<Args>(args)...);
            ret = *copy;

            AnnotateHappensBefore(__FILE__, __LINE__, &m_ctrl);

            if(m_ctrl.CompareExchange(curr, {copy, curr.m_version + 1},
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
                m_hp.RetireHazard(old_state);
                break;
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
struct LockfreeStatefulSerialWork
{
private:

    pe::atomic_shared_ptr<RequestDescriptor> m_request;

    pe::shared_ptr<RequestDescriptor> try_push_request(
        pe::shared_ptr<RequestDescriptor> desired) noexcept
    {
        pe::shared_ptr<RequestDescriptor> expected{nullptr};

        while(!expected) {
            if(m_request.compare_exchange_strong(expected, desired,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
                return desired;
            }
        }

        return expected;
    }

    bool try_release_request(pe::shared_ptr<RequestDescriptor> request)
    {
        return m_request.compare_exchange_strong(request, nullptr,
            std::memory_order_release, std::memory_order_relaxed);
    }

public:

    LockfreeStatefulSerialWork()
        : m_request{}
    {
        m_request.store(nullptr, std::memory_order_release);
    }

    template <typename RestartableCriticalSection>
    requires requires (RestartableCriticalSection cs, pe::shared_ptr<RequestDescriptor> request){
        {cs(request)} -> std::same_as<void>;
    }
    void PerformSerially(pe::shared_ptr<RequestDescriptor> request, 
        RestartableCriticalSection critical_section)
    {
        pe::shared_ptr<RequestDescriptor> curr;
        do{
            curr = try_push_request(request);
            critical_section(curr);
            try_release_request(curr);
        }while(curr != request);
    }
};

/*****************************************************************************/
/* LOCKFREE PARALLEL WORK                                                    */
/*****************************************************************************/

template <typename T>
struct work_traits;

export
template <typename WorkItem, typename Result, typename SharedState>
struct LockfreeParallelWork
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
        std::optional<Result>(*)(const WorkItem&),
        std::optional<Result>(*)(const WorkItem&, NonVoidState&)>;

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

    struct TaggedResult
    {
        uint32_t m_id;
        Result   m_result;

        bool operator==(const TaggedResult& rhs) const
        {
            return (m_id == rhs.m_id);
        }

        std::strong_ordering operator<=>(const TaggedResult& rhs) const
        {
            return (m_id <=> rhs.m_id);
        }
    };

    std::vector<WorkItemDescriptor>     m_work_descs;
    RestartableWorkFunc                 m_workfunc;
    OptionalRef<SharedState>            m_shared_state;
    std::atomic_int                     m_min_completed;
    IterableLockfreeList<TaggedResult>  m_results;

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
    
    template <std::ranges::range Range>
    LockfreeParallelWork(Range items, OptionalRef<SharedState> state, RestartableWorkFunc workfunc)
        : m_work_descs{std::ranges::size(items)}
        , m_workfunc{workfunc}
        , m_shared_state{state}
        , m_min_completed{}
        , m_results{}
    {
        int i = 0;
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
    LockfreeParallelWork(Range items, State& state, RestartableWorkFunc workfunc)
        : LockfreeParallelWork(items, std::ref(state), workfunc)
    {}

    template <std::ranges::range Range>
    LockfreeParallelWork(Range items, RestartableWorkFunc workfunc)
        : LockfreeParallelWork(items, std::nullopt, workfunc)
    {}

    LockfreeParallelWork(LockfreeParallelWork const&) = delete;
    LockfreeParallelWork& operator=(LockfreeParallelWork const&) = delete;
    LockfreeParallelWork& operator=(LockfreeParallelWork&&) = delete;

    LockfreeParallelWork(LockfreeParallelWork&& other)
        : m_work_descs{std::move(other.m_work_descs)}
        , m_workfunc{std::move(other.m_workfunc)}
        , m_shared_state{std::move(other.m_shared_state)}
        , m_min_completed{}
        , m_results{}
    {
        m_min_completed.store(0, std::memory_order_release);
    }

    void Complete()
    {
        while(m_min_completed.load(std::memory_order_relaxed) < m_work_descs.size()) {

            WorkItemDescriptor *curr = allocate_free_work();
            if(!curr)
                curr = steal_incomplete_work();
            if(!curr)
                break;

            std::optional<Result> result{};
            if constexpr (std::is_void_v<SharedState>) {
                result = m_workfunc(curr->m_work);
            }else{
                result = m_workfunc(curr->m_work, m_shared_state.value().get());
            }

            if(result.has_value()) {
                m_results.Insert({curr->m_id, result.value()});
            }
            commit_work(curr);
        }
        std::atomic_thread_fence(std::memory_order_release);
    }

    std::vector<Result> GetResult()
    {
        Complete();

        std::vector<TaggedResult> results = m_results.TakeSnapshot();
        std::vector<Result> ret{};
        ret.resize(results.size());
        std::transform(std::begin(results), std::end(results), std::begin(ret), 
            [](TaggedResult r){ return r.m_result; });
        return ret;
    }
};

template <typename SharedState, typename RestartableWorkFunc, std::ranges::range Range>
LockfreeParallelWork(Range range, SharedState& state, RestartableWorkFunc func) -> 
    LockfreeParallelWork<std::remove_pointer_t<decltype(std::ranges::data(range))>, 
                         typename function_traits<RestartableWorkFunc>::return_type::value_type,
                         SharedState>;

template <typename RestartableWorkFunc, std::ranges::range Range>
LockfreeParallelWork(Range range, RestartableWorkFunc func) ->
    LockfreeParallelWork<std::ranges::range_value_t<Range>, 
                         typename function_traits<RestartableWorkFunc>::return_type::value_type,
                         void>;

/*****************************************************************************/
/* LOCKFREE WORK PIPELINE                                                    */
/*****************************************************************************/

template <typename T, typename U>
concept not_same_as = not std::same_as<T, U>;

template <typename... Args>
struct work_traits<LockfreeParallelWork<Args...>>
{
    using input_type = std::tuple_element_t<0, std::tuple<Args...>>;
    using output_type = std::tuple_element_t<1, std::tuple<Args...>>;
    using workfunc_type = typename LockfreeParallelWork<Args...>::RestartableWorkFunc;
};

template <typename S, typename F>
struct is_compatible_workfunc;

template <typename... Args, typename F>
struct is_compatible_workfunc<LockfreeParallelWork<Args...>, F>
{
    constexpr static bool value = std::is_convertible_v<
        F, typename work_traits<LockfreeParallelWork<Args...>>::workfunc_type>;
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
struct LockfreeWorkPipeline
{
private:

    struct TypeErasedPipelineStage
    {
        struct PipelineStageInterface
        {
            virtual std::span<const std::byte> GetResult() = 0;
            virtual ~PipelineStageInterface() = default;
        };

        PipelineStageInterface *m_stage;

        template <typename Work>
        requires requires (Work work){
            {work.GetResult()} -> not_same_as<void>;
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

            virtual std::span<const std::byte> GetResult()
            {
                auto result = m_result.load(std::memory_order_acquire);
                if(result) {
                    return std::as_bytes(std::span{*result});
                }
                auto computed = pe::make_shared<result_type>(m_work.GetResult());
                if(m_result.compare_exchange_strong(result, computed,
                    std::memory_order_release, std::memory_order_acquire)){
                    return std::as_bytes(std::span{*computed});
                }
                return std::as_bytes(std::span{*result});
            }
        };

        std::span<const std::byte> GetResult()
        {
            return m_stage->GetResult();
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

    OptionalRef<SharedState> m_shared_state;
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
    std::ranges::range decltype(auto) lazy_eval()
    {
        using work_type = std::tuple_element_t<I, std::tuple<Stages...>>;
        using output_type = typename work_traits<work_type>::output_type;

        std::span<const std::byte> result;
        if(auto memo = try_get_memo<I>()) {
            result = memo.value().get().GetResult();
        }else{
            if constexpr (I > 0) {
                auto input = lazy_eval<I-1>();
                auto& stage = try_set_memo<I>(input);
                result = stage.GetResult();
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
    explicit LockfreeWorkPipeline(OptionalRef<SharedState> state, Input input, Funcs... funcs)
        : m_shared_state{state}
        , m_funcs{funcs...}
        , m_memo{}
    {
        try_set_memo<0>(input);
    }

public:

    template <std::ranges::input_range Input, typename... Funcs>
    LockfreeWorkPipeline(Input input, Funcs... funcs)
        : LockfreeWorkPipeline(std::nullopt, input, funcs...)
    {}

    template <std::ranges::input_range Input, typename State = SharedState, typename... Funcs>
    requires (!std::is_void_v<State>)
    LockfreeWorkPipeline(Input input, State& state, Funcs... funcs)
        : LockfreeWorkPipeline(std::ref(state), input, funcs...)
    {}

    ~LockfreeWorkPipeline()
    {
        for(int i = 0; i < m_memo.size(); i++) {
            m_memo[i].store(nullptr, std::memory_order_relaxed);
        }
    }

    void Complete()
    {
        lazy_eval<sizeof...(Stages)-1>();
    }

    std::ranges::range decltype(auto) GetResult()
    {
        return lazy_eval<sizeof...(Stages)-1>();
    }
};

} //namespace pe

