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
        std::atomic_thread_fence(std::memory_order_acquire);
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

export
template <typename WorkItem, typename Result, typename SharedState>
struct LockfreeParallelWork
{
private:

    enum class WorkItemState : uint64_t
    {
        eFree,
        eAllocated,
        eCommitted,
    };

    using AtomicControlBlock = std::atomic<WorkItemState>;

    using RestartableWorkFunc = std::conditional_t<std::is_void_v<SharedState>,
        std::optional<Result>(*)(const WorkItem&),
        std::optional<Result>(*)(const WorkItem&, std::add_lvalue_reference_t<SharedState>)>;

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
    {
        int i = 0;
        for(const auto& item : items) {
            m_work_descs[i].m_id = i;
            m_work_descs[i].m_work = item;
            m_work_descs[i].m_ctrl.store(WorkItemState::eFree, std::memory_order_release);
            i++;
        }
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
        std::atomic_thread_fence(std::memory_order_acquire);
    }

    std::vector<Result> GetResults()
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
    LockfreeParallelWork<std::remove_pointer_t<decltype(std::ranges::data(range))>, 
                         typename function_traits<RestartableWorkFunc>::return_type::value_type,
                         void>;

} //namespace pe

