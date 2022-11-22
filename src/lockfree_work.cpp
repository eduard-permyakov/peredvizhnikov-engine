export module lockfree_work;

import concurrency;
import shared_ptr;
import assert;
import hazard_ptr;
import logger;
import platform;
import meta;

import <atomic>;
import <optional>;
import <vector>;
import <ranges>;

namespace pe{

/*****************************************************************************/
/* LOCKFREE SERIAL WORK                                                      */
/*****************************************************************************/

export
template <typename State>
requires requires {
    requires (std::is_trivially_copyable_v<State>);
    requires (std::is_default_constructible_v<State>);
}
struct LockfreeSerialWork
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

    LockfreeSerialWork(State state)
        : m_ctrl{}
        , m_hp{}
    {
        State *copy = new State;
        std::memcpy(copy, &state, sizeof(State));
        m_ctrl.Store({copy, 0}, std::memory_order_release);
    }

    ~LockfreeSerialWork()
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
/* LOCkFREE PARALLEL WORK                                                    */
/*****************************************************************************/

export
template <std::size_t Size, typename WorkItem, typename SharedState>
struct LockfreeParallelWork
{
private:

    enum class WorkItemState : uint64_t
    {
        eFree,
        eAllocated,
        eCommitted,
    };

    struct alignas(16) ControlBlock
    {
        WorkItemState m_state;
        WorkItem     *m_item;
    };

    using AtomicControlBlock = DoubleQuadWordAtomic<ControlBlock>;
    using WorkFunc = void(*)(const WorkItem&, SharedState&);

    struct alignas(kCacheLineSize) WorkItemDescriptor
    {
        AtomicControlBlock m_ctrl;
        WorkItem           m_work;
    };

    std::array<WorkItemDescriptor, Size> m_work_descs;
    WorkFunc                             m_workfunc;
    std::reference_wrapper<SharedState>  m_shared_state;
    std::atomic_int                      m_min_completed;

    WorkItemDescriptor *allocate_free_work()
    {
        if(m_min_completed.load(std::memory_order_relaxed) == m_work_descs.size())
            return nullptr;

        for(WorkItemDescriptor& desc : m_work_descs) {
            ControlBlock curr = desc.m_ctrl.Load(std::memory_order_relaxed);
            if(curr.m_state != WorkItemState::eFree)
                continue;
            if(desc.m_ctrl.CompareExchange(curr, {WorkItemState::eAllocated, curr.m_item},
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
            ControlBlock curr = desc.m_ctrl.Load(std::memory_order_relaxed);
            if(curr.m_state == WorkItemState::eAllocated) {
                return &desc;
            }
        }
        return nullptr;
    }

    void commit_work(WorkItemDescriptor *item)
    {
        auto ctrl = item->m_ctrl.Load(std::memory_order_relaxed);
        if(ctrl.m_state == WorkItemState::eCommitted)
            return;
        if(item->m_ctrl.CompareExchange(ctrl, {WorkItemState::eCommitted, ctrl.m_item},
            std::memory_order_release, std::memory_order_relaxed)) {
            m_min_completed.fetch_add(1, std::memory_order_acquire);
        }
    }

public:

    template <std::ranges::range Range>
    LockfreeParallelWork(Range items, SharedState& state, WorkFunc workfunc)
        : m_work_descs{}
        , m_workfunc{workfunc}
        , m_shared_state{std::ref(state)}
        , m_min_completed{}
    {
        int i = 0;
        for(const auto& item : items) {
            m_work_descs[i].m_work = item;
            m_work_descs[i].m_ctrl.Store({
                WorkItemState::eFree,
                &m_work_descs.back().m_work
            });
            i++;
        }
    }

    void Complete()
    {
        while(m_min_completed.load(std::memory_order_relaxed) < m_work_descs.size()) {

            WorkItemDescriptor *curr = allocate_free_work();
            if(!curr)
                curr = steal_incomplete_work();
            if(!curr)
                break;

            m_workfunc(curr->m_work, m_shared_state.get());
            commit_work(curr);
        }
        std::atomic_thread_fence(std::memory_order_release);
    }
};

template <typename SharedState, typename WorkFunc, std::ranges::range Range>
LockfreeParallelWork(Range range, SharedState& state, WorkFunc func) -> 
    LockfreeParallelWork<std::ranges::size(range), 
                         std::remove_pointer_t<decltype(std::ranges::data(range))>, 
                         SharedState>;

/*****************************************************************************/
/* MULTIPLE PRODUCER SINGLE CONSUMER GUARD                                   */
/*****************************************************************************/

/* An atomic primitive which guards access to some lockfree 
 * resource. Users of the resource (producers and consumers) 
 * acquire their corresponding access to the resource via the 
 * atomic guard object. The guard will serialize access of 
 * the consumers to the resource. If the request is able to
 * be implemented in a lock-free fashion, then the entire
 * resource access will be lock-free.
 * 
 * Consumer access is serialized by an atomic CAS loop that 
 * will either succeed in acquiring resource access, or 
 * succeed in obtaining a descriptor of the currently serviced
 * request. The consumer can then help complete the request
 * and retry acquiring the resource.
 *
 * This is a generic mechanism that allows threads to "help"
 * service some request, thereby speeding up its' completion,
 * instread of getting blocked.
 */

export
template <typename Resource, typename RequestDescriptor>
struct MPSCGuard
{
private:

    std::atomic<Resource*>                   m_resource;
    pe::atomic_shared_ptr<RequestDescriptor> m_request;

public:

    MPSCGuard()
        : m_resource{}
        , m_request{}
    {
        m_resource.store(nullptr, std::memory_order_release);
        m_request.store(nullptr, std::memory_order_release);
    }

    MPSCGuard(Resource *resource)
        : m_resource{}
        , m_request{}
    {
        m_resource.store(resource, std::memory_order_release);
        m_request.store(nullptr, std::memory_order_release);
    }

    Resource *AcquireProducerAccess() const noexcept
    {
        return m_resource.load(std::memory_order_acquire);
    }

    void ReleaseProducerAccess() const noexcept
    {
        /* no-op */
    }

    std::pair<pe::shared_ptr<RequestDescriptor>, Resource*> 
    TryAcquireConsumerAccess(pe::shared_ptr<RequestDescriptor> desired) noexcept
    {
        Resource *resource = m_resource.load(std::memory_order_acquire);
        pe::shared_ptr<RequestDescriptor> expected{nullptr};

        while(!expected) {
            if(m_request.compare_exchange_strong(expected, desired,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
                return {desired, resource};
            }
        }

        return {expected, resource};
    }

    bool TryReleaseConsumerAccess(pe::shared_ptr<RequestDescriptor> request) noexcept
    {
        return m_request.compare_exchange_strong(request, nullptr,
            std::memory_order_relaxed, std::memory_order_relaxed);
    }
};

} //namespace pe

