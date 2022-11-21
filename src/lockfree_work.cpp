export module lockfree_work;

import concurrency;
import shared_ptr;
import assert;
import hazard_ptr;
import logger;
import platform;

import <atomic>;
import <optional>;
import <vector>;

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

        return *last_state;
    }
};

/*****************************************************************************/
/* LOCkFREE PARALLEL WORK                                                    */
/*****************************************************************************/

export
template <typename WorkItem>
struct LockfreeParallelWork
{
private:

    enum class WorkItemState : uint64_t
    {
        eFree,
        eAllocated,
        eStolen,
        eCommitted,
    };

    struct alignas(16) ControlBlock
    {
        WorkItemState m_ctrl;
        WorkItem     *m_item;
    };

    using AtomicControlBlock = DoubleQuadWordAtomic<ControlBlock>;
    using WorkFunc = void(*)(WorkItem&);

    struct WorkItemDescriptor
    {
        WorkItem           m_work;
        AtomicControlBlock m_ctrl;
    };

    std::vector<WorkItemDescriptor> m_work_descs;
    WorkFunc                        m_workfunc;

public:

    LockfreeParallelWork(std::vector<WorkItem> items, WorkFunc workfunc)
        : m_work_descs{}
        , m_workfunc{workfunc}
    {
        m_work_descs.reserve(items.size());
        for(const auto& item : items) {
            m_work_descs.emplace_back(item, {});
            m_work_descs.back().m_ctrl.Store(
                WorkItemState::eFree,
                &m_work_descs.back().m_work
            );
        }
    }

    void Complete()
    {
        // while(!done) {
            // 1. allocate a free work item
            // 2. if could not allocate free item, steal one
            // 3. perform the work
        //}
    }
};

/*****************************************************************************/
/* MULTIPLE PRODUCER SINGLE CONSUMER GUARD                                   */
/*****************************************************************************/

/* An atomic primitive which guards access to some lockfree 
 * resource. Users of the resource (producers and consumers) 
 * acquire their corresponding access to the resource via the 
 * atomic guard object. The guard will serialize access of 
 * the consumers to the resource. Ultimately this is not 
 * strictly-speaking lock-free a a consumer can block other
 * other threads from advancing if it is scheduled out by the
 * OS before it can release the resource.
 * 
 * Producer access to the resource is never restricted. 
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

        if(m_request.compare_exchange_strong(expected, desired,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
            return {desired, resource};
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

