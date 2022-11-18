export module wait_free_serial_work;

import concurrency;
import shared_ptr;
import assert;

import <atomic>;
import <optional>;

namespace pe{

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

