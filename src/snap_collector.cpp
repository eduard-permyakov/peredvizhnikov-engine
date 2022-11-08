export module snap_collector;

import tls;
import logger;
import assert;
import lockfree_list;

import <thread>;
import <atomic>;
import <set>;
import <memory>;

namespace pe{

/* Allows taking multiple concurrent linearizable snapshots
 * of a lockfree set data structure supporting Insert, Delete and 
 * Contains operations. Based on the paper "Lock-Free Data-Structure 
 * Iterators" by Erez Petrank and Shahar Timnat and prior works.
 */
export
template <typename Node, typename T>
class SnapCollector
{
public:

    struct Report
    {
        enum class Type
        {
            eInsert,
            eDelete
        };
        Node *m_node;
        T     m_value;
        Type  m_type;
    };

    struct NodeDescriptor
    {
        Node *m_ptr;
        T     m_value;

        std::strong_ordering operator<=>(const NodeDescriptor& rhs) const noexcept
        {
            return (m_value <=> rhs.m_value);
        }
    };

    void AddNode(Node *node, T value);
    void Report(struct Report report);
    bool IsActive() const;
    void BlockFurtherNodes();
    void Deactivate();
    void BlockFurtherReports();
    std::set<NodeDescriptor> ReadPointers() const;
    std::vector<struct Report> ReadReports() const;

    SnapCollector(bool active);

    static inline SnapCollector<Node, T>* Dummy()
    {
        static auto s_dummy = std::make_unique<SnapCollector<Node, T>>(false);
        return s_dummy.get();
    }

    SnapCollector(SnapCollector&&) = delete;
    SnapCollector(SnapCollector const&) = delete;
    SnapCollector& operator=(SnapCollector&&) = delete;
    SnapCollector& operator=(SnapCollector const&) = delete;

private:

    struct ThreadLocalContext
    {
        ThreadLocalContext         *m_next;
        std::atomic_flag            m_active;
        std::vector<struct Report>  m_reports;
    };

    /* For a first pass of the implementation, we are limited to just 
     * a single iterator thread. Later extend this logic to hold a 
     * lockfree list to hold the nodes and allow multiple threads to 
     * add nodes.
     */
    std::set<NodeDescriptor>          m_nodes;
    std::atomic_flag                  m_active;
    std::atomic_flag                  m_nodes_blocked;
    std::atomic_flag                  m_reports_blocked;
    TLSAllocation<ThreadLocalContext> m_tls;
};

template <typename Node, typename T>
SnapCollector<Node, T>::SnapCollector(bool active)
    : m_nodes{}
    , m_active{active}
    , m_nodes_blocked{false}
    , m_reports_blocked{false}
    , m_tls{AllocTLS<ThreadLocalContext>(false)}
{}

template <typename Node, typename T>
void SnapCollector<Node, T>::AddNode(Node *node, T value)
{
    if(m_nodes_blocked.test(std::memory_order_relaxed))
        return;
    m_nodes.insert({node, value});
}

template <typename Node, typename T>
void SnapCollector<Node, T>::Report(struct Report report)
{
    if(m_reports_blocked.test(std::memory_order_relaxed))
        return;

    auto ctx = m_tls.GetThreadSpecific();
    ctx->m_active.test_and_set(std::memory_order_relaxed);
    ctx->m_reports.push_back(report);
    ctx->m_active.clear(std::memory_order_release);
}

template <typename Node, typename T>
bool SnapCollector<Node, T>::IsActive() const
{
    return m_active.test(std::memory_order_relaxed);
}

template <typename Node, typename T>
void SnapCollector<Node, T>::BlockFurtherNodes()
{
    m_nodes_blocked.test_and_set(std::memory_order_relaxed);
}

template <typename Node, typename T>
void SnapCollector<Node, T>::Deactivate()
{
    m_active.clear(std::memory_order_relaxed);
}

template <typename Node, typename T>
void SnapCollector<Node, T>::BlockFurtherReports()
{
    m_reports_blocked.test_and_set(std::memory_order_relaxed);
}

template <typename Node, typename T>
std::set<typename SnapCollector<Node, T>::NodeDescriptor> 
SnapCollector<Node, T>::ReadPointers() const
{
    pe::assert(!m_active.test(std::memory_order_relaxed));
    pe::assert(m_nodes_blocked.test(std::memory_order_relaxed));
    pe::assert(m_reports_blocked.test(std::memory_order_relaxed));
    return m_nodes;
}

template <typename Node, typename T>
std::vector<struct SnapCollector<Node, T>::Report> SnapCollector<Node, T>::ReadReports() const
{
    pe::assert(!m_active.test(std::memory_order_relaxed));
    pe::assert(m_nodes_blocked.test(std::memory_order_relaxed));
    pe::assert(m_reports_blocked.test(std::memory_order_relaxed));

    std::vector<struct Report> ret{};
    auto all_contexts = m_tls.GetThreadPtrsSnapshot();
    for(auto ctx : all_contexts) {
        if(auto ptr = ctx.lock()) {
            while(ptr->m_active.test(std::memory_order_acquire));
            ret.insert(std::end(ret), std::begin(ptr->m_reports), std::end(ptr->m_reports));
        }
    }
    return ret;
}

}; //namespace pe

