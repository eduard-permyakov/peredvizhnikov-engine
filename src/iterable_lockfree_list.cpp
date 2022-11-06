export module iterable_lockfree_list;

import platform;
import concurrency;
import logger;
import snap_collector;

import <atomic>;
import <concepts>;
import <memory>;
import <utility>;
import <vector>;

namespace pe{

template <typename T>
concept LockfreeListItem = requires{
    requires (std::is_default_constructible_v<T>);
    requires (std::is_copy_constructible_v<T>);
    requires (std::is_copy_assignable_v<T>);
    requires (std::equality_comparable<T>);
    requires (std::three_way_comparable<T>);
};

/* Same as the LockfreeList, but allows multiple threads to 
 * concurrently iterate on snapshots of the list. The snapshots 
 * are guaranteed to be linearizable (i.e. every snapshot is 
 * some valid state between two atomic operations (Insert,
 * Delete, Find).
 */
export
template <LockfreeListItem T, int Tag>
class IterableLockfreeList
{
private:

    template <typename U>
    using AtomicPointer = std::atomic<U*>;

    struct Node
    {
        T                   m_value;
        AtomicPointer<Node> m_next;
    };

    static_assert(sizeof(AtomicPointer<Node>) == sizeof(Node*));
    static_assert(AtomicPointer<Node>::is_always_lock_free);

    using SCPointer = SnapCollector<Node, T>*;
    using AtomicSCPointer = AtomicPointer<SnapCollector<Node, T>>;

    Node                                         *m_head;
    Node                                         *m_tail;
    HPContext<Node, 2, 2, Tag>&                   m_hp;
    HPContext<SnapCollector<Node, T>, 1, 1, Tag>& m_schp;
    AtomicSCPointer                               m_psc;

    IterableLockfreeList();

    IterableLockfreeList(IterableLockfreeList&&) = delete;
    IterableLockfreeList(IterableLockfreeList const&) = delete;
    IterableLockfreeList& operator=(IterableLockfreeList&&) = delete;
    IterableLockfreeList& operator=(IterableLockfreeList const&) = delete;

    inline bool is_marked_reference(Node *next) const
    {
        return (reinterpret_cast<uintptr_t>(next) & 0x1) == 0x1;
    }

    inline Node *get_marked_reference(Node *next) const
    {
        return reinterpret_cast<Node*>(reinterpret_cast<uintptr_t>(next) | 0x1);
    }

    inline Node* get_unmarked_reference(Node *next) const
    {
        return reinterpret_cast<Node*>(reinterpret_cast<uintptr_t>(next) & ~0x1);
    }

    /* Returns the left and right nodes for a to-be-inserted value. 
     * The boolean indicates that a node with the value already 
     * exists in the list.
     */
    std::tuple<bool, Node*, Node*> search(const T& value);

    void report_delete(Node *victim, T value);
    void report_insert(Node *new_node, T value);
    SCPointer acquire_snap_collector();
    void collect_snapshot(SCPointer sc);
    std::vector<T> reconstruct_using_reports(SCPointer sc);

public:

    static inline IterableLockfreeList& Instance()
    {
        static IterableLockfreeList<T, Tag> s_instance{};
        return s_instance;
    }

    ~IterableLockfreeList();

    template <typename U = T>
    requires (std::is_constructible_v<T, U>)
    bool Insert(U&& value);
    bool Delete(const T& value);
    bool Find(const T& value);

    std::vector<T> TakeSnapshot();
};

template <LockfreeListItem T, int Tag>
IterableLockfreeList<T, Tag>::IterableLockfreeList()
    : m_head{}
    , m_tail{}
    , m_hp{HPContext<Node, 2, 2, Tag>::Instance()}
    , m_schp{HPContext<SnapCollector<Node, T>, 1, 1, Tag>::Instance()}
    , m_psc{SnapCollector<Node, T>::Dummy()}
{
    std::unique_ptr<Node> tail{new Node{{}, nullptr}};
    std::unique_ptr<Node> head{new Node{{}, tail.get()}};

    m_head = head.release();
    m_tail = tail.release();
}

template <LockfreeListItem T, int Tag>
std::tuple<
    bool, 
    typename IterableLockfreeList<T, Tag>::Node*, 
    typename IterableLockfreeList<T, Tag>::Node*
>
IterableLockfreeList<T, Tag>::search(const T& value)
{
retry:

    Node *prev = m_head;
    Node *curr = prev->m_next.load(std::memory_order_acquire);
    Node *pprev = nullptr;

    while(curr) {

        auto prev_hazard = m_hp.AddHazard(0, prev);
        if(pprev && prev != pprev->m_next.load(std::memory_order_relaxed)) {
            /* We bumped into a node that may have already been deleted 
             * We are forced to restart from the head of the list. 
             */
            goto retry;
        }

        auto curr_hazard = m_hp.AddHazard(1, curr);
        if(curr != prev->m_next.load(std::memory_order_relaxed))
            goto retry;

        /* Now 'prev' and 'curr' are safe to dereference */

        Node *next = curr->m_next.load(std::memory_order_acquire);
        if(is_marked_reference(next)) {
            if(!prev->m_next.compare_exchange_strong(curr, get_unmarked_reference(next),
                std::memory_order_release, std::memory_order_relaxed)) {
                goto retry;
            }
            report_delete(curr, curr->m_value);
            m_hp.RetireHazard(curr);
            curr = get_unmarked_reference(next);
        }else{
            if(curr != prev->m_next.load(std::memory_order_acquire))
                goto retry;
            if(curr->m_value >= value) {
                bool exists = (curr != m_tail) && curr->m_value == value;
                if(exists) {
                    report_insert(curr, curr->m_value);
                }
                return {exists, prev, curr};
            }

            pprev = prev;
            prev = curr;
            curr = next;
        }
    }
    return {false, pprev, prev};
}

template <LockfreeListItem T, int Tag>
IterableLockfreeList<T, Tag>::~IterableLockfreeList()
{
    Node *curr = m_head;
    while((curr = m_head->m_next.load(std::memory_order_relaxed)) != m_tail) {
        Delete(curr->m_value);
    }
    delete m_head;
    delete m_tail;
}

template <LockfreeListItem T, int Tag>
template <typename U>
requires (std::is_constructible_v<T, U>)
bool IterableLockfreeList<T, Tag>::Insert(U&& value)
{
    Node *new_node = new Node{std::forward<U>(value), nullptr};

    do{
        auto [exists, left_node, right_node] = search(value);
        if(exists) {
            report_insert(new_node, new_node->m_value);
            delete new_node;
            return false;
        }

        new_node->m_next.store(right_node, std::memory_order_relaxed);
        if(left_node->m_next.compare_exchange_strong(right_node, new_node,
            std::memory_order_release, std::memory_order_relaxed)) {    

            report_insert(new_node, new_node->m_value);
            return true;
        }

    }while(true);
}

template <LockfreeListItem T, int Tag>
bool IterableLockfreeList<T, Tag>::Delete(const T& value)
{
    bool exists;
    Node *left_node, *right_node, *right_node_next;
    do{
        std::tie(exists, left_node, right_node) = search(value);
        if(!exists)
            return false;

        right_node_next = right_node->m_next.load(std::memory_order_acquire);
        if(!is_marked_reference(right_node_next)) {
            if(right_node->m_next.compare_exchange_strong(right_node_next,
                get_marked_reference(right_node_next),
                std::memory_order_release, std::memory_order_relaxed))
                break;
        }
    }while(true);

    if(!left_node->m_next.compare_exchange_strong(right_node, right_node_next,
        std::memory_order_release, std::memory_order_relaxed)) {

        /* We could not delete the node we just marked:
         * traverse the list and delete it 
         */
        search(right_node->m_value);
    }else{
        report_delete(right_node, right_node->m_value);
        m_hp.RetireHazard(right_node);
    }
    return true;
}

template <LockfreeListItem T, int Tag>
bool IterableLockfreeList<T, Tag>::Find(const T& value)
{
    auto [exists, left_node, right_node] = search(value);
    return exists;
}

template <LockfreeListItem T, int Tag>
void IterableLockfreeList<T, Tag>::report_delete(
    typename IterableLockfreeList<T, Tag>::Node *victim, T value)
{
retry:
    SCPointer sc = m_psc.load(std::memory_order_acquire);
    auto sc_hazard = m_schp.AddHazard(0, sc);
    if(sc != m_psc.load(std::memory_order_relaxed))
        goto retry;

    if(sc->IsActive()) {
        sc->Report({victim, value, SnapCollector<Node, T>::Report::Type::eDelete});
    }
}

template <LockfreeListItem T, int Tag>
void IterableLockfreeList<T, Tag>::report_insert(
    typename IterableLockfreeList<T, Tag>::Node *new_node, T value)
{
retry:
    SCPointer sc = m_psc.load(std::memory_order_acquire);
    auto sc_hazard = m_schp.AddHazard(0, sc);
    if(sc != m_psc.load(std::memory_order_relaxed))
        goto retry;

    if(sc->IsActive() && !is_marked_reference(new_node)) {
        sc->Report({new_node, value, SnapCollector<Node, T>::Report::Type::eInsert});
    }
}

template <LockfreeListItem T, int Tag>
typename IterableLockfreeList<T, Tag>::SCPointer
IterableLockfreeList<T, Tag>::acquire_snap_collector()
{
retry:
    SCPointer sc = m_psc.load(std::memory_order_acquire);
    auto sc_hazard = m_schp.AddHazard(0, sc);
    if(sc != m_psc.load(std::memory_order_relaxed))
        goto retry;

    if(sc->IsActive())
        return sc;

    SCPointer new_sc = new SnapCollector<Node, T>{true};
    if(!m_psc.compare_exchange_strong(sc, new_sc,
        std::memory_order_release, std::memory_order_relaxed)) {
        delete new_sc;
    }else if(sc != SnapCollector<Node, T>::Dummy()) {
        m_schp.RetireHazard(sc);
    }

    return m_psc.load(std::memory_order_acquire);
}

template <LockfreeListItem T, int Tag>
void IterableLockfreeList<T, Tag>::collect_snapshot(
    typename IterableLockfreeList<T, Tag>::SCPointer sc)
{
retry:

    Node *prev = m_head;
    Node *curr = prev->m_next.load(std::memory_order_acquire);
    Node *pprev = nullptr;

    while(sc->IsActive()) {

        if(!curr) {
            sc->BlockFurtherNodes();
            sc->Deactivate();
            break;
        }

        auto prev_hazard = m_hp.AddHazard(0, prev);
        if(pprev && prev != pprev->m_next.load(std::memory_order_relaxed)) {
            /* We bumped into a node that may have already been deleted 
             * We are forced to restart from the head of the list. 
             */
            goto retry;
        }

        auto curr_hazard = m_hp.AddHazard(1, curr);
        if(curr != prev->m_next.load(std::memory_order_relaxed))
            goto retry;

        /* Now 'prev' and 'curr' are safe to dereference */

        if(curr != m_tail && !is_marked_reference(curr)) {
            sc->AddNode(curr, curr->m_value);
        }

        Node *next = curr->m_next.load(std::memory_order_acquire);
        if(next == m_tail) {
            sc->BlockFurtherNodes();
            sc->Deactivate();
            break;
        }

        pprev = prev;
        prev = curr;
        curr = next;
    }
    sc->BlockFurtherReports();
}

template <LockfreeListItem T, int Tag>
std::vector<T> IterableLockfreeList<T, Tag>::reconstruct_using_reports(
    typename IterableLockfreeList<T, Tag>::SCPointer sc)
{
    auto nodes = sc->ReadPointers();
    auto reports = sc->ReadReports();
    for(const auto& report : reports) {
        switch(report.m_type) {
        case SnapCollector<Node, T>::Report::Type::eInsert:
            nodes.insert({report.m_node, report.m_value});
            break;
        case SnapCollector<Node, T>::Report::Type::eDelete:
            nodes.erase({report.m_node, report.m_value});
            break;
        }
    }
    std::vector<T> ret{};
    for(const auto& node_desc : nodes) {
        ret.push_back(node_desc.m_value);
    }
    return ret;
}

template <LockfreeListItem T, int Tag>
std::vector<T> IterableLockfreeList<T, Tag>::TakeSnapshot()
{
retry:
    SCPointer sc = acquire_snap_collector();
    auto sc_hazard = m_schp.AddHazard(0, sc);
    if(sc != m_psc.load(std::memory_order_relaxed))
        goto retry;

    collect_snapshot(sc);
    auto&& ret = reconstruct_using_reports(sc);

    SCPointer dummy = SnapCollector<Node, T>::Dummy();
    if(m_psc.compare_exchange_strong(sc, dummy,
        std::memory_order_release, std::memory_order_relaxed)) {
        m_schp.RetireHazard(sc);
    }
    return ret;
}

} //namespace pe

