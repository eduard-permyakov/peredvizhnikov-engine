export module iterable_lockfree_list;

import platform;
import concurrency;
import logger;
import snap_collector;
import hazard_ptr;

import <atomic>;
import <concepts>;
import <memory>;
import <utility>;
import <vector>;
import <set>;

namespace pe{

export
template <typename T>
concept IterableLockfreeListItem = requires{
    requires (std::is_copy_constructible_v<T>
           || std::is_move_constructible_v<T>);
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
template <IterableLockfreeListItem T>
class IterableLockfreeList
{
protected:

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

    Node                                   *m_head;
    Node                                   *m_tail;
    HPContext<Node, 2, 2>                   m_hp;
    HPContext<SnapCollector<Node, T>, 1, 1> m_schp;
    AtomicSCPointer                         m_psc;

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
    std::tuple<bool, HazardPtr<Node, 2, 2>, HazardPtr<Node, 2, 2>> search(const T& value);

    void report_delete(Node *victim, T value);
    void report_insert(Node *new_node, T value);
    HazardPtr<SnapCollector<Node, T>, 1, 1> acquire_snap_collector();
    void collect_snapshot(SCPointer sc);
    std::vector<T> reconstruct_using_reports(SCPointer sc);

public:

    IterableLockfreeList();
    ~IterableLockfreeList();

    template <typename U = T>
    requires (std::is_constructible_v<T, U>)
    bool Insert(U&& value);
    bool Delete(const T& value);
    bool Find(const T& value);

    std::vector<T> TakeSnapshot();
};

template <typename T>
struct KeyValuePair
{
    uint64_t m_key;
    T        m_value;

    bool operator==(const KeyValuePair& rhs) const
    {
        return (m_key == rhs.m_key);
    }

    std::strong_ordering operator<=>(const KeyValuePair& rhs) const
    {
        return (m_key <=> rhs.m_key);
    }
};

export
template <typename T>
concept IterableLockfreeSetItem = requires{
    requires (std::is_copy_constructible_v<T>
           || std::is_move_constructible_v<T>);
    requires (std::is_copy_assignable_v<T>);
};

export
template <IterableLockfreeSetItem T>
class IterableLockfreeSet : private IterableLockfreeList<KeyValuePair<T>>
{
private:

    using base = IterableLockfreeList<KeyValuePair<T>>;

public:

    IterableLockfreeSet()
        : base()
    {}

    template <typename U = T>
    requires (std::is_constructible_v<T, U>)
    bool Insert(uint64_t key, U&& value)
    {
        KeyValuePair<T> pair{key, value};
        return base::Insert(pair);
    }

    bool Delete(uint64_t key)
    {
        KeyValuePair<T> pair{key, {}};
        return base::Delete(pair);
    }

    bool Find(uint64_t key)
    {
        KeyValuePair<T> pair{key, {}};
        return base::Find(pair);
    }

    std::optional<T> Get(uint64_t key)
    {
        KeyValuePair<T> inout{key};
        auto [exists, left_node, right_node] = base::search(inout, &inout);
        if(!exists)
            return std::nullopt;
        return {inout.m_value};
    }

    std::vector<std::pair<uint64_t, T>> TakeSnapshot()
    {
        std::vector<KeyValuePair<T>> snapshot = base::TakeSnapshot();
        std::vector<std::pair<uint64_t, T>> ret{};
        ret.resize(snapshot.size());
        std::transform(std::begin(snapshot), std::end(snapshot), std::begin(ret),
            [](KeyValuePair<T>& entry){
                return std::pair<uint64_t, T>(entry.m_key, entry.m_value);
            }
        );
        return ret;
    }
};

template <IterableLockfreeListItem T>
IterableLockfreeList<T>::IterableLockfreeList()
    : m_head{}
    , m_tail{}
    , m_hp{}
    , m_schp{}
    , m_psc{SnapCollector<Node, T>::Dummy()}
{
    std::unique_ptr<Node> tail{new Node{{}, nullptr}};
    std::unique_ptr<Node> head{new Node{{}, tail.get()}};

    m_head = head.release();
    m_tail = tail.release();
}

template <IterableLockfreeListItem T>
std::tuple<
    bool, 
    HazardPtr<typename IterableLockfreeList<T>::Node, 2, 2>, 
    HazardPtr<typename IterableLockfreeList<T>::Node, 2, 2>
>
IterableLockfreeList<T>::search(const T& value)
{
retry:

    Node *prev = m_head;
    Node *curr = prev->m_next.load(std::memory_order_acquire);

    auto prev_hazard = m_hp.AddHazard(0, prev);
    auto curr_hazard = m_hp.AddHazard(1, curr);
    if(curr != prev->m_next.load(std::memory_order_relaxed))
        goto retry;

    while(curr != m_tail) {

        /* Now 'prev' and 'curr' are safe to dereference */

        Node *next = curr->m_next.load(std::memory_order_acquire);
        if(is_marked_reference(next)) {
            report_delete(curr, curr->m_value);
            if(!prev->m_next.compare_exchange_strong(curr, get_unmarked_reference(next),
                std::memory_order_release, std::memory_order_relaxed)) {
                goto retry;
            }

            curr_hazard = m_hp.AddHazard(1, get_unmarked_reference(next));
            if(next != curr->m_next.load(std::memory_order_relaxed))
                goto retry;

            m_hp.RetireHazard(curr);
            curr = get_unmarked_reference(next);

        }else{
            if(curr != prev->m_next.load(std::memory_order_acquire))
                goto retry;
            if(curr->m_value >= value) {
                bool exists = curr->m_value == value;
                if(exists) {
                    report_insert(curr, curr->m_value);
                }
                return {exists, std::move(prev_hazard), std::move(curr_hazard)};
            }

            prev_hazard = m_hp.AddHazard(0, curr);
            curr_hazard = m_hp.AddHazard(1, next);
            if(next != curr->m_next.load(std::memory_order_relaxed))
                goto retry;

            prev = curr;
            curr = next;
        }
    }
    return {false, std::move(prev_hazard), std::move(curr_hazard)};
}

template <IterableLockfreeListItem T>
IterableLockfreeList<T>::~IterableLockfreeList()
{
    Node *curr = m_head;
    while((curr = m_head->m_next.load(std::memory_order_relaxed)) != m_tail) {
        Delete(curr->m_value);
    }
    delete m_head;
    delete m_tail;
}

template <IterableLockfreeListItem T>
template <typename U>
requires (std::is_constructible_v<T, U>)
bool IterableLockfreeList<T>::Insert(U&& value)
{
    Node *new_node = new Node{std::forward<U>(value), nullptr};

    do{
        auto [exists, left_node, right_node] = search(value);
        if(exists) {
            report_insert(new_node, new_node->m_value);
            delete new_node;
            return false;
        }

        new_node->m_next.store(*right_node, std::memory_order_relaxed);
        Node *expected = *right_node;

        if(left_node->m_next.compare_exchange_strong(expected, new_node,
            std::memory_order_release, std::memory_order_relaxed)) {

            report_insert(new_node, new_node->m_value);
            return true;
        }

    }while(true);
}

template <IterableLockfreeListItem T>
bool IterableLockfreeList<T>::Delete(const T& value)
{
    bool exists;
    Node *right_node_next;
    HazardPtr<Node, 2, 2> left_node{m_hp}, right_node{m_hp};
    do{
        std::tie(exists, left_node, right_node) = search(value);
        if(!exists)
            return false;

        right_node_next = right_node->m_next.load(std::memory_order_acquire);
        if(!is_marked_reference(right_node_next)) {
            if(right_node->m_next.compare_exchange_strong(right_node_next,
                get_marked_reference(right_node_next),
                std::memory_order_release, std::memory_order_relaxed)) {

                report_delete(*right_node, right_node->m_value);
                break;
            }
        }
    }while(true);

    Node *expected = *right_node;
    if(!left_node->m_next.compare_exchange_strong(expected, right_node_next,
        std::memory_order_release, std::memory_order_relaxed)) {

        /* We could not delete the node we just marked:
         * traverse the list and delete it 
         */
        search(value);
    }else{
        m_hp.RetireHazard(*right_node);
    }
    return true;
}

template <IterableLockfreeListItem T>
bool IterableLockfreeList<T>::Find(const T& value)
{
    auto [exists, left_node, right_node] = search(value);
    return exists;
}

template <IterableLockfreeListItem T>
void IterableLockfreeList<T>::report_delete(
    typename IterableLockfreeList<T>::Node *victim, T value)
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

template <IterableLockfreeListItem T>
void IterableLockfreeList<T>::report_insert(
    typename IterableLockfreeList<T>::Node *new_node, T value)
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

template <IterableLockfreeListItem T>
HazardPtr<SnapCollector<typename IterableLockfreeList<T>::Node, T>, 1, 1>
IterableLockfreeList<T>::acquire_snap_collector()
{
retry:
    SCPointer sc = m_psc.load(std::memory_order_acquire);
    auto sc_hazard = m_schp.AddHazard(0, sc);
    if(sc != m_psc.load(std::memory_order_relaxed))
        goto retry;

    if(sc->IsActive())
        return sc_hazard;

    SCPointer new_sc = new SnapCollector<Node, T>{true};
    if(!m_psc.compare_exchange_strong(sc, new_sc,
        std::memory_order_release, std::memory_order_relaxed)) {
        delete new_sc;
    }else if(sc != SnapCollector<Node, T>::Dummy()) {
        m_schp.RetireHazard(sc);
    }

    sc = m_psc.load(std::memory_order_acquire);
    sc_hazard = m_schp.AddHazard(0, sc);
    if(sc != m_psc.load(std::memory_order_relaxed))
        goto retry;
    if(!sc->IsActive())
        goto retry;
    return sc_hazard;
}

template <IterableLockfreeListItem T>
void IterableLockfreeList<T>::collect_snapshot(
    typename IterableLockfreeList<T>::SCPointer sc)
{
retry:

    Node *prev = m_head;
    Node *curr = prev->m_next.load(std::memory_order_acquire);
    Node *pprev = nullptr;

    auto prev_hazard = m_hp.AddHazard(0, prev);
    auto curr_hazard = m_hp.AddHazard(1, curr);
    if(curr != prev->m_next.load(std::memory_order_relaxed))
        goto retry;

    while(sc->IsActive()) {

        if(curr == m_tail) {
            sc->BlockFurtherNodes();
            sc->Deactivate();
            break;
        }

        /* Now 'prev' and 'curr' are safe to dereference */

        Node *next = curr->m_next.load(std::memory_order_acquire);
        if(!is_marked_reference(next)) {
            sc->AddNode(curr, curr->m_value);
        }

        next = get_unmarked_reference(next);
        prev_hazard = m_hp.AddHazard(0, curr);
        curr_hazard = m_hp.AddHazard(1, next);
        if(next != curr->m_next.load(std::memory_order_relaxed))
            goto retry;

        pprev = prev;
        prev = curr;
        curr = next;
    }
    sc->BlockFurtherReports();
}

template <IterableLockfreeListItem T>
std::vector<T> IterableLockfreeList<T>::reconstruct_using_reports(
    typename IterableLockfreeList<T>::SCPointer sc)
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

template <IterableLockfreeListItem T>
std::vector<T> IterableLockfreeList<T>::TakeSnapshot()
{
    auto sc = acquire_snap_collector();
    collect_snapshot(*sc);
    auto&& ret = reconstruct_using_reports(*sc);

    SCPointer dummy = SnapCollector<Node, T>::Dummy();
    SCPointer expected = *sc;

    if(m_psc.compare_exchange_strong(expected, dummy,
        std::memory_order_release, std::memory_order_relaxed)) {
        m_schp.RetireHazard(*sc);
    }
    return ret;
}

} //namespace pe

