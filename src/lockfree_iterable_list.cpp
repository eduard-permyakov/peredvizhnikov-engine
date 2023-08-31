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

export module lockfree_iterable_list;

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
concept LockfreeIterableListItem = requires{
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
template <LockfreeIterableListItem T>
class LockfreeIterableList
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

    LockfreeIterableList(LockfreeIterableList&&) = delete;
    LockfreeIterableList(LockfreeIterableList const&) = delete;
    LockfreeIterableList& operator=(LockfreeIterableList&&) = delete;
    LockfreeIterableList& operator=(LockfreeIterableList const&) = delete;

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

    LockfreeIterableList();
    ~LockfreeIterableList();

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
concept LockfreeIterableSetItem = requires{
    requires (std::is_copy_constructible_v<T>
           || std::is_move_constructible_v<T>);
    requires (std::is_copy_assignable_v<T>);
};

export
template <LockfreeIterableSetItem T>
class LockfreeIterableSet : private LockfreeIterableList<KeyValuePair<T>>
{
private:

    using base = LockfreeIterableList<KeyValuePair<T>>;

public:

    LockfreeIterableSet()
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

template <LockfreeIterableListItem T>
LockfreeIterableList<T>::LockfreeIterableList()
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

template <LockfreeIterableListItem T>
std::tuple<
    bool, 
    HazardPtr<typename LockfreeIterableList<T>::Node, 2, 2>, 
    HazardPtr<typename LockfreeIterableList<T>::Node, 2, 2>
>
LockfreeIterableList<T>::search(const T& value)
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

template <LockfreeIterableListItem T>
LockfreeIterableList<T>::~LockfreeIterableList()
{
    Node *curr = m_head;
    while((curr = m_head->m_next.load(std::memory_order_relaxed)) != m_tail) {
        Delete(curr->m_value);
    }
    auto sc = m_psc.load(std::memory_order_acquire);
    if(sc != SnapCollector<Node, T>::Dummy()) {
        m_schp.RetireHazard(sc);
    }
    delete m_head;
    delete m_tail;
}

template <LockfreeIterableListItem T>
template <typename U>
requires (std::is_constructible_v<T, U>)
bool LockfreeIterableList<T>::Insert(U&& value)
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

template <LockfreeIterableListItem T>
bool LockfreeIterableList<T>::Delete(const T& value)
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

template <LockfreeIterableListItem T>
bool LockfreeIterableList<T>::Find(const T& value)
{
    auto [exists, left_node, right_node] = search(value);
    return exists;
}

template <LockfreeIterableListItem T>
void LockfreeIterableList<T>::report_delete(
    typename LockfreeIterableList<T>::Node *victim, T value)
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

template <LockfreeIterableListItem T>
void LockfreeIterableList<T>::report_insert(
    typename LockfreeIterableList<T>::Node *new_node, T value)
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

template <LockfreeIterableListItem T>
HazardPtr<SnapCollector<typename LockfreeIterableList<T>::Node, T>, 1, 1>
LockfreeIterableList<T>::acquire_snap_collector()
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

template <LockfreeIterableListItem T>
void LockfreeIterableList<T>::collect_snapshot(
    typename LockfreeIterableList<T>::SCPointer sc)
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

template <LockfreeIterableListItem T>
std::vector<T> LockfreeIterableList<T>::reconstruct_using_reports(
    typename LockfreeIterableList<T>::SCPointer sc)
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

template <LockfreeIterableListItem T>
std::vector<T> LockfreeIterableList<T>::TakeSnapshot()
{
    auto sc = acquire_snap_collector();
    collect_snapshot(*sc);
    return reconstruct_using_reports(*sc);
}

} //namespace pe

