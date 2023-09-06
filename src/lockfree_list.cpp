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

export module lockfree_list;

import platform;
import concurrency;
import logger;
import hazard_ptr;

import <atomic>;
import <concepts>;
import <memory>;
import <utility>;
import <iostream>;
import <iomanip>;
import <optional>;

namespace pe{

export
template <typename T>
concept LockfreeListItem = requires{
    requires (std::is_default_constructible_v<T>);
    requires (std::is_copy_constructible_v<T>
           || std::is_move_constructible_v<T>);
    requires (std::equality_comparable<T>);
    requires (std::three_way_comparable<T>);
};

/* Forward declarations.
 */
template <typename Node, typename U>
class SnapCollector;

/*
 * Implementation of a Harris non-blocking linked list.
 */
export
template <LockfreeListItem T>
class LockfreeList
{
protected:

    template <typename Node, typename U>
    friend class SnapCollector;

    template <typename U>
    using AtomicPointer = std::atomic<U*>;

    struct Node
    {
        T                   m_value;
        AtomicPointer<Node> m_next;
    };

    static_assert(sizeof(AtomicPointer<Node>) == sizeof(Node*));
    static_assert(AtomicPointer<Node>::is_always_lock_free);

    Node                  *m_head;
    Node                  *m_tail;
    HPContext<Node, 2, 2>  m_hp;

    LockfreeList(LockfreeList&&) = delete;
    LockfreeList(LockfreeList const&) = delete;
    LockfreeList& operator=(LockfreeList&&) = delete;
    LockfreeList& operator=(LockfreeList const&) = delete;

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
    std::tuple<bool, HazardPtr<Node, 2, 2>, HazardPtr<Node, 2, 2>> search(const T& value, T *out);

public:

    LockfreeList();
    ~LockfreeList();

    template <typename U = T>
    requires (std::is_constructible_v<T, U>)
    bool Insert(U&& value);
    bool Delete(const T& value);
    bool Find(const T& value);
    std::optional<T> PeekHead();
    [[maybe_unused]] void PrintUnsafe();
};

/* Technically, the Harris list is already a set because it 
 * cannot store duplicate values. We just adapt the API slightly
 * to allow using keys/hashes for equality comaprison.
 */
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

template <typename T>
std::ostream& operator<<(std::ostream& stream, const KeyValuePair<T>& kv)
{
    return (stream << kv.m_key << ":" << kv.m_value);
}

export
template <typename T>
concept LockfreeSetItem = requires{
    requires (std::is_default_constructible_v<T>);
    requires (std::is_copy_constructible_v<T>
           || std::is_move_constructible_v<T>);
    requires (std::is_copy_assignable_v<T>);
};

export
template <LockfreeSetItem T>
class LockfreeSet : private LockfreeList<KeyValuePair<T>>
{
private:

    using base = LockfreeList<KeyValuePair<T>>;

public:

    LockfreeSet()
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

    std::optional<std::pair<uint64_t, T>> PeekHead()
    {
        std::optional<KeyValuePair<T>> head = base::PeekHead();
        if(!head.has_value())
            return std::nullopt;
        return {std::pair<uint64_t, T>{head.value().m_key, head.value().m_value}};
    }

    [[maybe_unused]] void PrintUnsafe()
    {
        base::PrintUnsafe();
    }
};

template <LockfreeListItem T>
LockfreeList<T>::LockfreeList()
    : m_head{}
    , m_tail{}
    , m_hp{}
{
    std::unique_ptr<Node> tail{new Node{{}, nullptr}};
    std::unique_ptr<Node> head{new Node{{}, tail.get()}};

    m_head = head.release();
    m_tail = tail.release();
}

template <LockfreeListItem T>
std::tuple<
    bool, 
    HazardPtr<typename LockfreeList<T>::Node, 2, 2>, 
    HazardPtr<typename LockfreeList<T>::Node, 2, 2>
>
LockfreeList<T>::search(const T& value, T *out)
{
retry:

    Node *prev = m_head;
    Node *curr = prev->m_next.load(std::memory_order_acquire);

    auto prev_hazard = m_hp.AddHazard(0, prev);
    auto curr_hazard = m_hp.AddHazard(1, curr);
    if(curr != prev->m_next.load(std::memory_order_relaxed))
        goto retry;

    /* Now 'prev' and 'curr' are safe to dereference */

    while(curr != m_tail) {

        Node *next = curr->m_next.load(std::memory_order_acquire);
        if(is_marked_reference(next)) {
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
                if(exists && out) {
                    *out = curr->m_value;
                    std::atomic_thread_fence(std::memory_order_release);
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

template <LockfreeListItem T>
LockfreeList<T>::~LockfreeList()
{
    Node *curr = m_head;
    while((curr = m_head->m_next.load(std::memory_order_relaxed)) != m_tail) {
        Delete(curr->m_value);
    }
    delete m_head;
    delete m_tail;
}

template <LockfreeListItem T>
template <typename U>
requires (std::is_constructible_v<T, U>)
bool LockfreeList<T>::Insert(U&& value)
{
    Node *new_node = new Node{std::forward<U>(value), nullptr};

    do{
        auto [exists, left_node, right_node] = search(value, nullptr);
        if(exists) {
            delete new_node;
            return false;
        }

        new_node->m_next.store(*right_node, std::memory_order_relaxed);
        Node *expected = *right_node;

        if(left_node->m_next.compare_exchange_strong(expected, new_node,
            std::memory_order_release, std::memory_order_relaxed)) {
            return true;
        }

    }while(true);
}

template <LockfreeListItem T>
bool LockfreeList<T>::Delete(const T& value)
{
    bool exists;
    Node *right_node_next;
    HazardPtr<Node, 2, 2> left_node{m_hp}, right_node{m_hp};
    do{
        std::tie(exists, left_node, right_node) = search(value, nullptr);
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

    Node *expected = *right_node;
    if(!left_node->m_next.compare_exchange_strong(expected, right_node_next,
        std::memory_order_release, std::memory_order_relaxed)) {

        /* We could not delete the node we just marked:
         * traverse the list and delete it 
         */
        search(value, nullptr);
    }else{
        m_hp.RetireHazard(*right_node);
    }
    return true;
}

template <LockfreeListItem T>
bool LockfreeList<T>::Find(const T& value)
{
    auto [exists, left_node, right_node] = search(value, nullptr);
    return exists;
}

template <LockfreeListItem T>
std::optional<T> LockfreeList<T>::PeekHead()
{
    do{
        Node *curr = m_head->m_next.load(std::memory_order_acquire);
        auto curr_hazard = m_hp.AddHazard(0, curr);
        if(curr != m_head->m_next.load(std::memory_order_relaxed))
            continue;

        if(curr == m_tail)
            return std::nullopt;

        Node *next = curr->m_next.load(std::memory_order_acquire);
        if(is_marked_reference(next)) {
            auto value = curr->m_value;
            search(value, nullptr);
            continue;
        }
        return curr->m_value;

    }while(true);
}

template <LockfreeListItem T>
void LockfreeList<T>::PrintUnsafe()
{
    constexpr int entries_per_line = 2;
    std::lock_guard<std::mutex> lock{iolock};

    Node *curr = m_head;
    int count = 0;

    while(curr) {

        auto name = (curr == m_head) ? "Head"
                  : (curr == m_tail) ? "Tail"
                  : "Node";

        bool newline = (count % entries_per_line == 0);
        if(count > 0) {
            pe::ioprint_unlocked(TextColor::eGreen, "", false, newline,
                " -> ");
        }
        pe::ioprint_unlocked(TextColor::eWhite, "", newline, false,
            "[", name, ":", curr, " (");
        pe::ioprint_unlocked(TextColor::eBlue, "", false, false,
            fmt::justified{curr->m_value, 8, fmt::Justify::eRight, '.'});
        pe::ioprint_unlocked(TextColor::eWhite, "", false, (curr == m_tail),
            ") marked:", is_marked_reference(curr), "]");

        curr = curr->m_next.load(std::memory_order_relaxed);
        count++;
    }
}

} //namespace pe

