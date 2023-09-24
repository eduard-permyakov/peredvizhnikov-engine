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

export module lockfree_deque;

import platform;
import concurrency;
import logger;
import hazard_ptr;
import assert;

import <atomic>;
import <concepts>;
import <optional>;
import <mutex>;
import <sstream>;

namespace pe{

export
template <typename T>
concept LockfreeDequeItem = requires{
    requires (std::is_copy_constructible_v<T>
           || std::is_move_constructible_v<T>);
};

/* Based on the paper "Lock-Free and Practical Deques using
 * Single-Word Compare-And-Swap" by Hakan Sundell and
 * Philippas Tsigas.
 */
export
template <LockfreeDequeItem T>
class LockfreeDeque
{
    struct Node;
    using Link = std::atomic<Node*>;

    struct Node
    {
        std::atomic_uint64_t m_refcount;
        Link                 m_prev;
        Link                 m_next;
        T                    m_value;
    };

    mutable HPContext<Node, 2, 2> m_hp;
    Node                         *m_head;
    Node                         *m_tail;

    using HazardPtr = decltype(m_hp)::hazard_ptr_type;

    LockfreeDeque(LockfreeDeque&&) = delete;
    LockfreeDeque(LockfreeDeque const&) = delete;
    LockfreeDeque& operator=(LockfreeDeque&&) = delete;
    LockfreeDeque& operator=(LockfreeDeque const&) = delete;

    inline bool is_marked_reference(Node *node) const
    {
        return (reinterpret_cast<uintptr_t>(node) & 0x1) == 0x1;
    }

    inline Node *get_unmarked_reference(Node *node) const
    {
        return reinterpret_cast<Node*>(reinterpret_cast<uintptr_t>(node) & ~0x1);
    }

    inline Node *get_marked_reference(Node *node) const
    {
        return reinterpret_cast<Node*>(reinterpret_cast<uintptr_t>(node) | 0x1);
    }

    inline bool is_marked_link(const Link& link) const
    {
        Node *node = link.load(std::memory_order_acquire);
        return is_marked_reference(node);
    }

    template <typename U = T>
    HazardPtr smr_allocate_node(U&& value) const
    {
        Node *newnode = new Node{0, 0, 0, std::forward<U>(value)};
        newnode->m_refcount.store(2, std::memory_order_relaxed);
        return m_hp.AddHazard(0, newnode);
    }

    uint64_t decrement_and_test_and_set(Node *node) const
    {
        assert(!is_marked_reference(node));
        uint64_t oldval = node->m_refcount.load(std::memory_order_relaxed);
        uint64_t newval;
        do{
            newval = oldval - 2;
            if(newval == 0)
                newval = 1;
            AnnotateHappensBefore(__FILE__, __LINE__, &node->m_refcount);
        }while(!node->m_refcount.compare_exchange_strong(oldval, newval,
            std::memory_order_release, std::memory_order_relaxed));
        return (oldval - newval) & 0x1;
    }

    void smr_release_node(Node *node) const
    {
        assert(!is_marked_reference(node));
        if(!node)
            return;
        if(decrement_and_test_and_set(node) == 0)
            return;

        std::atomic_thread_fence(std::memory_order_acquire);
        AnnotateHappensAfter(__FILE__, __LINE__, &node->m_refcount);
        Node *prev = node->m_prev.load(std::memory_order_relaxed);
        Node *next = node->m_next.load(std::memory_order_relaxed);

        smr_release_node(get_unmarked_reference(prev));
        smr_release_node(get_unmarked_reference(next));

        m_hp.RetireHazard(node);
    }

    Node *smr_copy_node(Node *node) const
    {
        assert(!is_marked_reference(node));
        if(!node)
            return nullptr;
        auto prev = node->m_refcount.fetch_add(2, std::memory_order_relaxed);
        assert(prev);
        assert(!(prev & 0x1));
        return node;
    }

    Node *smr_read_link(const Link& link) const
    {
        pe::assert(&link != &m_head->m_prev);
        pe::assert(&link != &m_tail->m_next);

        while(true) {
            Node *node = link.load(std::memory_order_acquire);
            assert(node);
            if(is_marked_reference(node))
                return nullptr;

            /* Use a hazard pointer to pin the node 
             * until we are done accessing the reference 
             * count. This is necessary since loading
             * the node pointer and incrementing the
             * reference counter are not atomic. The
             * reference count might go down to zero
             * after we've loaded the 'node' pointer.
             */
            auto hazard = m_hp.AddHazard(1, node);
            if(link.load(std::memory_order_relaxed) != node)
                continue;

            uint64_t prev_refcnt = node->m_refcount.fetch_add(2, std::memory_order_relaxed);
            if(prev_refcnt & 0x1) {
                /* The node's already been reclaimed! */
                continue;
            }

            if(link.load(std::memory_order_relaxed) != node) {
                smr_release_node(node);
                continue;
            }

            /* Now we are guaranteed that the memory's not going
             * anywhere until we decrement the reference count.
             */
            return node;
        }
    }

    Node *smr_read_marked_link(const Link& link) const
    {
        pe::assert(&link != &m_head->m_prev);
        pe::assert(&link != &m_tail->m_next);

        while(true) {
            Node *node = link.load(std::memory_order_acquire);
            assert(node);

            Node *unmarked = get_unmarked_reference(node);
            auto hazard = m_hp.AddHazard(1, unmarked);
            if(link.load(std::memory_order_relaxed) != node)
                continue;

            uint64_t prev_refcnt = unmarked->m_refcount.fetch_add(2, std::memory_order_relaxed);
            if(prev_refcnt & 0x1) {
                /* The node's already been reclaimed! */
                continue;
            }

            if(link.load(std::memory_order_relaxed) != node) {
                smr_release_node(unmarked);
                continue;
            }
            return unmarked;
        }
    }

    /* The delete_next procedure repatedly tries to delete
     * (in the sense of a chain of next pointers starting
     * from the head node) the given marked node (node) by
     * changing the next pointer from the previous non-marked
     * node.
     */
    void delete_next(Node *node)
    {
        assert(node != m_head);
        assert(node != m_tail);
        assert(!is_marked_reference(node));
        assert(is_marked_link(node->m_next));

        while(true) {
            Node *link = node->m_prev.load(std::memory_order_acquire);
            assert(link);

            if(is_marked_reference(link)
            || node->m_prev.compare_exchange_strong(link, get_marked_reference(link),
                std::memory_order_release, std::memory_order_relaxed)) {

                break;
            }
        }

        bool prev_node_deleted = true;
        Node *prev = smr_read_marked_link(node->m_prev);
        Node *next = smr_read_marked_link(node->m_next);
        Backoff backoff{10, 1'000, 0};

        assert(prev && prev != m_tail);
        assert(next && next != m_head);

        while(true) {
            /* Ensure 'node' is not already deleted 
             */
            if(prev == next)
                break;

            /* The node's already been deleted */
            if(prev == m_tail)
                break;

            /* Ensure the 'next' node is not marked 
             */
            if(is_marked_link(next->m_next)) {
                Node *tmp = smr_read_marked_link(next->m_next);
                smr_release_node(next);
                next = tmp;
                continue;
            }

            Node *prev_next = smr_read_link(prev->m_next);
            if(!prev_next) {
                assert(is_marked_link(prev->m_next));
                if(!prev_node_deleted) {
                    delete_next(prev);
                    prev_node_deleted = true;
                }
                Node *prev_prev = smr_read_marked_link(prev->m_prev);
                assert(prev_prev != m_tail);
                smr_release_node(prev);
                prev = prev_prev;
                continue;
            }
            /* Ensure 'prev' is the previous node of 'node' 
             */
            if(prev_next != node) {
                prev_node_deleted = false;
                smr_release_node(prev);
                prev = prev_next;
                continue;
            }
            smr_release_node(prev_next);
            assert(next);
            /* Atomically delete the node by chaning the 'next'
             * pointer of the previous non-marked node.
             */
            assert(next != prev);
            assert(next);
            Node *expected = get_unmarked_reference(node);

            smr_copy_node(next);
            if(prev->m_next.compare_exchange_strong(expected, get_unmarked_reference(next),
                std::memory_order_release, std::memory_order_relaxed)) {

                smr_release_node(node);
                break;
            }
            smr_release_node(next);
            backoff.BackoffMaybe();
        }
        smr_release_node(prev);
        smr_release_node(next);
    }

    /* The help_insert procedure repeatedly tries to correct
     * the prev pointer of the given node (node) given a suggestion
     * of a previous (not necessarily the first) node (prev).
     */
    Node *help_insert(Node *prev, Node *node)
    {
        assert(prev);
        assert(node);
        assert(prev != m_tail);
        assert(!is_marked_reference(node));

        Backoff backoff{10, 1'000, 0};
        bool prev_node_deleted = true;

        while(true) {

            /* The node's already been deleted */
            if(prev == m_tail) {
                smr_release_node(prev);
                prev = smr_read_link(prev->m_prev); 
                return prev;
            }

            /* Ensure that 'prev' is not marked 
             */
            assert(prev && (prev != m_tail));
            Node *prev_next = smr_read_link(prev->m_next);

            if(!prev_next) {
                assert(is_marked_link(prev->m_next));
                if(!prev_node_deleted) {
                    delete_next(prev);
                    prev_node_deleted = true;
                }
                prev_next = smr_read_marked_link(prev->m_prev);
                assert(prev_next != m_tail);
                smr_release_node(prev);
                prev = prev_next;
                continue;
            }
            /* Ensure 'node' is not marked
             */
            Node *link = node->m_prev.load(std::memory_order_acquire);
            if(is_marked_reference(link)) {
                smr_release_node(prev_next);
                break;
            }
            /* Ensure that 'prev' is the previous node of 'node'. 
             */
            if(prev_next != node) {
                prev_node_deleted = false;
                smr_release_node(prev);
                prev = prev_next;
                continue;
            }
            smr_release_node(prev_next);

            /* Atomically correct the 'prev' pointer
             */
            assert(prev != node);
            assert(prev);

            smr_copy_node(prev);
            if(node->m_prev.compare_exchange_strong(link, get_unmarked_reference(prev),
                std::memory_order_release, std::memory_order_relaxed)) {

                smr_release_node(get_unmarked_reference(link));

                if(is_marked_link(prev->m_prev)) {

                    Node *prev_next = prev->m_next.load(std::memory_order_acquire);
                    assert(is_marked_reference(prev_next));
                    continue;
                }
                break;
            }
            smr_release_node(prev);
            backoff.BackoffMaybe();
        }
        return prev;
    }

    void push_common(Node *node, Node *next)
    {
        assert(next != m_head);
        assert(node != m_tail);
        Backoff backoff{10, 1'000, 0};

        while(true) {
            Node *link = next->m_prev.load(std::memory_order_acquire);
            assert(link);

            if(is_marked_reference(link)
            || node->m_next.load(std::memory_order_relaxed) != get_unmarked_reference(next)) {
                break;
            }

            assert(node);
            smr_copy_node(node);
            if(next->m_prev.compare_exchange_strong(link, get_unmarked_reference(node),
                std::memory_order_release, std::memory_order_relaxed)) {

                smr_release_node(get_unmarked_reference(link));

                if(is_marked_link(node->m_prev)) {
                    Node *prev = smr_copy_node(node);
                    prev = help_insert(prev, next);
                    smr_release_node(prev);
                }
                break;
            }
            smr_release_node(node);
            backoff.BackoffMaybe();
        }
        smr_release_node(next);
        smr_release_node(node);
    }

    void remove_cross_reference(Node *node)
    {
        while(true) {
            Node *plink = node->m_prev.load(std::memory_order_acquire);
            Node *prev = get_unmarked_reference(plink); 

            if(is_marked_reference(prev->m_next.load(std::memory_order_relaxed))) {

                Node *prev_prev = smr_read_marked_link(prev->m_prev);
                node->m_prev.store(get_marked_reference(prev_prev), std::memory_order_release);
                smr_release_node(prev);
                continue;
            }

            Node *nlink = node->m_next.load(std::memory_order_acquire);
            Node *next = get_unmarked_reference(nlink);

            if(is_marked_reference(next->m_next.load(std::memory_order_relaxed))) {

                Node *next_next = smr_read_marked_link(next->m_next);
                node->m_next.store(get_marked_reference(next_next), std::memory_order_release);
                smr_release_node(next);
                continue;
            }
            break;
        }
    }

public:

    LockfreeDeque()
        : m_hp{}
        , m_head{*smr_allocate_node(T{})}
        , m_tail{*smr_allocate_node(T{})}
    {
        m_head->m_prev.store(nullptr, std::memory_order_release);
        m_head->m_next.store(smr_copy_node(m_tail), std::memory_order_release);

        m_tail->m_prev.store(smr_copy_node(m_head), std::memory_order_release);
        m_tail->m_next.store(nullptr, std::memory_order_release);
    }

    ~LockfreeDeque()
    {
        std::optional<T> curr;
        do{
            curr = PopLeft();
        }while(curr.has_value());

        smr_release_node(m_head->m_next.load(std::memory_order_acquire));
        smr_release_node(m_tail->m_prev.load(std::memory_order_acquire));

        m_head->m_next.store(nullptr, std::memory_order_release);
        m_tail->m_prev.store(nullptr, std::memory_order_release);

        smr_release_node(m_head);
        smr_release_node(m_tail);
    }

    template <typename U = T>
    requires (std::is_convertible_v<U, T>)
    void PushLeft(U&& value)
    {
        HazardPtr node = smr_allocate_node(std::forward<U>(value));
        Node *prev = smr_copy_node(m_head);
        Node *next = smr_read_link(prev->m_next);
        smr_copy_node(*node);
        Backoff backoff{10, 1'000, 0};

        while(true) {
            if(prev->m_next.load(std::memory_order_relaxed) != get_unmarked_reference(next)) {

                smr_release_node(next);
                next = smr_read_link(prev->m_next);
                continue;
            }
            assert(prev && next);
            node->m_prev.store(prev, std::memory_order_release);
            node->m_next.store(next, std::memory_order_release);

            assert(*node);
            Node *expected = get_unmarked_reference(next);

            if(prev->m_next.compare_exchange_strong(expected, *node,
                std::memory_order_release, std::memory_order_relaxed)) {

                smr_copy_node(*node);
                break;
            }
            backoff.BackoffMaybe();
        }
        push_common(*node, next);
        smr_release_node(*node);
    }

    template <typename U = T>
    void PushRight(U&& value)
    {
        HazardPtr node = smr_allocate_node(std::forward<U>(value));
        Node *next = smr_copy_node(m_tail);
        Node *prev = smr_read_link(next->m_prev);
        smr_copy_node(*node);
        Backoff backoff{10, 1'000, 0};

        while(true) {
            if(prev->m_next.load(std::memory_order_relaxed) != get_unmarked_reference(next)) {
                prev = help_insert(prev, next);
                continue;
            }
            assert(prev && next);
            node->m_prev.store(prev, std::memory_order_release);
            node->m_next.store(next, std::memory_order_release);

            assert(*node);
            Node *expected = get_unmarked_reference(next);

            if(prev->m_next.compare_exchange_strong(expected, *node,
                std::memory_order_release, std::memory_order_relaxed)) {

                smr_copy_node(*node);
                break;
            }
            backoff.BackoffMaybe();
        }
        push_common(*node, next);
        smr_release_node(*node);
    }

    std::optional<T> PopLeft()
    {
        std::optional<T> ret{};
        Node *node, *prev = smr_copy_node(m_head);
        Backoff backoff{10, 1'000, 0};

        while(true) {
            node = smr_read_link(prev->m_next);
            if(node == m_tail) {
                smr_release_node(node);
                smr_release_node(prev);
                return std::nullopt;
            }
            Node *link = node->m_next.load(std::memory_order_acquire);
            assert(link);

            if(is_marked_reference(link)) {
                delete_next(node);
                smr_release_node(node);
                continue;
            }

            assert(link);
            if(node->m_next.compare_exchange_strong(link, get_marked_reference(link),
                std::memory_order_release, std::memory_order_relaxed)) {

                assert(node != m_tail);
                delete_next(node);

                Node *next = smr_read_marked_link(node->m_next);
                prev = help_insert(prev, next);

                smr_release_node(prev);
                smr_release_node(next);

                ret = node->m_value;
                std::atomic_thread_fence(std::memory_order_release);
                break;
            }
            smr_release_node(node);
            backoff.BackoffMaybe();
        }
        remove_cross_reference(node);
        smr_release_node(node);
        return ret;
    }

    std::optional<T> PopRight()
    {
        std::optional<T> ret{};
        Backoff backoff{10, 1'000, 0};
        Node *next = smr_copy_node(m_tail);
        Node *node = smr_read_link(next->m_prev);
        assert(node);

        while(true) {
            if(node->m_next.load(std::memory_order_relaxed) != get_unmarked_reference(next)) {
                node = help_insert(node, next);
                continue;
            }

            if(node == m_head) {
                smr_release_node(node);
                smr_release_node(next);
                return std::nullopt;
            }

            assert(next);
            Node *expected = get_unmarked_reference(next);

            if(node->m_next.compare_exchange_strong(expected, get_marked_reference(next),
                std::memory_order_release, std::memory_order_relaxed)) {

                delete_next(node);
                Node *prev = smr_read_marked_link(node->m_prev);
                assert(prev);
                prev = help_insert(prev, next);

                assert(prev != next);
                smr_release_node(prev);
                smr_release_node(next);

                ret = node->m_value;
                std::atomic_thread_fence(std::memory_order_release);
                break;
            }
            backoff.BackoffMaybe();
        }
        remove_cross_reference(node);
        smr_release_node(node);
        return ret;
    }

    [[maybe_unused]] void PrintUnsafe()
    {
        constexpr int entries_per_line = 3;
        constexpr int node_width = 20;
        std::lock_guard<std::mutex> lock{iolock};

        auto print_row = [this](Node *curr, size_t max) {

            std::vector<Node*> nodes{};
            Node *iter = curr;
            while(iter && nodes.size() < max) {
                nodes.push_back(iter);
                iter = get_unmarked_reference(iter->m_next.load());
            }

            struct line_opts
            {
                std::size_t      m_width;
                bool             m_next_arrow;
                bool             m_prev_arrow;
                std::string_view m_heading;
            };

            auto print_line = [&](std::vector<Node*>& nodes, line_opts opts, auto&& getter) {

                for(int i = 0; i < nodes.size(); i++) {

                    bool first = (i == 0);
                    bool last = (i == nodes.size() - 1);

                    std::stringstream ss{};
                    ss << getter(*nodes[i]);

                    if(opts.m_prev_arrow) {
                        pe::ioprint_unlocked(TextColor::eGreen, "", first, false, "<--");
                    }else{
                        pe::ioprint_unlocked(TextColor::eGreen, "", first, false, "   ");
                    }
                    pe::ioprint_unlocked(TextColor::eWhite, "", false, false, "|");
                    pe::ioprint_unlocked(TextColor::eYellow, "", false, false, 
                        fmt::justified{
                            std::string{opts.m_heading} 
                          + (opts.m_heading.size() ? ": " : "")
                          + ss.str(),
                            opts.m_width,
                            fmt::Justify::eLeft,
                            ' '
                        }
                    );
                    pe::ioprint_unlocked(TextColor::eWhite, "", false, false, "|");

                    if(opts.m_next_arrow) {
                        pe::ioprint_unlocked(TextColor::eGreen, "", false, last, "-->");
                    }else{
                        pe::ioprint_unlocked(TextColor::eGreen, "", false, last, "   ");
                    }
                }
            };

            auto print_borders = [&](std::vector<Node*>& nodes, std::size_t width) {

                for(int i = 0; i < nodes.size(); i++) {

                    bool first = (i == 0);
                    bool last = (i == nodes.size() - 1);

                    pe::ioprint_unlocked(TextColor::eWhite, "", first, last,
                        "   +", std::string(width, '-'), "+   ");
                }
            };

            print_borders(nodes, node_width);
            print_line(nodes, {node_width, false, false, "ptr"}, [](const Node& node){
                return &node;
            });
            print_line(nodes, {node_width, false, false, "val"}, [](const Node& node){
                return node.m_value;
            });
            print_line(nodes, {node_width, false, false, "refcnt"}, [](const Node& node){
                return node.m_refcount.load();
            });
            print_borders(nodes, node_width);
            print_line(nodes, {node_width, true, false, "next"}, [](const Node& node){
                return node.m_next.load();
            });
            print_line(nodes, {node_width, false, true, "prev"}, [](const Node& node){
                return node.m_prev.load();
            });
            print_borders(nodes, node_width);

            return iter;
        };

        Node *iter = m_head;
        do{
            iter = print_row(iter, entries_per_line);
        }while(iter);
    }
};

} // namespace pe

