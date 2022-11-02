export module lockfree_list;

import platform;
import concurrency;
import logger;

import <atomic>;
import <concepts>;
import <memory>;
import <utility>;
import <iostream>;
import <sstream>;
import <iomanip>;

namespace pe{

/*
 * Implementation of a Harris non-blocking linked list.
 */
export
template <typename T, int Tag>
requires requires{
    requires (std::is_default_constructible_v<T>);
    requires (std::is_copy_constructible_v<T>);
    requires (std::is_copy_assignable_v<T>);
    requires (std::equality_comparable<T>);
    requires (std::three_way_comparable<T>);
}
class LockfreeList
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

    Node *m_head;
    Node *m_tail;
    HPContext<Node, 2, 2, Tag>& m_hp;

    LockfreeList()
        : m_head{}
        , m_tail{}
        , m_hp{HPContext<Node, 2, 2, Tag>::Instance()}
    {
        std::unique_ptr<Node> tail{new Node{{}, nullptr}};
        std::unique_ptr<Node> head{new Node{{}, tail.get()}};

        m_head = head.release();
        m_tail = tail.release();
    }

    LockfreeList(LockfreeList&&) = delete;
    LockfreeList(LockfreeList const&) = delete;
    LockfreeList& operator=(LockfreeList&&) = delete;
    LockfreeList& operator=(LockfreeList const&) = delete;

    bool is_marked_reference(Node *next) const
    {
        return (reinterpret_cast<uintptr_t>(next) & 0x1) == 0x1;
    }

    Node *get_marked_reference(Node *next) const
    {
        return reinterpret_cast<Node*>(reinterpret_cast<uintptr_t>(next) | 0x1);
    }

    Node* get_unmarked_reference(Node *next) const
    {
        return reinterpret_cast<Node*>(reinterpret_cast<uintptr_t>(next) & ~0x1);
    }

    /* Returns the left and right nodes for a to-be-inserted value. 
     * The boolean indicates that a node with the value already 
     * exists in the list.
     */
    std::tuple<bool, Node*, Node*> search(const T& value)
    {
    try_again:

        Node *prev = m_head;
        Node *curr = prev->m_next.load(std::memory_order_acquire);
        Node *pprev = nullptr;

        while(curr) {

            auto prev_hazard = m_hp.AddHazard(0, prev);
            if(pprev && prev != pprev->m_next.load(std::memory_order_relaxed))
                goto try_again;

            auto curr_hazard = m_hp.AddHazard(1, curr);
            if(curr != prev->m_next.load(std::memory_order_relaxed))
                goto try_again;

            /* Now 'prev' and 'curr' are safe to dereference */

            Node *next = curr->m_next.load(std::memory_order_acquire);
            if(is_marked_reference(next)) {
                if(!prev->m_next.compare_exchange_strong(curr, get_unmarked_reference(next),
                    std::memory_order_release, std::memory_order_relaxed)) {
                    goto try_again;
                }
                m_hp.RetireHazard(curr);
                curr = get_unmarked_reference(next);
            }else{
                if(curr != prev->m_next.load(std::memory_order_acquire))
                    goto try_again;
                if(curr->m_value >= value) {
                    bool exists = curr->m_value == value;
                    return {exists, prev, curr};
                }

                pprev = prev;
                prev = curr;
                curr = next;
            }
        }
        return {false, pprev, prev};
    }

public:

    static LockfreeList& Instance()
    {
        static LockfreeList s_instance{};
        return s_instance;
    }

    ~LockfreeList()
    {
        Node *curr = m_head;
        while((curr = m_head->m_next.load(std::memory_order_relaxed)) != m_tail) {
            Delete(curr->m_value);
        }
    }

    template <typename U = T>
    requires (std::is_constructible_v<T, U>)
    bool Insert(U&& value)
    {
        Node *new_node = new Node{std::forward<U>(value), nullptr};

        do{
            auto [exists, left_node, right_node] = search(value);
            if(exists) {
                /* There already exists a node with this value */
                delete new_node;
                return false;
            }

            new_node->m_next.store(right_node, std::memory_order_relaxed);
            if(left_node->m_next.compare_exchange_strong(right_node, new_node,
                std::memory_order_release, std::memory_order_relaxed))
                return true;

        }while(true);
    }

    bool Delete(const T& value)
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
        }while(true); /* B4 */

        if(!left_node->m_next.compare_exchange_strong(right_node, right_node_next,
            std::memory_order_release, std::memory_order_relaxed)) {

            /* We could not delete the node we just marked:
             * traverse the list and delete it 
             */
            search(right_node->m_value);
        }else{
            m_hp.RetireHazard(right_node);
        }
        return true;
    }

    bool Find(const T& value)
    {
        auto [exists, left_node, right_node] = search(value);
        return exists;
    }

    [[maybe_unused]]
    void PrintUnsafe()
    {
        constexpr int entries_per_line = 2;
        std::lock_guard<std::mutex> lock{iolock};

        Node *curr = m_head;
        int count = 0;

        while(curr) {

            auto name = (curr == m_head) ? "Head"
                      : (curr == m_tail) ? "Tail"
                      : "Node";

            std::stringstream stream;
            stream << std::setfill('.') << std::setw(8) << curr->m_value;

            bool newline = (count % entries_per_line == 0);
            if(count > 0) {
                pe::log_ex(std::cout, nullptr, TextColor::eGreen, "", false, newline,
                    " -> ");
            }
            pe::log_ex(std::cout, nullptr, TextColor::eWhite, "", newline, false,
                "[", name, ":", curr, " (");
            pe::log_ex(std::cout, nullptr, TextColor::eBlue, "", false, false,
                stream.str());
            pe::log_ex(std::cout, nullptr, TextColor::eWhite, "", false, (curr == m_tail),
                ") marked:", is_marked_reference(curr), "]");

            curr = curr->m_next.load(std::memory_order_relaxed);
            count++;
        }
    }
};

}; //namespace pe

