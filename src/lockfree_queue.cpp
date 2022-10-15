export module lockfree_queue;

import platform;
import concurrency;

export import <optional>;
import <atomic>;
import <type_traits>;

namespace pe{

/* 
 * Implementation of a Michael and Scott lockfree queue.
 */
export
template <typename T, int Tag>
class LockfreeQueue
{
private:

    struct Node;

    struct alignas(16) Pointer
    {
        Node *m_ptr{nullptr};
        std::uintptr_t m_count{0};

        bool operator==(const Pointer& rhs) const;
        bool operator!=(const Pointer& rhs) const;
    };

    using AtomicPointer = DoubleQuadWordAtomic<Pointer>;

    struct Node
    {
        T m_value{};
        AtomicPointer m_next{};
    };

    AtomicPointer               m_head;
    AtomicPointer               m_tail;
    HPContext<Node, 2, 2, Tag>& m_hp;

    LockfreeQueue(Node *head)
        : m_head{head, uintptr_t{0}}
        , m_tail{head, uintptr_t{0}}
        , m_hp{HPContext<Node, 2, 2, Tag>::Instance()}
    {}

    LockfreeQueue()
        : LockfreeQueue(new Node{})
    {}

    LockfreeQueue(LockfreeQueue&&) = delete;
    LockfreeQueue(LockfreeQueue const&) = delete;
    LockfreeQueue& operator=(LockfreeQueue&&) = delete;
    LockfreeQueue& operator=(LockfreeQueue const&) = delete;

public:

    static LockfreeQueue& Instance()
    {
        static LockfreeQueue s_instance{};
        return s_instance;
    }

    ~LockfreeQueue()
    {
        std::optional<T> value;
        do{
            value = Dequeue();
        }while(value.has_value());
    }

    template <typename U = T>
    void Enqueue(U&& value)
    {
        Node *node = new Node{std::forward<U>(value), {}};
        Pointer tail, next;

        while(true) {

            tail = m_tail.Load(std::memory_order_acquire);
            auto tail_hazard = m_hp.AddHazard(0, tail.m_ptr);
            if(tail != m_tail.Load(std::memory_order_acquire))
                continue;

            next = tail.m_ptr->m_next.Load(std::memory_order_acquire);
            if(next.m_ptr == nullptr) {
                if(tail.m_ptr->m_next.CompareExchange(next, {node, next.m_count + 1},
                    std::memory_order_release, std::memory_order_relaxed))
                    break;
            }else{
                m_tail.CompareExchange(tail, {next.m_ptr, tail.m_count + 1},
                    std::memory_order_release, std::memory_order_relaxed);
            }
        }
        m_tail.CompareExchange(tail, {node, tail.m_count + 1},
            std::memory_order_release, std::memory_order_relaxed);
    }

    std::optional<T> Dequeue()
    {
        std::optional<T> ret{};
        Pointer head, tail, next;

        while(true) {

            head = m_head.Load(std::memory_order_acquire);
            auto head_hazard = m_hp.AddHazard(0, head.m_ptr);
            if(head != m_head.Load(std::memory_order_acquire))
                continue;

            tail = m_tail.Load(std::memory_order_relaxed);
            next = head.m_ptr->m_next.Load(std::memory_order_acquire);
            auto next_hazard = m_hp.AddHazard(1, next.m_ptr);
            if(head != m_head.Load(std::memory_order_relaxed))
                continue;

            if(next.m_ptr == nullptr)
                return std::nullopt;

            if(head.m_ptr == tail.m_ptr) {
                m_tail.CompareExchange(tail, {next.m_ptr, tail.m_count + 1},
                    std::memory_order_release, std::memory_order_relaxed);
                continue;
            }

            /* Read value before CAS, otherwise another dequeue might 
             * free the next node 
             */
            ret = std::move(next.m_ptr->m_value);
            if(m_head.CompareExchange(head, {next.m_ptr, head.m_count + 1},
                std::memory_order_release, std::memory_order_relaxed))
                break;
        }
        m_hp.RetireHazard(head.m_ptr);
        return ret;
    }
};

template <typename T, int Tag>
bool LockfreeQueue<T, Tag>::Pointer::operator==(const Pointer& rhs) const
{
   return (m_ptr == rhs.m_ptr) && (m_count == rhs.m_count);
}

template <typename T, int Tag>
bool LockfreeQueue<T, Tag>::Pointer::operator!=(const Pointer& rhs) const
{
  return !operator==(rhs);
}

}; //namespace pe
