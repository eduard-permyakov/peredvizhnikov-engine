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

export module lockfree_queue;

import platform;
import concurrency;
import hazard_ptr;

import <atomic>;
import <type_traits>;
import <optional>;

namespace pe{

/* 
 * Implementation of a Michael and Scott lockfree queue.
 */
export
template <typename T>
requires (std::is_default_constructible_v<T> 
      && (std::is_copy_constructible_v<T> || std::is_move_constructible_v<T>))
class LockfreeQueue
{
private:

    struct Node;

    struct alignas(16) Pointer
    {
        Node     *m_ptr{nullptr};
        uintptr_t m_count{0};

        bool operator==(const Pointer& rhs) const;
        bool operator!=(const Pointer& rhs) const;
    };

    using AtomicPointer = DoubleQuadWordAtomic<Pointer>;

    struct Node
    {
        T m_value{};
        AtomicPointer m_next{};
    };

    AtomicPointer         m_head;
    AtomicPointer         m_tail;
    HPContext<Node, 2, 2> m_hp;

    LockfreeQueue(Node *head)
        : m_head{head, uintptr_t{0}}
        , m_tail{head, uintptr_t{0}}
        , m_hp{}
    {}

    LockfreeQueue(LockfreeQueue&&) = delete;
    LockfreeQueue(LockfreeQueue const&) = delete;
    LockfreeQueue& operator=(LockfreeQueue&&) = delete;
    LockfreeQueue& operator=(LockfreeQueue const&) = delete;

public:

    LockfreeQueue()
        : LockfreeQueue(new Node{})
    {}

    ~LockfreeQueue()
    {
        std::optional<T> value;
        do{
            value = Dequeue();
        }while(value.has_value());

        Node *sentinel = m_head.Load(std::memory_order_relaxed).m_ptr;
        delete sentinel;
    }

    template <typename U = T>
    void Enqueue(U&& value)
    {
        Node *node = new Node{std::forward<U>(value), {}};
        Pointer tail, next;

        while(true) {

            tail = m_tail.Load(std::memory_order_acquire);
            auto tail_hazard = m_hp.AddHazard(0, tail.m_ptr);
            if(tail != m_tail.Load(std::memory_order_relaxed))
                continue;

            next = tail.m_ptr->m_next.Load(std::memory_order_acquire);
            if(next.m_ptr == nullptr) {

                AnnotateHappensBefore(__FILE__, __LINE__, &tail.m_ptr->m_next);
                if(tail.m_ptr->m_next.CompareExchange(next, {node, next.m_count + 1},
                    std::memory_order_release, std::memory_order_relaxed))
                    break;
            }else{

                AnnotateHappensBefore(__FILE__, __LINE__, &m_tail);
                m_tail.CompareExchange(tail, {next.m_ptr, tail.m_count + 1},
                    std::memory_order_release, std::memory_order_relaxed);
            }
        }
        AnnotateHappensBefore(__FILE__, __LINE__, &m_tail);
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
            if(head != m_head.Load(std::memory_order_relaxed))
                continue;

            tail = m_tail.Load(std::memory_order_acquire);
            next = head.m_ptr->m_next.Load(std::memory_order_acquire);
            AnnotateHappensAfter(__FILE__, __LINE__, &head.m_ptr->m_next);

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

            /* Read value before CAS and make sure to issue a release
             * barrier to prevent any of the reads from being reordered
             * after the CAS, as the node can be deleted as soon as the
             * CAS succeeds.
             */
            ret = next.m_ptr->m_value;
            AnnotateHappensBefore(__FILE__, __LINE__, &m_head);

            if(m_head.CompareExchange(head, {next.m_ptr, head.m_count + 1},
                std::memory_order_release, std::memory_order_relaxed))
                break;
        }
        m_hp.RetireHazard(head.m_ptr);
        return ret;
    }
};

template <typename T>
requires (std::is_default_constructible_v<T> 
      && (std::is_copy_constructible_v<T> || std::is_move_constructible_v<T>))
bool LockfreeQueue<T>::Pointer::operator==(const Pointer& rhs) const
{
    return (m_ptr == rhs.m_ptr) && (m_count == rhs.m_count);
}

template <typename T>
requires (std::is_default_constructible_v<T> 
      && (std::is_copy_constructible_v<T> || std::is_move_constructible_v<T>))
bool LockfreeQueue<T>::Pointer::operator!=(const Pointer& rhs) const
{
    return !operator==(rhs);
}

}; //namespace pe

