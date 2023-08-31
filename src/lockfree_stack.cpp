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

export module lockfree_stack;

import platform;
import concurrency;
import meta;
import logger;
import assert;

import <atomic>;
import <array>;
import <optional>;
import <cstdlib>;

namespace pe{

class timeout_exception : public std::runtime_error 
{
public:
    timeout_exception() : std::runtime_error{""} {}
};

/*
 * Implementation of an Elimination Back-off Stack.
 * The stack is initialized with a fixed-sized memory pool for nodes,
 * such that no additional dynamic memory allocations are required.
 */
export
template <std::size_t Capacity, typename T>
requires (std::is_default_constructible_v<T>)
      && (std::is_copy_constructible_v<T> || std::is_move_constructible_v<T>)
class LockfreeStack
{
    struct Node;

    struct alignas(16) Pointer
    {
        Node     *m_ptr{nullptr};
        uintptr_t m_tag{0};

        bool operator==(const Pointer& rhs) const;
        bool operator!=(const Pointer& rhs) const;
    };

    using AtomicPointer = DoubleQuadWordAtomic<Pointer>;

    struct Node
    {
        T             m_value;
        AtomicPointer m_next;
    };

    /* Permits two threads to exhcange values of type T */
    struct Exchanger
    {
        enum class State : uint64_t
        {
            eEmpty,
            eWaiting,
            eBusy
        };

        struct alignas(16) StampedReference
        {
            Node  *m_ptr{nullptr};
            State  m_state{State::eEmpty};
        };

        using AtomicStampedReference = DoubleQuadWordAtomic<StampedReference>;

        AtomicStampedReference m_slot{};

        Node *Exchange(Node *item, long timeout);
    };

    static constexpr std::size_t kEliminationArraySize = 32;
    static constexpr std::size_t kTimeoutMicroSeconds = 1000;

    std::array<Node, Capacity>                   m_nodes;
    std::array<Exchanger, kEliminationArraySize> m_elimination_array;
    AtomicPointer                                m_head;
    AtomicPointer                                m_freehead;

    LockfreeStack(LockfreeStack&&) = delete;
    LockfreeStack(LockfreeStack const&) = delete;
    LockfreeStack& operator=(LockfreeStack&&) = delete;
    LockfreeStack& operator=(LockfreeStack const&) = delete;

    Node *allocate()
    {
        do{
            Pointer freehead = m_freehead.Load(std::memory_order_acquire);
            if(!freehead.m_ptr)
                return nullptr;

            Pointer next = freehead.m_ptr->m_next.Load(std::memory_order_acquire);
            AnnotateHappensBefore(__FILE__, __LINE__, &m_freehead);

            if(m_freehead.CompareExchange(freehead, 
                {next.m_ptr, freehead.m_tag + 1},
                std::memory_order_release, std::memory_order_relaxed)) {

                /* The thread which succeeded in the CAS now holds exclusive
                 * ownership of the node.
                 */
                AnnotateHappensAfter(__FILE__, __LINE__, &m_freehead);
                return freehead.m_ptr;
            }
        }while(true);
    }

    void deallocate(Node *node)
    {
        do{
            Pointer freehead = m_freehead.Load(std::memory_order_acquire);
            Pointer next = node->m_next.Load(std::memory_order_acquire);

            AnnotateHappensBefore(__FILE__, __LINE__, &node->m_next);
            if(!node->m_next.CompareExchange(next, {freehead.m_ptr, next.m_tag + 1}, 
                std::memory_order_release, std::memory_order_relaxed)) {
                continue;
            }
            AnnotateHappensAfter(__FILE__, __LINE__, &node->m_next);

            AnnotateHappensBefore(__FILE__, __LINE__, &m_freehead);
            if(m_freehead.CompareExchange(freehead, {node, freehead.m_tag + 1},
                std::memory_order_release, std::memory_order_relaxed)) {

                AnnotateHappensAfter(__FILE__, __LINE__, &m_freehead);
                break;
            }

        }while(true);
    }

    __attribute__((no_sanitize("thread"), noinline))
    T unsafe_copy(const T& from)
    {
        return from;
    }

    Node *visit(Node *value)
    {
        std::size_t slot = std::rand() % kEliminationArraySize;
        return m_elimination_array[slot].Exchange(value, kTimeoutMicroSeconds);
    }

    bool try_push(Pointer& head, Node *node)
    {
        AnnotateHappensBefore(__FILE__, __LINE__, &m_head);
        if(m_head.CompareExchange(head, {node, head.m_tag + 1},
            std::memory_order_release, std::memory_order_relaxed)) {

            AnnotateHappensAfter(__FILE__, __LINE__, &m_head);
            return true;
        }
        return false;
    }

    std::pair<std::optional<T>, bool> try_pop()
    {
        Pointer head = m_head.Load(std::memory_order_acquire);
        AnnotateHappensAfter(__FILE__, __LINE__, &m_head);
        if(head.m_ptr == nullptr)
            return {std::nullopt, true};

        /* It's possible that the head node has already been popped
         * by a different thread. In this case, we will be reading
         * from a node that may have already been free'd and re-used.
         * However, due to the fact that the nodes are never returned
         * to the operating system, this race is benign. We will read
         * some undefined values but the subsequent CAS will ensure
         * the Pop() will not succeed.
         */
        Pointer next = head.m_ptr->m_next.Load(std::memory_order_acquire);
        auto ret = unsafe_copy(head.m_ptr->m_value);
        AnnotateHappensBefore(__FILE__, __LINE__, &m_head);

        if(m_head.CompareExchange(head, {next.m_ptr, head.m_tag + 1},
            std::memory_order_release, std::memory_order_relaxed)) {

            AnnotateHappensAfter(__FILE__, __LINE__, &m_head);
            deallocate(head.m_ptr);
            return {ret, true};
        }

        return {std::nullopt, false};
    }

public:

    LockfreeStack()
        : m_nodes{}
        , m_elimination_array{}
        , m_head{nullptr, uintptr_t{0}}
        , m_freehead{m_nodes.data(), uintptr_t{0}}
    {
        for(int i = 0; i < Capacity-1; i++) {
            m_nodes[i].m_next.Store({&m_nodes[i + 1], uintptr_t{0}});
        }
        m_nodes[Capacity-1].m_next.Store({nullptr, uintptr_t{0}});
    }

    template <typename U = T>
    bool Push(U&& value)
    {
        Node *node = allocate();
        if(!node)
            return false;

        node->m_value = std::forward<U>(value);
        while(true) {

            Pointer head = m_head.Load(std::memory_order_acquire);
            Pointer next = node->m_next.Load(std::memory_order_acquire);
            node->m_next.Store({head.m_ptr, next.m_tag + 1}, std::memory_order_release);

            if(try_push(head, node))
                break;

            try{
                Node *other = visit(node);
                if(!other) {
                    /* Exchanged with Pop() */
                    break;
                }
            }catch(timeout_exception& e) {}
        }
        return true;
    }

    std::optional<T> Pop()
    {
        while(true) {

            auto ret = try_pop();
            if(ret.second)
                return ret.first;

            try{
                Node *other = visit(nullptr);
                if(other) {
                    /* Exchanged with Push(). After a successful exchange, we have exclusive
                     * ownership of this node. 
                     */
                    auto ret = other->m_value;
                    deallocate(other);
                    return ret;
                }
            }catch(timeout_exception& e) {}
        }
    }
};

template <std::size_t Capacity, typename T>
requires (std::is_default_constructible_v<T>)
      && (std::is_copy_constructible_v<T> || std::is_move_constructible_v<T>)
bool LockfreeStack<Capacity, T>::Pointer::operator==(const Pointer& rhs) const
{
    return (m_ptr == rhs.m_ptr) && (m_tag == rhs.m_tag);
}

template <std::size_t Capacity, typename T>
requires (std::is_default_constructible_v<T>)
      && (std::is_copy_constructible_v<T> || std::is_move_constructible_v<T>)
bool LockfreeStack<Capacity, T>::Pointer::operator!=(const Pointer& rhs) const
{
    return !operator==(rhs);
}

template <std::size_t Capacity, typename T>
requires (std::is_default_constructible_v<T>)
      && (std::is_copy_constructible_v<T> || std::is_move_constructible_v<T>)
LockfreeStack<Capacity, T>::Node*
LockfreeStack<Capacity, T>::Exchanger::Exchange(Node *item, long timeout)
{
    uint32_t begin = rdtsc_before();
    Backoff backoff{10, 1'000, 0};

    while(true) {

        uint32_t now = rdtsc_after();
        if((int32_t)(now - begin) >= timeout) {
            throw timeout_exception{};
        }

        StampedReference slot = m_slot.Load(std::memory_order_acquire);
        State state = slot.m_state;

        switch(state) {
        case State::eEmpty:
            AnnotateHappensBefore(__FILE__, __LINE__, &m_slot);
            if(m_slot.CompareExchange(slot, {item, State::eWaiting},
                std::memory_order_release, std::memory_order_relaxed)) {

                while((int32_t)(rdtsc_after() - begin) < timeout) {
                    slot = m_slot.Load(std::memory_order_acquire);
                    if(slot.m_state == State::eBusy) {
                        m_slot.Store({nullptr, State::eEmpty}, std::memory_order_release);
                        AnnotateHappensAfter(__FILE__, __LINE__, &m_slot);
                        return slot.m_ptr;
                    }
                }

                StampedReference expected{item, State::eWaiting};
                if(m_slot.CompareExchange(expected, {nullptr, State::eEmpty})) {
                    throw timeout_exception{};
                }else{
                    m_slot.Store({nullptr, State::eEmpty}, std::memory_order_release);
                    AnnotateHappensAfter(__FILE__, __LINE__, &m_slot);
                    return expected.m_ptr;
                }
            }
            break;
        case State::eWaiting:
            AnnotateHappensBefore(__FILE__, __LINE__, &m_slot);
            if(m_slot.CompareExchange(slot, {item, State::eBusy},
                std::memory_order_release, std::memory_order_relaxed)) {
                AnnotateHappensAfter(__FILE__, __LINE__, &m_slot);
                return slot.m_ptr;
            }
            break;
        case State::eBusy:
            break;
        default:
            /* impossible */
            assert(0);
            break;
        }
        backoff.BackoffMaybe();
    }
}

}; //namespace pe

