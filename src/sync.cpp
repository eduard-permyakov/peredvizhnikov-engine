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

export module sync;
export import :scheduler;

import :system_tasks;
import logger;
import concurrency;
import meta;
import platform;
import assert; // TOOD: temp

import <coroutine>;
import <cstdint>;
import <atomic>;
import <vector>;
import <functional>;
import <thread>;
import <array>;
import <tuple>;
import <memory>;

namespace pe{

/*****************************************************************************/
/* LATCH                                                                     */
/*****************************************************************************/

export
class Latch
{
private:

    struct Awaitable
    {
        Latch&                  m_latch;
        Schedulable             m_schedulable;
        std::atomic<Awaitable*> m_next;

        Awaitable(Latch& latch)
            : m_latch{latch}
            , m_schedulable{}
            , m_next{nullptr}
        {}

        bool await_ready() const noexcept
        {
            uint64_t count = m_latch.m_ctrl.Load(std::memory_order_relaxed).m_max;
            return (count == 0); 
        }

        template <typename PromiseType>
        bool await_suspend(std::coroutine_handle<PromiseType> awaiter)
        {
            m_schedulable = awaiter.promise().Schedulable();
            return m_latch.try_add_awaiter_safe(*this);
        }

        void await_resume() const noexcept {}
    };

    struct alignas(16) ControlBlock
    {
        uint64_t   m_max;
        Awaitable *m_awaiters_head;
    };

    using AtomicControlBlock = DoubleQuadWordAtomic<ControlBlock>;

    AtomicControlBlock m_ctrl;
    Scheduler&         m_scheduler;

    bool try_add_awaiter_safe(Awaitable& awaitable)
    {
        auto expected = m_ctrl.Load(std::memory_order_relaxed);
        do{
            awaitable.m_next.store(expected.m_awaiters_head, std::memory_order_release);
            if(expected.m_max == 0)
                return false;
        }while(!m_ctrl.CompareExchange(expected, {expected.m_max, &awaitable},
            std::memory_order_release, std::memory_order_relaxed));
        return true;
    }

    void wake_awaiters()
    {
        Awaitable *curr = m_ctrl.Load(std::memory_order_acquire).m_awaiters_head;
        while(curr) {
            m_scheduler.enqueue_task(curr->m_schedulable);
            curr = curr->m_next.load(std::memory_order_acquire);
        }
    }

public:

    Latch(Latch&&) = delete;
    Latch(Latch const&) = delete;
    Latch& operator=(Latch&&) = delete;
    Latch& operator=(Latch const&) = delete;

    explicit Latch(Scheduler& scheduler, uint64_t count)
        : m_ctrl{count, nullptr}
        , m_scheduler{scheduler}
    {}

    void CountDown()
    {
        auto expected = m_ctrl.Load(std::memory_order_relaxed);
        do{
            if(expected.m_max == 0) [[unlikely]]
                throw std::runtime_error{"CountDown on expired latch."};
        }while(!m_ctrl.CompareExchange(expected, {expected.m_max - 1, expected.m_awaiters_head},
            std::memory_order_release, std::memory_order_relaxed));

        if(expected.m_max == 1) {
            wake_awaiters();
        }
    }

    bool TryWait()
    {
        uint64_t count = m_ctrl.Load(std::memory_order_relaxed).m_max;
        return (count == 0);
    }

    Awaitable ArriveAndWait()
    {
        CountDown();
        return {*this};
    }

    Awaitable operator co_await() noexcept
    {
        return {*this};
    }
};

/*****************************************************************************/
/* BARRIER                                                                   */
/*****************************************************************************/

export
class Barrier
{
private:

    struct AwaiterNode
    {
        std::atomic<AwaiterNode*> m_next;
        uint16_t                  m_phase;
        Schedulable               m_schedulable;

        AwaiterNode(uint16_t phase, Schedulable schedulable)
            : m_next{nullptr}
            , m_phase{phase}
            , m_schedulable{schedulable}
        {}
    };

    struct Awaitable
    {
        Barrier&                m_barrier;
        uint16_t                m_phase;

        Awaitable(Barrier& barrier, uint16_t phase)
            : m_barrier{barrier}
            , m_phase{phase}
        {}

        bool await_ready() const noexcept
        {
            auto read = m_barrier.m_ctrl.Load(std::memory_order_relaxed);
            return (read.m_phase != m_phase);
        }

        template <typename PromiseType>
        bool await_suspend(std::coroutine_handle<PromiseType> awaiter)
        {
            auto schedulable = awaiter.promise().Schedulable();
            auto node = std::make_unique<AwaiterNode>(m_phase, schedulable);
            AnnotateHappensBefore(__FILE__, __LINE__, &m_barrier.m_ctrl);
            return m_barrier.try_add_awaiter_safe(std::move(node));
        }

        void await_resume() const noexcept {}
    };

    struct alignas(16) ControlBlock
    {
        uint16_t     m_phase;
        uint16_t     m_max;
        uint16_t     m_p0_counter;
        uint16_t     m_p1_counter;
        AwaiterNode *m_awaiters_head;
    };

    using AtomicControlBlock = DoubleQuadWordAtomic<ControlBlock>;

    AtomicControlBlock    m_ctrl;
    Scheduler&            m_scheduler;
    std::function<void()> m_completion;

    bool try_add_awaiter_safe(std::unique_ptr<AwaiterNode> awaiter)
    {
        ControlBlock next;
        auto expected = m_ctrl.Load(std::memory_order_acquire);

        do{
            awaiter->m_next.store(expected.m_awaiters_head, std::memory_order_release);
            AnnotateHappensBefore(__FILE__, __LINE__, &m_ctrl);

            if(expected.m_phase != awaiter->m_phase)
                return false;
            next = {expected.m_phase, expected.m_max, expected.m_p0_counter, 
                expected.m_p1_counter, awaiter.get()};

        }while(!m_ctrl.CompareExchange(expected, next,
            std::memory_order_release, std::memory_order_relaxed));

        /* Another thread will need to 'acquire' the control block
         * to make sure that the writes to 'm_schedulable' are visible.
         */
        awaiter.release();
        return true;
    }

    void wake_awaiters(AwaiterNode *head)
    {
        while(head) {
            auto node = std::unique_ptr<AwaiterNode>(head);
            m_scheduler.enqueue_task(node->m_schedulable);
            head = node->m_next.load(std::memory_order_acquire);
        }
    }

    inline uint16_t us(unsigned long long value)
    {
        return static_cast<std::uint16_t>(value);
    }

public:

    Barrier(Barrier&&) = delete;
    Barrier(Barrier const&) = delete;
    Barrier& operator=(Barrier&&) = delete;
    Barrier& operator=(Barrier const&) = delete;

    template <std::invocable<> Callable>
    requires (std::is_nothrow_invocable_v<Callable>)
    explicit Barrier(Scheduler& scheduler, uint16_t count, Callable on_completion = {})
        : m_ctrl{us(0), count, count, count, nullptr}
        , m_scheduler{scheduler}
        , m_completion{on_completion}
    {}

    explicit Barrier(Scheduler& scheduler, uint16_t count)
        : m_ctrl{us(0), count, count, count, nullptr}
        , m_scheduler{scheduler}
        , m_completion{}
    {}

    void Arrive()
    {
        auto expected = m_ctrl.Load(std::memory_order_relaxed);
        ControlBlock next;
        do{
            int curr_phase = expected.m_phase % 2;
            uint16_t& curr_count = (curr_phase == 0) ? expected.m_p0_counter : expected.m_p1_counter;
            if(curr_count == 0) [[unlikely]]
                throw std::runtime_error{"Arrive on expired barrier."};
            next = {expected.m_phase, expected.m_max,
                (curr_phase == 0) ? us(curr_count - 1) : expected.m_p0_counter,
                (curr_phase == 1) ? us(curr_count - 1) : expected.m_p1_counter,
                expected.m_awaiters_head};
        }while(!m_ctrl.CompareExchange(expected, next,
            std::memory_order_release, std::memory_order_relaxed));

        int curr_phase = expected.m_phase % 2;
        uint16_t& curr_count = (curr_phase == 0) ? expected.m_p0_counter : expected.m_p1_counter;
        if(curr_count == 1) {

            if(m_completion) {
                m_completion();
            }
            /* As soon as we resume any of the awaiters, they 
             * are free to touch the barrier again, so reset
             * the control block state before then.
             */
            next = {
                us(expected.m_phase + 1), expected.m_max,
                (curr_phase == 1) ? expected.m_max : expected.m_p0_counter,
                (curr_phase == 0) ? expected.m_max : expected.m_p1_counter,
                nullptr
            };
            /* Reset control block state. Acquire semantics are necessary
             * here to ensure that all prior writes by suspending awaiters
             * are visible.
             */
            while(!m_ctrl.CompareExchange(expected, next,
                std::memory_order_acq_rel, std::memory_order_relaxed));

            AnnotateHappensAfter(__FILE__, __LINE__, &m_ctrl);
            wake_awaiters(expected.m_awaiters_head);
        }
    }

    Awaitable ArriveAndWait()
    {
        uint16_t phase = m_ctrl.Load(std::memory_order_acquire).m_phase;
        Arrive();
        return {*this, phase};
    }

    void ArriveAndDrop()
    {
        auto expected = m_ctrl.Load(std::memory_order_relaxed);
        ControlBlock next;
        do{
            int curr_phase = expected.m_phase % 2;
            uint16_t& curr_count = (curr_phase == 0) ? expected.m_p0_counter : expected.m_p1_counter;
            if(curr_count == 0) [[unlikely]]
                throw std::runtime_error{"Arrive on expired barrier."};
            if(expected.m_max == 0) [[unlikely]]
                throw std::runtime_error{"Drop on empty barrier."};
            next = {expected.m_phase, us(expected.m_max - 1),
                (curr_phase == 0) ? us(curr_count - 1) : expected.m_p0_counter,
                (curr_phase == 1) ? us(curr_count - 1) : expected.m_p1_counter,
                expected.m_awaiters_head};
        }while(!m_ctrl.CompareExchange(expected, next,
            std::memory_order_release, std::memory_order_relaxed));

        int curr_phase = expected.m_phase % 2;
        uint16_t& curr_count = (curr_phase == 0) ? expected.m_p0_counter : expected.m_p1_counter;
        if(curr_count == 1) {

            if(m_completion) {
                m_completion();
            }

            next = {
                us(expected.m_phase + 1), us(expected.m_max - 1),
                (curr_phase == 1) ? us(expected.m_max - 1) : expected.m_p0_counter,
                (curr_phase == 0) ? us(expected.m_max - 1) : expected.m_p1_counter,
                nullptr
            };
            /* reset control block state */
            while(!m_ctrl.CompareExchange(expected, next,
                std::memory_order_acq_rel, std::memory_order_relaxed));

            AnnotateHappensAfter(__FILE__, __LINE__, &m_ctrl);
            wake_awaiters(expected.m_awaiters_head);
        }
    }

    Awaitable operator co_await() noexcept
    {
        uint16_t phase = m_ctrl.Load(std::memory_order_acquire).m_phase;
        return {*this, phase};
    }
};

/*****************************************************************************/
/* SCHEDULER                                                                 */
/*****************************************************************************/

void Scheduler::start_system_tasks()
{
    std::ignore = QuitHandler::Create(*this, Priority::eCritical, 
        CreateMode::eLaunchSync, Affinity::eMainThread);

    std::ignore = ExceptionForwarder::Create(*this, Priority::eCritical,
        CreateMode::eLaunchSync, Affinity::eMainThread);
}

}; //namespace pe

