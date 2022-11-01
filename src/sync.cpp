export module sync;
export import :scheduler;

import logger;
import concurrency;
import meta;

import <coroutine>;
import <cstdint>;
import <atomic>;
import <vector>;
import <functional>;
import <string>;

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
        awaitable.m_next.store(expected.m_awaiters_head, std::memory_order_release);
        do{
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

    struct Awaitable
    {
        Barrier&                m_barrier;
        Schedulable             m_schedulable;
        uint16_t                m_phase;
        std::atomic<Awaitable*> m_next;

        Awaitable(Barrier& barrier, uint16_t phase)
            : m_barrier{barrier}
            , m_schedulable{}
            , m_phase{phase}
            , m_next{nullptr}
        {}

        /* await_transform requires a copy constructor */
        template <typename Other>
        requires (std::is_same_v<std::remove_cvref_t<Other>, Awaitable>)
        Awaitable(Other&& other)
            : m_barrier{other.m_barrier}
            , m_schedulable{}
            , m_phase{other.m_phase}
            , m_next{}
        {}

        bool await_ready() const noexcept
        {
            auto read = m_barrier.m_ctrl.Load(std::memory_order_relaxed);
            return (read.m_phase != m_phase);
        }

        template <typename PromiseType>
        bool await_suspend(std::coroutine_handle<PromiseType> awaiter)
        {
            m_schedulable = awaiter.promise().Schedulable();
            bool ret = m_barrier.try_add_awaiter_safe(*this);
            //pe::dbgprint(awaiter.promise().Name(), "in await_suspend", ret, "[", this, "]");
            return ret;
        }

        void await_resume() const noexcept {}
    };

    struct alignas(16) ControlBlock
    {
		uint16_t   m_phase;
        uint16_t   m_max;
		uint16_t   m_p0_counter;
		uint16_t   m_p1_counter;
        Awaitable *m_awaiters_head;
    };

    using AtomicControlBlock = DoubleQuadWordAtomic<ControlBlock>;

	AtomicControlBlock    m_ctrl;
	Scheduler& 		      m_scheduler;
    std::function<void()> m_completion;

    bool try_add_awaiter_safe(Awaitable& awaitable)
    {
        auto expected = m_ctrl.Load(std::memory_order_relaxed);
        ControlBlock next;
        awaitable.m_next.store(expected.m_awaiters_head, std::memory_order_release);
        do{
            if(expected.m_phase != awaitable.m_phase)
                return false;
            next = {expected.m_phase, expected.m_max, expected.m_p0_counter, 
                expected.m_p1_counter, &awaitable};
        }while(!m_ctrl.CompareExchange(expected, next,
            std::memory_order_release, std::memory_order_relaxed));

        /* Another thread will need to 'acquire' the control block
         * to make sure that the writes to 'm_schedulable' are visible.
         */
        return true;
    }

    void wake_awaiters(Awaitable *head)
    {
        //pe::dbgprint(" WAKE AWAITERS: ");
        while(head) {
            //pe::dbgprint("      >>> enqueued one!", head);
            m_scheduler.enqueue_task(head->m_schedulable);
            head = head->m_next.load(std::memory_order_acquire);
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
                std::memory_order_acq_rel, std::memory_order_acquire));

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
                (curr_phase == 0) ? us(curr_count - 1) : us(expected.m_max - 1),
                (curr_phase == 1) ? us(curr_count - 1) : us(expected.m_max - 1),
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
                std::memory_order_acq_rel, std::memory_order_acquire));

            wake_awaiters(expected.m_awaiters_head);
        }
    }

    Awaitable operator co_await() noexcept
    {
        uint16_t phase = m_ctrl.Load(std::memory_order_acquire).m_phase;
        return {*this, phase};
    }
};

}; //namespace pe

