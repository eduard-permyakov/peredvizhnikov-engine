export module sync;
export import :scheduler;

import logger;
import concurrency;

import <coroutine>;
import <cstdint>;
import <atomic>;
import <vector>;

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
            uint64_t count = m_latch.m_ctrl.Load(std::memory_order_relaxed).m_count;
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
        uint64_t   m_count;
        Awaitable *m_awaiters_head;
    };

    using AtomicControlBlock = DoubleQuadWordAtomic<ControlBlock>;

    Scheduler&         m_scheduler;
    AtomicControlBlock m_ctrl;

    bool try_add_awaiter_safe(Awaitable& awaitable)
    {
        auto expected = m_ctrl.Load(std::memory_order_relaxed);
        awaitable.m_next.store(expected.m_awaiters_head, std::memory_order_release);
        do{
            if(expected.m_count == 0)
                return false;
        }while(!m_ctrl.CompareExchange(expected, {expected.m_count, &awaitable},
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

    explicit Latch(Scheduler& scheduler, uint64_t count)
        : m_scheduler{scheduler}
        , m_ctrl{count, nullptr}
    {}

    void CountDown()
    {
        auto expected = m_ctrl.Load(std::memory_order_relaxed);
        do{
            if(expected.m_count == 0) [[unlikely]]
                throw std::runtime_error{"CountDown on expired latch."};
        }while(!m_ctrl.CompareExchange(expected, {expected.m_count - 1, expected.m_awaiters_head},
            std::memory_order_release, std::memory_order_relaxed));

        if(expected.m_count == 1) {
            wake_awaiters();
        }
    }

    bool TryWait()
    {
        uint64_t count = m_ctrl.Load(std::memory_order_relaxed).m_count;
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

}; //namespace pe

