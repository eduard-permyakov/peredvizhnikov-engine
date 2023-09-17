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

module;

#include <stddef.h>

#ifdef __linux__
#include <linux/futex.h>
#include <sys/syscall.h>
#endif

export module sync:io_pool;

import :worker_pool;
import platform;
import shared_ptr;
import lockfree_sequenced_queue;
import logger;
import assert;
import futex;
import concurrency;

import <any>;
import <optional>;
import <string>;
import <atomic>;
import <array>;
import <thread>;
import <coroutine>;
import <memory>;
import <limits>;
import <optional>;

namespace pe{

inline constexpr unsigned kNumIOThreads = 64;

/*****************************************************************************/
/* FUTEX                                                                     */
/*****************************************************************************/

int futex(int *uaddr, int futex_op, int val, const struct timespec *timeout,
    int* uaddr2, int val3)
{
    return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}

/*****************************************************************************/
/* IO RESULT                                                                 */
/*****************************************************************************/

template <typename T>
class IOResult
{
private:

    using ResultType = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

    ResultType         m_result;
    std::exception_ptr m_exception;

public:

    template <typename U = T>
    requires (!std::is_void_v<U>)
    void Publush(U&& result)
    {
        m_result = std::move(result);
    }

    template <typename U = T>
    requires (!std::is_void_v<U>)
    U&& Consume()
    {
        return std::move(m_result);
    }

    void SetException(std::exception_ptr exc)
    {
        m_exception = exc;
    }

    void TryRethrowException()
    {
        if(m_exception) {
            std::rethrow_exception(m_exception);
        }
    }
};

/*****************************************************************************/
/* IO AWAITABLE                                                              */
/*****************************************************************************/

export class IOPool;

export
template <std::invocable Callable>
struct IOAwaitable
{
private:

    using ReturnType = std::invoke_result_t<Callable>;
    using ResultPtrType = pe::shared_ptr<IOResult<ReturnType>>;

    Scheduler&     m_scheduler;
    Schedulable    m_awaiter;
    IOPool        *m_pool;
    Callable       m_callable;
    ResultPtrType  m_result;

public:

    IOAwaitable(Scheduler& sched, Schedulable awaiter, IOPool *pool, Callable callable)
        : m_scheduler{sched}
        , m_awaiter{awaiter}
        , m_pool{pool}
        , m_callable{callable}
        , m_result{pe::make_shared<IOResult<ReturnType>>()}
    {}

    bool await_ready() noexcept;

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> awaiter)
    {
        return true;
    }

    template <typename Result = ReturnType>
    requires (!std::is_same_v<Result, void>)
    ReturnType await_resume() const
    {
        m_result->TryRethrowException();
        return std::move(m_result->Consume());
    }

    template <typename Result = ReturnType>
    requires (std::is_same_v<Result, void>)
    ReturnType await_resume() const
    {
        m_result->TryRethrowException();
    }
};

/*****************************************************************************/
/* IO WORK                                                                   */
/*****************************************************************************/

export void EnqueueTask(Scheduler *sched, Schedulable task);

class IOWork
{
private:

    Scheduler           *m_scheduler;
    Schedulable          m_awaiter;
    std::any             m_callable;
    pe::shared_ptr<void> m_result;
    void               (*m_work)(Scheduler*, Schedulable, std::any, pe::shared_ptr<void>);

public:

    IOWork() = default;

    template <std::invocable Callable>
    IOWork(Scheduler *scheduler, Schedulable awaiter, Callable callable, 
        pe::shared_ptr<IOResult<std::invoke_result_t<Callable>>> result)
        : m_scheduler{scheduler}
        , m_awaiter{awaiter}
        , m_callable{callable}
        , m_result{result}
        , m_work{+[](Scheduler *sched, Schedulable task, std::any callable, pe::shared_ptr<void> ptr){

            using ReturnType = std::invoke_result_t<Callable>;

            auto functor = any_cast<Callable>(callable);
            auto result = pe::static_pointer_cast<IOResult<ReturnType>>(ptr);

            try{
                if constexpr(!std::is_same_v<std::invoke_result_t<Callable>, void>) {
                    result->Publush(std::move(functor()));
                }else{
                    functor();
                }
            }catch(...) {
                result->SetException(std::current_exception());
            }
            EnqueueTask(sched, task);
        }}
    {}

    void Complete()
    {
        m_work(m_scheduler, m_awaiter, m_callable, m_result);
    }
};

/*****************************************************************************/
/* IO POOL                                                                   */
/*****************************************************************************/
/*
 * The IO Pool is a pool of threads that are 
 * meant to be primarily sleeping. The purpose
 * of the pool is to offload any blocking calls
 * so that the worker threads can saturate the 
 * system CPUs.
 */
export
class IOPool
{
private:

    struct alignas(8) QueueSize
    {
        uint32_t m_seqnum;
        uint32_t m_size;
    };

    using AtomicQueueSize = std::atomic<QueueSize>;

    static_assert(sizeof(AtomicQueueSize) == 8);

    std::array<std::thread, kNumIOThreads> m_io_workers;
    LockfreeSequencedQueue<IOWork>         m_io_work;
    pe::shared_ptr<AtomicQueueSize>        m_work_size;
    std::atomic_flag                       m_quit;
    std::atomic_uint32_t                   m_num_exited;

    static bool seqnum_passed(uint32_t a, uint32_t b);

    void wait_on_work(uint32_t *futex_addr);
    void signal_work(uint32_t *futex_addr);
    void signal_quit(uint32_t *futex_addr);
    void work();

public:

    IOPool();

    void EnqueueWork(IOWork work);
    void Quiesce();
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

template <std::invocable Callable>
bool IOAwaitable<Callable>::await_ready() noexcept
{
    m_pool->EnqueueWork(IOWork{&m_scheduler, m_awaiter, m_callable, m_result});
    return false;
}

IOPool::IOPool()
    : m_io_workers{}
    , m_io_work{}
    , m_work_size{pe::make_shared<AtomicQueueSize>()}
    , m_quit{}
    , m_num_exited{}
{
    for(int i = 0; i < kNumIOThreads; i++) {
        m_io_workers[i] = std::thread{&IOPool::work, this};
        SetThreadName(m_io_workers[i], "io-worker-" + std::to_string(i));
    }
}

void IOPool::EnqueueWork(IOWork work)
{
    m_io_work.ConditionallyEnqueue(+[](pe::shared_ptr<AtomicQueueSize> size, uint32_t seqnum,
        IOWork value) {

        auto expected = size->load(std::memory_order_relaxed);
        if(seqnum_passed(expected.m_seqnum, seqnum))
            return true;
        if(expected.m_seqnum == seqnum)
            return true;
        size->compare_exchange_strong(expected, {seqnum, expected.m_size + 1},
            std::memory_order_relaxed, std::memory_order_relaxed);
        return true;
    }, m_work_size, work);

    std::byte *base = reinterpret_cast<std::byte*>(m_work_size.get());
    uint32_t *size_addr = reinterpret_cast<uint32_t*>(base + offsetof(QueueSize, m_size));
    signal_work(size_addr);
}

void IOPool::Quiesce()
{
    pe::assert(std::this_thread::get_id() == g_main_thread_id);

    std::byte *base = reinterpret_cast<std::byte*>(m_work_size.get());
    uint32_t *size_addr = reinterpret_cast<uint32_t*>(base + offsetof(QueueSize, m_size));
    signal_quit(size_addr);

    for(auto& thread : m_io_workers) {
        thread.join();
    }
}

bool IOPool::seqnum_passed(uint32_t a, uint32_t b)
{
    return (static_cast<int32_t>((b) - (a)) < 0);
}

void IOPool::wait_on_work(uint32_t *futex_addr)
{
    while(true) {
        int *addr = reinterpret_cast<int*>(futex_addr);
        int futex_rc = futex(addr, FUTEX_WAIT_PRIVATE, 0, nullptr, nullptr, 0);
        if(futex_rc == -1) {
            /* the size has already changed */
            if(errno == EAGAIN)
                break;
            char errbuff[256];
            strerror_r(errno, errbuff, sizeof(errbuff));
            throw std::runtime_error{"Error waiting on futex:" + std::string{errbuff}};
        }
        auto value = m_work_size->load(std::memory_order_relaxed);
        if(value.m_size == 0)
            continue;
        break;
    }
}

void IOPool::signal_work(uint32_t *futex_addr)
{
    Backoff backoff{10, 1'000, 10'000};
    QueueSize old_size = m_work_size->load(std::memory_order_relaxed);
    while(true) {
        int *addr = reinterpret_cast<int*>(futex_addr);
        int futex_rc = futex(addr, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
        if(futex_rc == -1) {
            char errbuff[256];
            strerror_r(errno, errbuff, sizeof(errbuff));
            throw std::runtime_error{"Error waiting on futex:" + std::string{errbuff}};
        }
        if(futex_rc > 0)
            break;
        if(backoff.TimedOut())
            break;

        /* The following check guarantees that there is at least one
         * worker 'active'. (i.e. we detected with certainty that at
         * least one dequeue operation has taken place since we've 
         * enqueued the work item, which in turn guarantees that 
         * this worker will eventually get to processing our newly
         * enqueued work item).
         */
        QueueSize size = m_work_size->load(std::memory_order_relaxed);
        if(size.m_size == 0 || (size.m_size < old_size.m_size))
            break;
        old_size = size;
    }
}

void IOPool::signal_quit(uint32_t *futex_addr)
{
    m_quit.test_and_set(std::memory_order_relaxed);
    m_work_size->store({std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint32_t>::max()},
        std::memory_order_relaxed);

    int total_woken = 0;
    while(m_num_exited.load(std::memory_order_relaxed) < kNumIOThreads) {

        int *addr = reinterpret_cast<int*>(futex_addr);
        int futex_rc = futex(addr, FUTEX_WAKE_PRIVATE, kNumIOThreads, nullptr, nullptr, 0);
        if(futex_rc == -1) {
            char errbuff[256];
            strerror_r(errno, errbuff, sizeof(errbuff));
            throw std::runtime_error{"Error waiting on futex:" + std::string{errbuff}};
        }
        total_woken += futex_rc;
        if(total_woken == kNumIOThreads)
            break;
    }
}

void IOPool::work()
{
    while(true) {

        if(m_quit.test(std::memory_order_relaxed)) {
            m_num_exited.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        /* Attempt to dequeue a work item while atomically
         * decrementing the queue size.
         */
        auto ret = m_io_work.ConditionallyDequeue(+[](pe::shared_ptr<AtomicQueueSize> size,
            uint32_t seqnum, IOWork value){

            auto expected = size->load(std::memory_order_relaxed);
            if(seqnum_passed(expected.m_seqnum, seqnum))
                return true;
            if(expected.m_seqnum == seqnum)
                return true;
            size->compare_exchange_strong(expected, {seqnum, expected.m_size - 1},
                std::memory_order_relaxed, std::memory_order_relaxed);
            return true;
        }, m_work_size);

        /* If there is no work to be done, block until
         * the queue size becomes non-zero.
         */
        if(!ret.first.has_value()) {
            std::byte *base = reinterpret_cast<std::byte*>(m_work_size.get());
            uint32_t *size_addr = reinterpret_cast<uint32_t*>(base + offsetof(QueueSize, m_size));
            wait_on_work(size_addr);
            continue;
        }

        auto work = ret.first.value();
        work.Complete();
    }
}

} //namespace pe

