export module sync:io_pool;

import :worker_pool;
import platform;
import shared_ptr;
import lockfree_sequenced_queue;
import logger;
import assert;
import futex;

import <any>;
import <optional>;
import <string>;
import <atomic>;
import <array>;
import <thread>;
import <coroutine>;
import <memory>;
import <limits>;

namespace pe{

inline constexpr unsigned kNumIOThreads = 32;

/*****************************************************************************/
/* FUTEX                                                                     */
/*****************************************************************************/

int futex(int *uaddr, int futex_op, int val, const struct timespec *timeout,
    int* uaddr2, int val3)
{
    return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}

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
    using ResultPtrType = pe::shared_ptr<pe::atomic_shared_ptr<ReturnType>>;

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
        , m_result{pe::make_shared<pe::atomic_shared_ptr<ReturnType>>()}
    {}

    bool await_ready() noexcept;

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> awaiter)
    {
        return (m_result->load(std::memory_order_relaxed) == nullptr);
    }

    template <typename Result = std::invoke_result_t<Callable>>
    requires (!std::is_same_v<Result, void>)
    std::invoke_result_t<Callable> await_resume() const noexcept
    {
        auto ret = m_result->exchange(nullptr, std::memory_order_acquire);
        return *ret;
    }

    template <typename Result = std::invoke_result_t<Callable>>
    requires (std::is_same_v<Result, void>)
    std::invoke_result_t<Callable> await_resume() const noexcept
    {
        m_result->exchange(nullptr, std::memory_order_relaxed);
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

    static inline const pe::shared_ptr<void> s_completed_marker = pe::static_pointer_cast<void>(
        pe::make_shared<std::monostate>()
    );

public:

    IOWork() = default;

    template <std::invocable Callable>
    IOWork(Scheduler *scheduler, Schedulable awaiter, Callable callable, 
        pe::shared_ptr<pe::atomic_shared_ptr<std::invoke_result_t<Callable>>> result)
        : m_scheduler{scheduler}
        , m_awaiter{awaiter}
        , m_callable{callable}
        , m_result{result}
        , m_work{+[](Scheduler *sched, Schedulable task, std::any callable, pe::shared_ptr<void> ptr){

            using ReturnType = std::invoke_result_t<Callable>;

            auto functor = any_cast<Callable>(callable);
            auto result = pe::static_pointer_cast<pe::atomic_shared_ptr<ReturnType>>(ptr);

            if constexpr(!std::is_same_v<std::invoke_result_t<Callable>, void>) {
                auto result = functor();
                result->store(result);
            }else{
                functor();
                result->store(s_completed_marker);
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
        size->compare_exchange_strong(expected, {seqnum, expected.m_size + 1});
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
        int futex_rc = futex(addr, FUTEX_WAIT, 0, nullptr, nullptr, 0);
        if(futex_rc == -1) {
            /* spurrious wakeup */
            if(errno == EAGAIN)
                continue;
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
    int *addr = reinterpret_cast<int*>(futex_addr);
    int futex_rc = futex(addr, FUTEX_WAKE, 1, nullptr, nullptr, 0);
    if(futex_rc == -1) {
        char errbuff[256];
        strerror_r(errno, errbuff, sizeof(errbuff));
        throw std::runtime_error{"Error waiting on futex:" + std::string{errbuff}};
    }
}

void IOPool::signal_quit(uint32_t *futex_addr)
{
    m_quit.test_and_set(std::memory_order_relaxed);
    m_work_size->store({0, std::numeric_limits<uint32_t>::max()},
        std::memory_order_relaxed);

    int *addr = reinterpret_cast<int*>(futex_addr);
    int futex_rc = futex(addr, FUTEX_WAKE, kNumIOThreads, nullptr, nullptr, 0);
    if(futex_rc == -1) {
        char errbuff[256];
        strerror_r(errno, errbuff, sizeof(errbuff));
        throw std::runtime_error{"Error waiting on futex:" + std::string{errbuff}};
    }
}

void IOPool::work()
{
    while(true) {

        if(m_quit.test(std::memory_order_relaxed))
            return;

        /* Attempt to dequeue a work item while atomically
         * decrementing the queue size.
         */
        auto ret = m_io_work.ConditionallyDequeue(+[](pe::shared_ptr<AtomicQueueSize> size,
            uint32_t seqnum, IOWork value){

            auto expected = size->load(std::memory_order_relaxed);
            if(expected.m_size == 0)
                return false;
            if(seqnum_passed(expected.m_seqnum, seqnum))
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

