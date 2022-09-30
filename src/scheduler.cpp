export module scheduler;
export import <coroutine>;

import logger;

import <queue>;
import <cstdint>;
import <thread>;
import <mutex>;
import <condition_variable>;
import <memory>;
import <atomic>;
import <iostream>;

/*****************************************************************************/
/* MODULE INTERFACE                                                          */
/*****************************************************************************/

namespace pe{

/*
 * Forward declarations
 */
export
template <typename T, typename... Args>
class TaskHandle;

export class Scheduler;

template <typename T, typename... Args>
struct TaskValuePromise;

export
template <typename T, typename Derived, typename... Args>
class Task;

/*
 * RAII wrapper for the native coroutine_handle type
 */
template <typename PromiseType>
class Coroutine
{
private:

    std::coroutine_handle<PromiseType> m_handle;
    std::string                        m_name;

public:

    Coroutine(std::coroutine_handle<PromiseType> handle, std::string name)
        : m_handle{handle}
        , m_name{name}
    {}

    Coroutine(const Coroutine&) = delete;
    Coroutine& operator=(const Coroutine&) = delete;

    Coroutine(Coroutine&& other) noexcept
    {
        if(this == &other)
            return;
        if(m_handle)
            m_handle.destroy();
        std::swap(m_handle, other.m_handle);
        std::swap(m_name, other.m_name);
    }

    Coroutine& operator=(Coroutine&& other) noexcept
    {
        if(this == &other)
            return *this;
        if(m_handle)
            m_handle.destroy();
        std::swap(m_handle, other.m_handle);
        std::swap(m_name, other.m_name);
        return *this;
    }

    ~Coroutine()
    {
        if(m_handle)
            m_handle.destroy();
    }

    void Resume()
    {
        m_handle.resume();
    }

    template <typename T = PromiseType>
    requires (!std::is_void_v<T>)
    T& Promise()
    {
        return m_handle.promise();
    }

    const std::string& Name()
    {
        return m_name;
    }
};

using UntypedCoroutine = Coroutine<void>;

template <typename T>
using SharedCoroutinePtr = std::shared_ptr<Coroutine<T>>;

/*
 * Allows safe interleaved assignment and fetching from
 * different threads. Note that it is assumed that each
 * yielded value is always consumed exactly once.
 */
template <typename T>
class SynchronizedYieldValue
{
private:
    std::atomic_bool m_empty;
    T                m_value;
public:
    SynchronizedYieldValue()
        : m_empty{true}
        , m_value{}
    {}

    void Yield(T&& value)
    {
        /* Wait until the value is consumed */
        while(!m_empty.load(std::memory_order_consume));
        m_value = value;
        m_empty.store(false, std::memory_order_release);
    }

    T Consume()
    {
        /* Wait until the value is yielded */
        while(m_empty.load(std::memory_order_consume));
        T ret = m_value;
        m_empty.store(true, std::memory_order_release);
        return ret;
    }
};

/*
 * A TaskValueAwaitable / TaskValuePromise pair implement 
 * the mechanics of awaiting on a value from a task, be
 * that from a co_yield or a co_return.
 */
template <typename T, typename PromiseType>
struct TaskValueAwaitable
{
    using handle_type = std::coroutine_handle<PromiseType>;

    Scheduler&                      m_scheduler;
    SharedCoroutinePtr<PromiseType> m_coro;

    TaskValueAwaitable(Scheduler& scheduler, SharedCoroutinePtr<PromiseType> coro)
        : m_scheduler{scheduler}
        , m_coro{coro}
    {}

    bool await_ready() noexcept;
    void await_suspend(handle_type awaiter_handle) noexcept;

    T await_resume() noexcept;
};

template <typename T, typename... Args>
struct TaskValuePromise
{
    using promise_type = TaskValuePromise<T, Args...>;
    using coroutine_type = Coroutine<promise_type>;
    using task_type = std::enable_shared_from_this<void>;
    using awaitable_type = TaskValueAwaitable<T, promise_type>;

    SynchronizedYieldValue<T>        m_value;
    std::exception_ptr               m_exception;
    Scheduler&                       m_scheduler;
    uint32_t                         m_priority;
    SharedCoroutinePtr<promise_type> m_coro;

    /* Keep around a shared pointer to the Task instance which has the
     * 'Run' coroutine method. This way we will prevent destruction of
     * that instance until the 'Run' method runs to completion and the 
     * promise destructor is invoked, allowing us to safely use the 
     * 'this' pointer in the method without worrying about the object 
     * lifetime.
     */
    std::shared_ptr<task_type>       m_task;

    TaskHandle<T, Args...>           get_return_object();
    void                             unhandled_exception();
    std::suspend_never               initial_suspend();
    std::suspend_always              final_suspend() noexcept;

    template <typename U>
    awaitable_type                   await_transform(U&& value);

    template <std::convertible_to<T> From>
    std::suspend_always              yield_value(From&& value);

    template <std::convertible_to<T> From>
    void                             return_value(From&& value);

    TaskValuePromise(auto& task, Args&... args);
};

/*
 * A TaskReturnAwaitable / TaskReturnPromise pair implement 
 * the mechanics of waiting on a co_return statement from a
 * task.
 */
template <typename T, typename PromiseType>
struct TaskReturnAwaitable
{
};

template <typename T, typename... Args>
struct TaskReturnPromise
{
};

/*
 * A generic handle to refer to task. Can obtain different
 * kinds of awaitables from its' methods to allow the caller
 * to suspend until certain contitions have been met at a 
 * point when the task suspends.
 */
template <typename T, typename... Args>
class TaskHandle final
{
public:

    using promise_type = TaskValuePromise<T, Args...>;
    using coroutine_ptr_type = SharedCoroutinePtr<promise_type>;

    TaskHandle(const TaskHandle&) = delete;
    TaskHandle& operator=(const TaskHandle&) = delete;

    TaskHandle(TaskHandle&& other) noexcept = default;
    TaskHandle& operator=(TaskHandle&& other) noexcept = default;

    TaskHandle() = default;
    explicit TaskHandle(coroutine_ptr_type coroutine);

    void Join();

private:

    friend struct TaskValuePromise<T, Args...>;
    friend struct TaskReturnPromise<T, Args...>;

    /* Use a shared pointer for referring to the native coroutine
     * handle to allow the scheduler to retain it for detached tasks 
     */
    coroutine_ptr_type m_coro{0};
};

/*
 * An abstract base class for implementing user tasks. Yields
 * a TaskHandle from its' 'Run' method.
 */
template <typename T, typename Derived, typename... Args>
class Task : public std::enable_shared_from_this<void>
{
private:

    Scheduler& m_scheduler;
    uint32_t   m_priority;

    friend struct TaskValuePromise<T, Args...>;
    friend struct TaskReturnPromise<T, Args...>;

    /* Uninstantiatable type to prevent constructing 
     * the type from outside the 'Create' method. 
     */
    class TaskCreateToken 
    {
        TaskCreateToken() = default;
        friend class Task<T, Derived, Args...>;
    };

public:

    using handle_type = TaskHandle<T, Args...>;

    Task(TaskCreateToken token, Scheduler& scheduler, uint32_t priority = 0);
    virtual ~Task() = default;

    template <typename... ConstructorArgs>
    [[nodiscard]] static std::shared_ptr<Derived> Create(
        Scheduler& scheduler, uint32_t priority, ConstructorArgs... args)
    {
        return std::make_shared<Derived>(TaskCreateToken{}, scheduler, priority, args...);
    }

    [[nodiscard]] virtual handle_type Run(Args...) = 0;
    Scheduler& Scheduler() const;
    uint32_t Priority() const;
};

/*
 * Multithreaded task scheduler implementation.
 */
class Scheduler
{
private:

    struct Schedulable
    {
        uint32_t              m_priority;
        std::shared_ptr<void> m_handle;

        bool operator()(Schedulable lhs, Schedulable rhs)
        {
            return lhs.m_priority > rhs.m_priority;
        }
    };

    using queue_type = std::priority_queue<
        Schedulable, 
        std::vector<Schedulable>, 
        Schedulable
    >;

    queue_type               m_ready_queue;
    std::mutex               m_ready_lock;
    std::condition_variable  m_ready_cond;
    std::size_t              m_nworkers;
    std::vector<std::thread> m_worker_pool;

    void enqueue_task(Schedulable schedulable);
    void work();

    template <typename T, typename PromiseType>
    friend struct TaskValueAwaitable;

    template <typename T, typename... Args>
    friend struct TaskValuePromise;
    
public:
    Scheduler();
    void Run();
    void Shutdown();
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

template <typename T, typename PromiseType>
bool TaskValueAwaitable<T, PromiseType>::await_ready() noexcept
{
    return false;
}

template <typename T, typename PromiseType>
void TaskValueAwaitable<T, PromiseType>::await_suspend(handle_type awaiter_handle) noexcept
{
    const auto& promise = awaiter_handle.promise();
    m_scheduler.enqueue_task({promise.m_priority, promise.m_coro});
}

template <typename T, typename PromiseType>
T TaskValueAwaitable<T, PromiseType>::await_resume() noexcept
{
    auto& promise = m_coro->Promise();
    T ret = promise.m_value.Consume();
    if(promise.m_coro) {
        m_scheduler.enqueue_task({promise.m_priority, promise.m_coro});
    }
    return ret;
}

template <typename T, typename... Args>
TaskHandle<T, Args...> TaskValuePromise<T, Args...>::get_return_object()
{
    return TaskHandle<T, Args...>{m_coro};
}

template <typename T, typename... Args>
void TaskValuePromise<T, Args...>::unhandled_exception()
{
    m_exception = std::current_exception();
}

template <typename T, typename... Args>
std::suspend_never TaskValuePromise<T, Args...>::initial_suspend()
{
    return {};
}

template <typename T, typename... Args>
std::suspend_always TaskValuePromise<T, Args...>::final_suspend() noexcept
{
    return {};
}

template <typename T, typename... Args>
template <typename U>
typename TaskValuePromise<T, Args...>::awaitable_type TaskValuePromise<T, Args...>::await_transform(U&& value)
{
    return {m_scheduler, value.m_coro};
}

template <typename T, typename... Args>
template <std::convertible_to<T> From>
std::suspend_always TaskValuePromise<T, Args...>::yield_value(From&& value)
{
    m_value.Yield(std::forward<From>(value));
    return {};
}

template <typename T, typename... Args>
template <std::convertible_to<T> From>
void TaskValuePromise<T, Args...>::return_value(From&& value)
{
    m_coro = nullptr;
    m_value.Yield(std::forward<From>(value));
}

template <typename T, typename... Args>
TaskValuePromise<T, Args...>::TaskValuePromise(auto& task, Args&... args)
    : m_value{}
    , m_exception{}
    , m_scheduler{task.Scheduler()}
    , m_priority{task.Priority()}
    , m_coro{
        std::make_shared<Coroutine<promise_type>>(
            std::coroutine_handle<TaskValuePromise<T, Args...>>::from_promise(*this),
            typeid(task).name())}
    , m_task{static_pointer_cast<task_type>(task.shared_from_this())}
{}

template <typename T, typename... Args>
TaskHandle<T, Args...>::TaskHandle(coroutine_ptr_type coroutine)
    : m_coro{coroutine}
{}

template <typename T, typename... Args>
void TaskHandle<T, Args...>::Join()
{
}

template <typename T, typename Derived, typename... Args>
Task<T, Derived, Args...>::Task(TaskCreateToken token, class Scheduler& scheduler, uint32_t priority)
    : m_scheduler{scheduler}
    , m_priority{priority}
{}

template <typename T, typename Derived, typename... Args>
Scheduler& Task<T, Derived, Args...>::Scheduler() const
{
    return m_scheduler;
}

template <typename T, typename Derived, typename... Args>
uint32_t Task<T, Derived, Args...>::Priority() const
{
    return m_priority;
}

Scheduler::Scheduler()
    : m_ready_queue{}
    , m_ready_lock{}
    , m_ready_cond{}
    , m_nworkers{std::max(1u, std::thread::hardware_concurrency())}
    , m_worker_pool{}
{}

void Scheduler::work()
{
    while(true) {

        std::unique_lock<std::mutex> lock{m_ready_lock};
        if(m_ready_queue.empty()) {
            m_ready_cond.wait(lock, [&](){ return !m_ready_queue.empty(); });
        }

        auto coro = static_pointer_cast<UntypedCoroutine>(m_ready_queue.top().m_handle);
        m_ready_queue.pop();
        lock.unlock();

        coro->Resume();
    }
}

void Scheduler::enqueue_task(Schedulable schedulable)
{
    std::unique_lock<std::mutex> lock{m_ready_lock};
    m_ready_queue.push(schedulable);
    m_ready_cond.notify_one();
}

void Scheduler::Run()
{
    m_worker_pool.reserve(m_nworkers);
    for(int i = 0; i < m_nworkers; i++) {
        m_worker_pool.emplace_back(&Scheduler::work, this);
    }

    for(auto& thread : m_worker_pool) {
        thread.join();
    }
}

void Scheduler::Shutdown()
{

}

} // namespace pe

