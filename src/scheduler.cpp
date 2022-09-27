export module scheduler;
export import <coroutine>;

import <queue>;
import <cstdint>;
import <thread>;
import <mutex>;
import <condition_variable>;
import <memory>;
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
template <typename T>
struct Coroutine
{
    std::coroutine_handle<T> m_handle;

    Coroutine(std::coroutine_handle<T> handle)
        : m_handle{handle}
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
    }

    Coroutine& operator=(Coroutine&& other) noexcept
    {
        if(this == &other)
            return *this;
        if(m_handle)
            m_handle.destroy();
        std::swap(m_handle, other.m_handle);
        return *this;
    }

    ~Coroutine()
    {
        if(m_handle)
            m_handle.destroy();
    }
};

using UntypedCoroutine = Coroutine<void>;

template <typename T>
using SharedCoroutinePtr = std::shared_ptr<Coroutine<T>>;

template <typename T>
using WeakCoroutinePtr = std::weak_ptr<Coroutine<T>>;

/*
 * A TaskValueAwaitable / TaskValuePromise pair implement 
 * the mechanics of awaiting on a value from a task, be
 * that from a co_yield or a co_return.
 */
template <typename T, typename... Args>
struct TaskValueAwaitable
{
    using promise_type = TaskValuePromise<T, Args...>;
    using handle_type = std::coroutine_handle<TaskValuePromise<T, Args...>>;

    Scheduler&                       m_scheduler;
    SharedCoroutinePtr<promise_type> m_coro;

    TaskValueAwaitable(Scheduler& scheduler, SharedCoroutinePtr<promise_type> coro)
        : m_scheduler{scheduler}
        , m_coro{coro}
    {}

    bool await_ready() noexcept;
    void await_suspend(handle_type handle) noexcept;

    T await_resume() noexcept;
};

template <typename T, typename... Args>
struct TaskValuePromise
{
    using promise_type = TaskValuePromise<T, Args...>;
    using coroutine_type = Coroutine<promise_type>;
    using task_type = std::enable_shared_from_this<void>;

    T                              m_value;
    std::exception_ptr             m_exception;
    Scheduler&                     m_scheduler;
    uint32_t                       m_priority;
    WeakCoroutinePtr<promise_type> m_coro;

    /* Keep around a shared pointer to the Task instance which has the
     * 'Run' coroutine method. This way we will prevent destruction of
     * that instance until the 'Run' method runs to completion and the 
     * promise destructor is invoked, allowing us to safely use the 
     * 'this' pointer in the method without worrying about the object 
     * lifetime.
     */
    std::shared_ptr<task_type>     m_task;

    TaskHandle<T, Args...>         get_return_object();
    void                           unhandled_exception();
    std::suspend_always            initial_suspend();
    std::suspend_always            final_suspend() noexcept;

    template <typename U>
    TaskValueAwaitable<T, Args...> await_transform(U&& value);

    template <std::convertible_to<T> From>
    std::suspend_always            yield_value(From&& value);

    template <std::convertible_to<T> From>
    void                           return_value(From&& value);

    TaskValuePromise(auto& task, Args&... args);
};

/*
 * A TaskReturnAwaitable / TaskReturnPromise pair implement 
 * the mechanics of waiting on a co_return statement from a
 * task.
 */
template <typename T, typename... Args>
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
    explicit TaskHandle(coroutine_ptr_type&& coroutine);

    T Value();
    void Resume();
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

    template <typename T, typename... Args>
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

template <typename T, typename... Args>
bool TaskValueAwaitable<T, Args...>::await_ready() noexcept
{
    return false;
}

template <typename T, typename... Args>
void TaskValueAwaitable<T, Args...>::await_suspend(handle_type handle) noexcept
{
    const auto& promise = handle.promise();
    m_scheduler.enqueue_task({promise.m_priority, promise.m_coro.lock()});
}

template <typename T, typename... Args>
T TaskValueAwaitable<T, Args...>::await_resume() noexcept
{
    auto& promise = m_coro->m_handle.promise();
    return promise.m_value;
}

template <typename T, typename... Args>
TaskHandle<T, Args...> TaskValuePromise<T, Args...>::get_return_object()
{
    auto handle = std::coroutine_handle<TaskValuePromise<T, Args...>>::from_promise(*this);
    auto coro = std::make_shared<Coroutine<promise_type>>(handle);
    m_coro = coro;
    return TaskHandle<T, Args...>{std::move(coro)};
}

template <typename T, typename... Args>
void TaskValuePromise<T, Args...>::unhandled_exception()
{
    m_exception = std::current_exception();
}

template <typename T, typename... Args>
std::suspend_always TaskValuePromise<T, Args...>::initial_suspend()
{
    m_scheduler.enqueue_task({m_priority, m_coro.lock()});
    return {};
}

template <typename T, typename... Args>
std::suspend_always TaskValuePromise<T, Args...>::final_suspend() noexcept
{
    return {};
}

template <typename T, typename... Args>
template <typename U>
TaskValueAwaitable<T, Args...> TaskValuePromise<T, Args...>::await_transform(U&& value)
{
    return {m_scheduler, value.m_coro};
}

template <typename T, typename... Args>
template <std::convertible_to<T> From>
std::suspend_always TaskValuePromise<T, Args...>::yield_value(From&& value)
{
    m_value = std::forward<From>(value);
    m_scheduler.enqueue_task({m_priority, m_coro.lock()});
    return {};
}

template <typename T, typename... Args>
template <std::convertible_to<T> From>
void TaskValuePromise<T, Args...>::return_value(From&& value)
{
    m_value = std::forward<From>(value);
}

template <typename T, typename... Args>
TaskValuePromise<T, Args...>::TaskValuePromise(auto& task, Args&... args)
    : m_value{}
    , m_exception{}
    , m_scheduler{task.Scheduler()}
    , m_priority{task.Priority()}
    , m_task{static_pointer_cast<task_type>(task.shared_from_this())}
{}

template <typename T, typename... Args>
TaskHandle<T, Args...>::TaskHandle(coroutine_ptr_type&& coroutine)
    : m_coro{coroutine}
{}

template <typename T, typename... Args>
T TaskHandle<T, Args...>::Value()
{
    return m_coro->m_handle.promise().m_value;
}

template <typename T, typename... Args>
void TaskHandle<T, Args...>::Resume()
{
    if(*m_coro && !m_coro->done())
        m_coro->resume();
}

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

        coro->m_handle.resume();
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

