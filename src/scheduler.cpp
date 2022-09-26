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
struct TaskPromise;

export
template <typename T, typename... Args>
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

template <typename T>
using SharedCoroutinePtr = std::shared_ptr<Coroutine<T>>;

template <typename T, typename... Args>
struct TaskAwaiter
{
    using handle_type = std::coroutine_handle<TaskPromise<T, Args...>>;

    Scheduler& m_scheduler;

    TaskAwaiter(Scheduler& scheduler)
        : m_scheduler{scheduler}
    {}

    bool await_ready() noexcept;
    void await_suspend(handle_type handle) noexcept;
    void await_resume() noexcept;
};

template <typename T, typename... Args>
struct TaskPromise
{
    using coroutine_type = Coroutine<TaskPromise<T, Args...>>;

    T                             m_value;
    std::exception_ptr            m_exception;
    Scheduler&                    m_scheduler;
    uint32_t                      m_priority;
    std::weak_ptr<coroutine_type> m_coro;

    TaskHandle<T, Args...>        get_return_object();
    void                          unhandled_exception();
    TaskAwaiter<T, Args...>       initial_suspend();
    std::suspend_always           final_suspend() noexcept;

    template <typename U>
    TaskAwaiter<T, Args...>       await_transform(U&& value);

    template <std::convertible_to<T> From>
    std::suspend_always           yield_value(From&& value);

    template <std::convertible_to<T> From>
    void                          return_value(From&& value);

    TaskPromise(Task<T, Args...>& self, Args&... args);
};

template <typename T, typename... Args>
class TaskHandle final
{
public:

    using promise_type = TaskPromise<T, Args...>;
    using coroutine_ptr_type = SharedCoroutinePtr<promise_type>;

    TaskHandle(const TaskHandle&) = delete;
    TaskHandle& operator=(const TaskHandle&) = delete;

    TaskHandle(TaskHandle&& other) noexcept = default;
    TaskHandle& operator=(TaskHandle&& other) noexcept = default;

    TaskHandle() = default;
    explicit TaskHandle(coroutine_ptr_type&& coroutine);

    T Value();
    void Resume();

private:

    /* Use a shared pointer for referring to the native coroutine
     * handle to allow the scheduler to retain it for detached tasks 
     */
    coroutine_ptr_type m_handle{0};
};

template <typename T, typename... Args>
class Task
{
private:
    Scheduler& m_scheduler;
    uint32_t   m_priority;

    friend struct TaskPromise<T, Args...>;
public:

    using handle_type = TaskHandle<T, Args...>;

    Task(Scheduler& scheduler, uint32_t priority = 0);
    [[nodiscard]] virtual handle_type Run(Args...) = 0;
    Scheduler& Scheduler();
};

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
    TaskAwaiter<T, Args...> schedule();

    template <typename T, typename... Args>
    friend struct TaskAwaiter;

    template <typename T, typename... Args>
    friend struct TaskPromise;
    
public:
    Scheduler();
    void Run();
    void Shutdown();
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

template <typename T, typename... Args>
bool TaskAwaiter<T, Args...>::await_ready() noexcept
{
    return false;
}

template <typename T, typename... Args>
void TaskAwaiter<T, Args...>::await_suspend(handle_type handle) noexcept
{
    const auto& promise = handle.promise();
    m_scheduler.enqueue_task({promise.m_priority, promise.m_coro.lock()});
}

template <typename T, typename... Args>
void TaskAwaiter<T, Args...>::await_resume() noexcept
{}

template <typename T, typename... Args>
TaskHandle<T, Args...> TaskPromise<T, Args...>::get_return_object()
{
    auto handle = std::coroutine_handle<TaskPromise<T, Args...>>::from_promise(*this);
    auto coro = std::make_shared<Coroutine<TaskPromise<T, Args...>>>(handle);
    m_coro = coro;
    return TaskHandle<T, Args...>{std::move(coro)};
}

template <typename T, typename... Args>
void TaskPromise<T, Args...>::unhandled_exception()
{
    m_exception = std::current_exception();
}

template <typename T, typename... Args>
TaskAwaiter<T, Args...> TaskPromise<T, Args...>::initial_suspend()
{
    return {m_scheduler};
}

template <typename T, typename... Args>
std::suspend_always TaskPromise<T, Args...>::final_suspend() noexcept
{
    return {};
}

template <typename T, typename... Args>
template <typename U>
TaskAwaiter<T, Args...> TaskPromise<T, Args...>::await_transform(U&& value)
{
    return {m_scheduler};
}

template <typename T, typename... Args>
template <std::convertible_to<T> From>
std::suspend_always TaskPromise<T, Args...>::yield_value(From&& value)
{
    m_value = std::forward<From>(value);
    m_scheduler.enqueue_task({m_priority, m_coro.lock()});
    return {};
}

template <typename T, typename... Args>
template <std::convertible_to<T> From>
void TaskPromise<T, Args...>::return_value(From&& value)
{
    m_value = std::forward<From>(value);
}

template <typename T, typename... Args>
TaskPromise<T, Args...>::TaskPromise(Task<T, Args...>& task, Args&... args)
    : m_value{}
    , m_exception{}
    , m_scheduler{task.m_scheduler}
    , m_priority{task.m_priority}
{}

template <typename T, typename... Args>
TaskHandle<T, Args...>::TaskHandle(coroutine_ptr_type&& coroutine)
    : m_handle{coroutine}
{}

template <typename T, typename... Args>
T TaskHandle<T, Args...>::Value()
{
    return m_handle->m_handle.promise().m_value;
}

template <typename T, typename... Args>
void TaskHandle<T, Args...>::Resume()
{
    if(*m_handle && !m_handle->done())
        m_handle->resume();
}

template <typename T, typename... Args>
Task<T, Args...>::Task(class Scheduler& scheduler, uint32_t priority)
    : m_scheduler{scheduler}
    , m_priority{priority}
{}

template <typename T, typename... Args>
Scheduler& Task<T, Args...>::Scheduler()
{
    return m_scheduler;
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

        auto coro = static_pointer_cast<Coroutine<void>>(m_ready_queue.top().m_handle);
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

template <typename T, typename... Args>
TaskAwaiter<T, Args...> Scheduler::schedule()
{
    return TaskAwaiter<T>{*this};
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

