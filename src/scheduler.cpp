export module scheduler;
export import <coroutine>;

import <queue>;
import <cstdint>;
import <thread>;
import <mutex>;
import <condition_variable>;
import <iostream>;

/*****************************************************************************/
/* MODULE INTERFACE                                                          */
/*****************************************************************************/

namespace pe{

export
template <typename T, typename... Args>
class TaskHandle;

export class Scheduler;

template <typename T, typename... Args>
struct TaskPromise;

export
template <typename T, typename... Args>
class Task;

export using tid_t = uint32_t;

template <typename T, typename... Args>
struct TaskAwaiter
{
    Scheduler& m_scheduler;

    TaskAwaiter(Scheduler& scheduler)
        : m_scheduler{scheduler}
    {}

    bool await_ready() noexcept;
    void await_suspend(std::coroutine_handle<TaskPromise<T, Args...>> handle) noexcept;
    void await_resume() noexcept;
};

template <typename T, typename... Args>
struct TaskPromise
{
    T                           m_value;
    std::exception_ptr          m_exception;
    Scheduler&                  m_scheduler;
    uint32_t                    m_priority;

    TaskHandle<T, Args...>      get_return_object();
    void                        unhandled_exception();
    TaskAwaiter<T, Args...>     initial_suspend();
    std::suspend_always         final_suspend() noexcept;
    TaskAwaiter<T, Args...>     await_transform(T&& retval);

    template <std::convertible_to<T> From>
    std::suspend_always         yield_value(From&& value);

    template <std::convertible_to<T> From>
    void                        return_value(From&& value);

    TaskPromise(Task<T, Args...>& self, Args&... args);
};

template <typename T, typename... Args>
class TaskHandle final
{
public:

    using promise_type = TaskPromise<T, Args...>;
    using handle_type = std::coroutine_handle<promise_type>;

    TaskHandle(const TaskHandle&) = delete;
    TaskHandle& operator=(const TaskHandle&) = delete;

    TaskHandle(TaskHandle&& other) noexcept;
    TaskHandle& operator=(TaskHandle&& other) noexcept;

    TaskHandle() = default;
    explicit TaskHandle(const handle_type coroutine);
    ~TaskHandle<T, Args...>();

    T Value();
    void Resume();

private:

    handle_type m_handle{0};
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
};

class Scheduler
{
private:

    struct Schedulable
    {
        uint32_t                m_priority;
        std::coroutine_handle<> m_handle;

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
void TaskAwaiter<T, Args...>::await_suspend(std::coroutine_handle<TaskPromise<T, Args...>> handle) noexcept
{
    const auto& promise = handle.promise();
    m_scheduler.enqueue_task({promise.m_priority, std::coroutine_handle<>(handle)});
}

template <typename T, typename... Args>
void TaskAwaiter<T, Args...>::await_resume() noexcept
{}

template <typename T, typename... Args>
TaskHandle<T, Args...> TaskPromise<T, Args...>::get_return_object()
{
    return TaskHandle<T, Args...>{
        std::coroutine_handle<TaskPromise<T, Args...>>::from_promise(*this)
    };
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
TaskAwaiter<T, Args...> TaskPromise<T, Args...>::await_transform(T&& retval)
{
    return {m_scheduler};
}

template <typename T, typename... Args>
template <std::convertible_to<T> From>
std::suspend_always TaskPromise<T, Args...>::yield_value(From&& value)
{
    m_value = std::forward<From>(value);

    auto handle = std::coroutine_handle<TaskPromise<T, Args...>>::from_promise(*this);
    m_scheduler.enqueue_task({m_priority, std::coroutine_handle<>(handle)});

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
TaskHandle<T, Args...>::TaskHandle(const handle_type coroutine)
    : m_handle{coroutine}
{}

template <typename T, typename... Args>
TaskHandle<T, Args...>::~TaskHandle()
{
    if(m_handle) {
        m_handle.destroy();
        m_handle = nullptr;
    }
}

template <typename T, typename... Args>
T TaskHandle<T, Args...>::Value()
{
    return m_handle.promise().m_value;
}

template <typename T, typename... Args>
void TaskHandle<T, Args...>::Resume()
{
    if(m_handle && !m_handle.done())
        m_handle.resume();
}

template <typename T, typename... Args>
TaskHandle<T, Args...>::TaskHandle(TaskHandle<T, Args...>&& other) noexcept
{
    if(this == &other)
        return;

    if(m_handle) {
        m_handle.destroy();
        m_handle = nullptr;
    }

    std::swap(m_handle, other.m_handle);
}

template <typename T, typename... Args>
TaskHandle<T, Args...>& TaskHandle<T, Args...>::operator=(TaskHandle&& other) noexcept
{
    if(this == &other)
        return *this;

    if(m_handle) {
        m_handle.destroy();
        m_handle = nullptr;
    }

    std::swap(m_handle, other.m_handle);
    return *this;
}

template <typename T, typename... Args>
Task<T, Args...>::Task(Scheduler& scheduler, uint32_t priority)
    : m_scheduler{scheduler}
    , m_priority{priority}
{}

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

        auto coro = m_ready_queue.top().m_handle;
        m_ready_queue.pop();
        lock.unlock();

        coro.resume();
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

