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
import <type_traits>;

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
struct TaskYieldPromise;

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

    friend class Scheduler;

    void Resume()
    {
        m_handle.resume();
    }

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

    template <typename T = PromiseType>
    requires (!std::is_void_v<T>)
    T& Promise() const
    {
        return m_handle.promise();
    }

    const std::string& Name() const
    {
        return m_name;
    }
};

enum class CoroutineState{
    eRunning,
    eDone
};

using UntypedCoroutine = Coroutine<void>;

template <typename PromiseType>
using SharedCoroutinePtr = std::shared_ptr<Coroutine<PromiseType>>;

/*
 * Empty type to be used as a required yield type from
 * void-return-typed coroutines.
 */
struct VoidType {};
export constexpr VoidType Void = VoidType{};

/*
 * Allows safe interleaved assignment and fetching from
 * different threads. Note that it is assumed that each
 * yielded value is always consumed exactly once.
 */
template <typename T>
class SynchronizedYieldValue
{
private:

    using value_type = std::conditional_t<std::is_void_v<T>, VoidType, T>;

    std::atomic_bool                 m_empty;
    [[no_unique_address]] value_type m_value;

public:

    SynchronizedYieldValue()
        : m_empty{true}
        , m_value{}
    {}

    template <typename U = T>
    void Yield(U&& value) requires (!std::is_void_v<U>)
    {
        /* Wait until the value is consumed */
        while(!m_empty.load(std::memory_order_consume));
        m_value = value;
        m_empty.store(false, std::memory_order_release);
    }

    template <typename U = T>
    T Consume() requires (!std::is_void_v<U>)
    {
        /* Wait until the value is yielded */
        while(m_empty.load(std::memory_order_consume));
        T ret = m_value;
        m_empty.store(true, std::memory_order_release);
        return ret;
    }

    /*
     * Even when the awaiting task does not consume a value
     * from the yield expression, perform the serialization
     * so we have a guarantee that all side-effects from the
     * yielding thread are visible to the awaiter.
     */
    template <typename U = T>
    void Yield() requires (std::is_void_v<U>)
    {
        while(!m_empty.load(std::memory_order_consume));
        m_empty.store(false, std::memory_order_release);
    }

    template <typename U = T>
    T Consume() requires (std::is_void_v<U>)
    {
        while(m_empty.load(std::memory_order_consume));
        m_empty.store(true, std::memory_order_release);
    }
};

/*
 * Same as SynchronizedYieldValue, but allowing the producer
 * to yield the value only a single time. All subsequent yields 
 * will be no-ops.
 */
template <typename T>
class SynchronizedSingleYieldValue
{
private:

    std::atomic_bool m_empty{true};
    T                m_value{};

public:

    bool Set(T&& value)
    {
        if(!m_empty.load(std::memory_order_consume))
            return false;
        m_value = value;
        m_empty.store(false, std::memory_order_release);
        return true;
    }

    T Get()
    {
        while(m_empty.load(std::memory_order_consume));
        return m_value;
    }
};

/*
 * A TaskYieldAwaitable / TaskYieldPromise pair implement 
 * the mechanics of awaiting on a yield from a task, be
 * that from a co_yield or a co_return.
 */
template <typename T, typename PromiseType>
struct TaskYieldAwaitable
{
    template <typename TaskType, typename... TaskArgs>
    using handle_type = std::coroutine_handle<TaskYieldPromise<TaskType, TaskArgs...>>;

    Scheduler&                      m_scheduler;
    SharedCoroutinePtr<PromiseType> m_coro;

    TaskYieldAwaitable(Scheduler& scheduler, SharedCoroutinePtr<PromiseType> coro);

    bool await_ready();

    template <typename TaskType, typename... TaskArgs>
    void await_suspend(handle_type<TaskType, TaskArgs...> awaiter_handle) noexcept;

    template <typename U = T>
    requires (!std::is_void_v<U>)
    U await_resume();

    template <typename U = T>
    requires (std::is_void_v<U>)
    U await_resume();
};

/* The compiler doesn't allow these two declarations to appear
 * together in one promise_type class, so we must conditonally 
 * inherit one of them.
 */
template <typename T, typename Derived>
struct TaskYieldValuePromiseBase
{
    template <std::convertible_to<T> From>
    void return_value(From&& value);
};

template <typename Derived>
struct TaskYieldVoidBase
{
    void return_void();
};

template <typename T, typename... Args>
struct TaskYieldPromise : public std::conditional_t<
    std::is_void_v<T>,
    TaskYieldVoidBase<TaskYieldPromise<T, Args...>>,
    TaskYieldValuePromiseBase<T, TaskYieldPromise<T, Args...>>
>
{
    using promise_type = TaskYieldPromise<T, Args...>;
    using coroutine_type = Coroutine<promise_type>;
    using task_type = std::enable_shared_from_this<void>;

    template <typename TaskType, typename... TaskArgs>
    using awaitable_type = 
        TaskYieldAwaitable<TaskType, TaskYieldPromise<TaskType, TaskArgs...>>;

    SynchronizedYieldValue<T>          m_value;
    Scheduler&                         m_scheduler;
    uint32_t                           m_priority;
    SharedCoroutinePtr<promise_type>   m_coro;
    SynchronizedSingleYieldValue<bool> m_has_awaiter;

    /* A task awaiting an exception can be resumed on a different
     * thread than the one on which the exception was thrown and 
     * stashed. As such, we need to synchronzie the access.
     */
    SynchronizedYieldValue<std::exception_ptr> m_exception;

    /* Keep around a shared pointer to the Task instance which has 
     * the 'Run' coroutine method. This way we will prevent 
     * destruction of that instance until the 'Run' method runs to 
     * completion and the promise destructor is invoked, allowing 
     * us to safely use the 'this' pointer in the method without 
     * worrying about the instance lifetime.
     */
    std::shared_ptr<task_type> m_task;

    /* A coroutine awaiter cannot safely query handle.done()
     * since the awaited coroutine can be suspended on a different
     * thread than the one the awaiter is resumed on, resulting
     * in a race condition. To address this problem, publish the 
     * state of the coroutine from the running thread each time 
     * the coroutine is suspended, and consume it from the 
     * awaiter's thread each time it is resumed. Cache the result 
     * for subsequent queries.
     */
    CoroutineState                         m_state;
    SynchronizedYieldValue<CoroutineState> m_next_state;

    TaskHandle<T, Args...>           get_return_object();
    void                             unhandled_exception();
    std::suspend_never               initial_suspend();
    std::suspend_always              final_suspend() noexcept;

    template <typename TaskType, typename... TaskArgs>
    awaitable_type<TaskType, TaskArgs...>
    await_transform(TaskHandle<TaskType, TaskArgs...>&& value);

    template <typename TaskType, typename... TaskArgs>
    awaitable_type<TaskType, TaskArgs...>
    await_transform(TaskHandle<TaskType, TaskArgs...>& value);

    template <typename U = T, std::convertible_to<U> From>
    requires (!std::is_void_v<U>)
    std::suspend_always yield_value(From&& value);

    template <typename U = T>
    requires (std::is_void_v<U>)
    std::suspend_always yield_value(const VoidType&);

    TaskYieldPromise(auto& task, Args&... args);
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
 * point when the task suspends. Awaiting on the TaskHandle
 * instance itself will suspend the caller until the next 
 * yielding of the task.
 */
template <typename T, typename... Args>
class TaskHandle final
{
public:

    using promise_type = TaskYieldPromise<T, Args...>;
    using coroutine_ptr_type = SharedCoroutinePtr<promise_type>;

    TaskHandle(const TaskHandle&) = delete;
    TaskHandle& operator=(const TaskHandle&) = delete;

    TaskHandle(TaskHandle&& other) noexcept = default;
    TaskHandle& operator=(TaskHandle&& other) noexcept = default;

    TaskHandle() = default;
    explicit TaskHandle(coroutine_ptr_type coroutine);

    bool Done() const;
    void Join();

private:

    template <typename TaskType, typename... TaskArgs>
    friend struct TaskYieldPromise;

    template <typename TaskType, typename... TaskArgs>
    friend struct TaskReturnPromise;

    /* Use a shared pointer for referring to the native 
     * coroutine handle to allow the scheduler to retain 
     * it for detached tasks.
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

    friend struct TaskYieldPromise<T, Args...>;
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
    friend struct TaskYieldAwaitable;

    template <typename T, typename... Args>
    friend struct TaskYieldPromise;
    
public:
    Scheduler();
    void Run();
    void Shutdown();
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

template <typename T, typename PromiseType>
TaskYieldAwaitable<T, PromiseType>::TaskYieldAwaitable(Scheduler& scheduler, 
    SharedCoroutinePtr<PromiseType> coro)
    : m_scheduler{scheduler}
    , m_coro{coro}
{}

template <typename T, typename PromiseType>
bool TaskYieldAwaitable<T, PromiseType>::await_ready()
{
    auto& promise = m_coro->Promise();
    promise.m_has_awaiter.Set(true);
    if(promise.m_state == CoroutineState::eDone)
        throw std::runtime_error{"Cannot wait on a finished task."};
    return false;
}

template <typename T, typename PromiseType>
template <typename TaskType, typename... TaskArgs>
void TaskYieldAwaitable<T, PromiseType>::await_suspend(
    handle_type<TaskType, TaskArgs...> awaiter_handle) noexcept
{
    const auto& promise = awaiter_handle.promise();
    m_scheduler.enqueue_task({promise.m_priority, promise.m_coro});
}

template <typename T, typename PromiseType>
template <typename U>
requires (!std::is_void_v<U>)
U TaskYieldAwaitable<T, PromiseType>::await_resume()
{
    auto& promise = m_coro->Promise();
    promise.m_state = promise.m_next_state.Consume();

    std::exception_ptr exc;
    if((exc = promise.m_exception.Consume())) {
        std::rethrow_exception(exc);
        return {};
    }

    T ret = promise.m_value.Consume();
    if(promise.m_coro) {
        m_scheduler.enqueue_task({promise.m_priority, promise.m_coro});
    }
    return ret;
}

template <typename T, typename PromiseType>
template <typename U>
requires (std::is_void_v<U>)
U TaskYieldAwaitable<T, PromiseType>::await_resume()
{
    auto& promise = m_coro->Promise();
    promise.m_state = promise.m_next_state.Consume();

    std::exception_ptr exc;
    if((exc = promise.m_exception.Consume())) {
        std::rethrow_exception(exc);
        return;
    }

    promise.m_value.Consume();
    if(promise.m_coro) {
        m_scheduler.enqueue_task({promise.m_priority, promise.m_coro});
    }
}

template <typename T, typename... Args>
TaskHandle<T, Args...> TaskYieldPromise<T, Args...>::get_return_object()
{
    return TaskHandle<T, Args...>{m_coro};
}

template <typename T, typename... Args>
void TaskYieldPromise<T, Args...>::unhandled_exception()
{
    m_exception.Yield(std::current_exception());
    m_next_state.Yield(CoroutineState::eRunning);
}

template <typename T, typename... Args>
std::suspend_never TaskYieldPromise<T, Args...>::initial_suspend()
{
    return {};
}

template <typename T, typename... Args>
std::suspend_always TaskYieldPromise<T, Args...>::final_suspend() noexcept
{
    std::exception_ptr exc;
    if(!m_has_awaiter.Get() && (exc = m_exception.Consume())) {
        std::rethrow_exception(exc);
    }
    return {};
}

template <typename T, typename... Args>
template <typename TaskType, typename... TaskArgs>
TaskYieldAwaitable<TaskType, TaskYieldPromise<TaskType, TaskArgs...>>
TaskYieldPromise<T, Args...>::await_transform(TaskHandle<TaskType, TaskArgs...>&& value)
{
    return {m_scheduler, value.m_coro};
}

template <typename T, typename... Args>
template <typename TaskType, typename... TaskArgs>
TaskYieldAwaitable<TaskType, TaskYieldPromise<TaskType, TaskArgs...>>
TaskYieldPromise<T, Args...>::await_transform(TaskHandle<TaskType, TaskArgs...>& value)
{
    return {m_scheduler, value.m_coro};
}

template <typename T, typename... Args>
template <typename U, std::convertible_to<U> From>
requires (!std::is_void_v<U>)
std::suspend_always TaskYieldPromise<T, Args...>::yield_value(From&& value)
{
    m_value.Yield(std::forward<From>(value));
    m_exception.Yield(nullptr);
    m_next_state.Yield(CoroutineState::eRunning);
    return {};
}

template <typename T, typename... Args>
template <typename U>
requires (std::is_void_v<U>)
std::suspend_always TaskYieldPromise<T, Args...>::yield_value(const VoidType&)
{
    m_value.Yield();
    m_exception.Yield(nullptr);
    m_next_state.Yield(CoroutineState::eRunning);
    return {};
}

template <typename T, typename Derived>
template <std::convertible_to<T> From>
void TaskYieldValuePromiseBase<T, Derived>::return_value(From&& value)
{
    static_cast<Derived*>(this)->m_coro = nullptr;
    static_cast<Derived*>(this)->m_value.Yield(std::forward<From>(value));
    static_cast<Derived*>(this)->m_exception.Yield(nullptr);
    static_cast<Derived*>(this)->m_next_state.Yield(CoroutineState::eDone);
}

template <typename Derived>
void TaskYieldVoidBase<Derived>::return_void()
{
    static_cast<Derived*>(this)->m_coro = nullptr;
    static_cast<Derived*>(this)->m_value.Yield();
    static_cast<Derived*>(this)->m_exception.Yield(nullptr);
    static_cast<Derived*>(this)->m_next_state.Yield(CoroutineState::eDone);
}

template <typename T, typename... Args>
TaskYieldPromise<T, Args...>::TaskYieldPromise(auto& task, Args&... args)
    : m_value{}
    , m_scheduler{task.Scheduler()}
    , m_coro{
        std::make_shared<Coroutine<promise_type>>(
            std::coroutine_handle<TaskYieldPromise<T, Args...>>::from_promise(*this),
            typeid(task).name())}
    , m_has_awaiter{}
    , m_exception{}
    , m_task{static_pointer_cast<task_type>(task.shared_from_this())}
    , m_state{CoroutineState::eRunning}
    , m_next_state{}
{}

template <typename T, typename... Args>
TaskHandle<T, Args...>::TaskHandle(coroutine_ptr_type coroutine)
    : m_coro{coroutine}
{}

template <typename T, typename... Args>
bool TaskHandle<T, Args...>::Done() const
{
    return (m_coro->Promise().m_state == CoroutineState::eDone);
}

template <typename T, typename... Args>
void TaskHandle<T, Args...>::Join()
{
}

template <typename T, typename Derived, typename... Args>
Task<T, Derived, Args...>::Task(TaskCreateToken token, 
    class Scheduler& scheduler, uint32_t priority)
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

        auto coro = static_pointer_cast<UntypedCoroutine>(
            m_ready_queue.top().m_handle
        );
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

