export module scheduler;
export import <coroutine>;

export import concurrency;
import logger;
import platform;

import <queue>;
import <cstdint>;
import <thread>;
import <mutex>;
import <condition_variable>;
import <memory>;
import <type_traits>;

namespace pe{

/*
 * Forward declarations
 */
export template <typename T, typename... Args> class TaskHandle;
export class Scheduler;
export template <typename T, typename Derived, typename... Args> class Task;
template <typename T, typename... Args> struct TaskPromise;

/*****************************************************************************/
/* COROUTINE                                                                 */
/*****************************************************************************/
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

enum class CoroutineState
{
    eRunning,
    eDone,
};

using UntypedCoroutine = Coroutine<void>;

template <typename PromiseType>
using SharedCoroutinePtr = std::shared_ptr<Coroutine<PromiseType>>;

/*****************************************************************************/
/* AFFINITY                                                                  */
/*****************************************************************************/

export
enum class Affinity
{
    eAny,
    eMainThread,
};

/*****************************************************************************/
/* TASK AWAITABLES                                                           */
/*****************************************************************************/
/*
 * Awaitable for the initial suspend of the task. Acts as either
 * std::suspend_always or std::suspend_never depending on the 
 * 'suspend' argument.
 */
struct TaskInitialAwaitable
{
private:
    bool m_suspend;
public:
    TaskInitialAwaitable(bool suspend) : m_suspend{suspend} {}
    bool await_ready() const noexcept { return !m_suspend; }
    void await_suspend(std::coroutine_handle<>) const noexcept {}
    void await_resume() const noexcept {}
};

/*
 * A TaskAwaitable / TaskPromise pair implement 
 * the mechanics of awaiting on a yield from a task, be
 * that from a co_yield or a co_return.
 */
template <typename T, typename PromiseType>
struct TaskAwaitable
{
    template <typename TaskType, typename... TaskArgs>
    using handle_type = std::coroutine_handle<TaskPromise<TaskType, TaskArgs...>>;

    Scheduler&                      m_scheduler;
    SharedCoroutinePtr<PromiseType> m_coro;
    bool                            m_terminate;

    TaskAwaitable(Scheduler& scheduler, SharedCoroutinePtr<PromiseType> coro,
        bool terminate = false);

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

/*****************************************************************************/
/* TASK PROMISES                                                             */
/*****************************************************************************/
/*
 * The compiler doesn't allow these two declarations to appear
 * together in one promise_type class, so we must conditonally 
 * inherit one of them.
 */
template <typename T, typename Derived>
struct TaskValuePromiseBase
{
    template <std::convertible_to<T> From>
    void return_value(From&& value);
};

template <typename Derived>
struct TaskVoidPromiseBase
{
    void return_void();
};

template <typename T, typename... Args>
struct TaskPromise : public std::conditional_t<
    std::is_void_v<T>,
    TaskVoidPromiseBase<TaskPromise<T, Args...>>,
    TaskValuePromiseBase<T, TaskPromise<T, Args...>>
>
{
    using promise_type = TaskPromise<T, Args...>;
    using coroutine_type = Coroutine<promise_type>;
    using task_type = std::enable_shared_from_this<void>;

    template <typename TaskType, typename... TaskArgs>
    using awaitable_type = 
        TaskAwaitable<TaskType, TaskPromise<TaskType, TaskArgs...>>;

    SynchronizedYieldValue<T>          m_value;
    Scheduler&                         m_scheduler;
    uint32_t                           m_priority;
    Affinity                           m_affinity;
    bool                               m_started;
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
    TaskInitialAwaitable             initial_suspend();
    std::suspend_always              final_suspend() noexcept;

    template <typename TaskType, typename... TaskArgs>
    awaitable_type<TaskType, TaskArgs...>
    await_transform(TaskHandle<TaskType, TaskArgs...>&& value);

    template <typename TaskType, typename... TaskArgs>
    awaitable_type<TaskType, TaskArgs...>
    await_transform(TaskHandle<TaskType, TaskArgs...>& value);

    template <typename TaskType, typename... TaskArgs>
    awaitable_type<TaskType, TaskArgs...>
    await_transform(awaitable_type<TaskType, TaskArgs...>&& value);

    template <typename TaskType, typename... TaskArgs>
    awaitable_type<TaskType, TaskArgs...>
    await_transform(awaitable_type<TaskType, TaskArgs...>& value);

    template <typename U = T, std::convertible_to<U> From>
    requires (!std::is_void_v<U>)
    std::suspend_always yield_value(From&& value);

    template <typename U = T>
    requires (std::is_void_v<U>)
    std::suspend_always yield_value(const VoidType&);

    TaskPromise(auto& task, Args&... args);
};

/*****************************************************************************/
/* TASK HANDLE                                                               */
/*****************************************************************************/
/*
 * A generic handle to refer to task. Awaiting on the task 
 * handle will suspend until the task returns or yields
 * control back to the caller.
 */
template <typename T, typename... Args>
class TaskHandle final
{
public:

    using promise_type = TaskPromise<T, Args...>;
    using awaitable_type = TaskAwaitable<T, promise_type>;
    using coroutine_ptr_type = SharedCoroutinePtr<promise_type>;

    TaskHandle(const TaskHandle&) = delete;
    TaskHandle& operator=(const TaskHandle&) = delete;

    TaskHandle(TaskHandle&& other) noexcept = default;
    TaskHandle& operator=(TaskHandle&& other) noexcept = default;

    TaskHandle() = default;
    explicit TaskHandle(coroutine_ptr_type coroutine);

    bool Done() const;
    awaitable_type Terminate(Scheduler& scheduler);

private:

    template <typename TaskType, typename... TaskArgs>
    friend struct TaskPromise;

    /* Use a shared pointer for referring to the native 
     * coroutine handle to allow the scheduler to retain 
     * it for detached tasks.
     */
    coroutine_ptr_type m_coro{0};
};

/*****************************************************************************/
/* TASK                                                                      */
/*****************************************************************************/
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
    bool       m_initially_suspended;
    Affinity   m_affinity;

    friend struct TaskPromise<T, Args...>;

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

    Task(TaskCreateToken token, Scheduler& scheduler, uint32_t priority = 0, 
        bool initially_suspended = false, Affinity affinity = Affinity::eAny);
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
    bool InitiallySuspended() const;
    Affinity Affinity() const;
};

/*****************************************************************************/
/* SCHEDULER                                                                 */
/*****************************************************************************/
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
        Affinity              m_affinity;

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
    queue_type               m_main_ready_queue;
    std::mutex               m_ready_lock;
    std::condition_variable  m_ready_cond;
    std::size_t              m_nworkers;
    std::vector<std::thread> m_worker_pool;

    void enqueue_task(Schedulable schedulable);
    void work();
    void main_work();

    template <typename T, typename PromiseType>
    friend struct TaskAwaitable;

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

template <typename T, typename PromiseType>
TaskAwaitable<T, PromiseType>::TaskAwaitable(Scheduler& scheduler, 
    SharedCoroutinePtr<PromiseType> coro, bool terminate)
    : m_scheduler{scheduler}
    , m_coro{coro}
    , m_terminate{terminate}
{}

template <typename T, typename PromiseType>
bool TaskAwaitable<T, PromiseType>::await_ready()
{
    auto& promise = m_coro->Promise();
    promise.m_has_awaiter.Set(true);

    if(!promise.m_started) {
        m_scheduler.enqueue_task({promise.m_priority, promise.m_coro, promise.m_affinity});
        promise.m_started = true;
    }

    if(promise.m_state == CoroutineState::eDone)
        throw std::runtime_error{"Cannot wait on a finished task."};

    return false;
}

template <typename T, typename PromiseType>
template <typename TaskType, typename... TaskArgs>
void TaskAwaitable<T, PromiseType>::await_suspend(
    handle_type<TaskType, TaskArgs...> awaiter_handle) noexcept
{
    const auto& promise = awaiter_handle.promise();
    m_scheduler.enqueue_task({promise.m_priority, promise.m_coro, promise.m_affinity});
}

template <typename T, typename PromiseType>
template <typename U>
requires (!std::is_void_v<U>)
U TaskAwaitable<T, PromiseType>::await_resume()
{
    auto& promise = m_coro->Promise();
    promise.m_state = promise.m_next_state.Consume();

    std::exception_ptr exc;
    if((exc = promise.m_exception.Consume())) {
        std::rethrow_exception(exc);
        return {};
    }

    if(m_terminate) {
        promise.m_coro = nullptr;
        promise.m_state = CoroutineState::eDone;
    }

    T ret = promise.m_value.Consume();
    if(promise.m_coro) {
        m_scheduler.enqueue_task({promise.m_priority, promise.m_coro, promise.m_affinity});
    }
    return ret;
}

template <typename T, typename PromiseType>
template <typename U>
requires (std::is_void_v<U>)
U TaskAwaitable<T, PromiseType>::await_resume()
{
    auto& promise = m_coro->Promise();
    promise.m_state = promise.m_next_state.Consume();

    std::exception_ptr exc;
    if((exc = promise.m_exception.Consume())) {
        std::rethrow_exception(exc);
        return;
    }

    if(m_terminate) {
        promise.m_coro = nullptr;
        promise.m_state = CoroutineState::eDone;
    }

    promise.m_value.Consume();
    if(promise.m_coro) {
        m_scheduler.enqueue_task({promise.m_priority, promise.m_coro, promise.m_affinity});
    }
}

template <typename T, typename... Args>
TaskHandle<T, Args...> TaskPromise<T, Args...>::get_return_object()
{
    return TaskHandle<T, Args...>{m_coro};
}

template <typename T, typename... Args>
void TaskPromise<T, Args...>::unhandled_exception()
{
    m_exception.Yield(std::current_exception());
    m_next_state.Yield(CoroutineState::eRunning);
}

template <typename T, typename... Args>
TaskInitialAwaitable TaskPromise<T, Args...>::initial_suspend()
{
    return {!m_started};
}

template <typename T, typename... Args>
std::suspend_always TaskPromise<T, Args...>::final_suspend() noexcept
{
    std::exception_ptr exc;
    if(!m_has_awaiter.Get() && (exc = m_exception.Consume())) {
        std::rethrow_exception(exc);
    }
    return {};
}

template <typename T, typename... Args>
template <typename TaskType, typename... TaskArgs>
TaskAwaitable<TaskType, TaskPromise<TaskType, TaskArgs...>>
TaskPromise<T, Args...>::await_transform(TaskHandle<TaskType, TaskArgs...>&& value)
{
    return {m_scheduler, value.m_coro};
}

template <typename T, typename... Args>
template <typename TaskType, typename... TaskArgs>
TaskAwaitable<TaskType, TaskPromise<TaskType, TaskArgs...>>
TaskPromise<T, Args...>::await_transform(TaskHandle<TaskType, TaskArgs...>& value)
{
    return {m_scheduler, value.m_coro};
}

template <typename T, typename... Args>
template <typename TaskType, typename... TaskArgs>
TaskAwaitable<TaskType, TaskPromise<TaskType, TaskArgs...>>
TaskPromise<T, Args...>::await_transform(TaskAwaitable<TaskType, TaskPromise<TaskType, TaskArgs...>>&& value)
{
    return value;
}

template <typename T, typename... Args>
template <typename TaskType, typename... TaskArgs>
TaskAwaitable<TaskType, TaskPromise<TaskType, TaskArgs...>>
TaskPromise<T, Args...>::await_transform(TaskAwaitable<TaskType, TaskPromise<TaskType, TaskArgs...>>& value)
{
    return value;
}

template <typename T, typename... Args>
template <typename U, std::convertible_to<U> From>
requires (!std::is_void_v<U>)
std::suspend_always TaskPromise<T, Args...>::yield_value(From&& value)
{
    m_value.Yield(std::forward<From>(value));
    m_exception.Yield(nullptr);
    m_next_state.Yield(CoroutineState::eRunning);
    return {};
}

template <typename T, typename... Args>
template <typename U>
requires (std::is_void_v<U>)
std::suspend_always TaskPromise<T, Args...>::yield_value(const VoidType&)
{
    m_value.Yield();
    m_exception.Yield(nullptr);
    m_next_state.Yield(CoroutineState::eRunning);
    return {};
}

template <typename T, typename Derived>
template <std::convertible_to<T> From>
void TaskValuePromiseBase<T, Derived>::return_value(From&& value)
{
    static_cast<Derived*>(this)->m_coro = nullptr;
    static_cast<Derived*>(this)->m_value.Yield(std::forward<From>(value));
    static_cast<Derived*>(this)->m_exception.Yield(nullptr);
    static_cast<Derived*>(this)->m_next_state.Yield(CoroutineState::eDone);
}

template <typename Derived>
void TaskVoidPromiseBase<Derived>::return_void()
{
    static_cast<Derived*>(this)->m_coro = nullptr;
    static_cast<Derived*>(this)->m_value.Yield();
    static_cast<Derived*>(this)->m_exception.Yield(nullptr);
    static_cast<Derived*>(this)->m_next_state.Yield(CoroutineState::eDone);
}

template <typename T, typename... Args>
TaskPromise<T, Args...>::TaskPromise(auto& task, Args&... args)
    : m_value{}
    , m_scheduler{task.Scheduler()}
    , m_affinity{task.Affinity()}
    , m_started{!task.InitiallySuspended()}
    , m_coro{
        std::make_shared<Coroutine<promise_type>>(
            std::coroutine_handle<TaskPromise<T, Args...>>::from_promise(*this),
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
typename TaskHandle<T, Args...>::awaitable_type 
TaskHandle<T, Args...>::Terminate(Scheduler& scheduler)
{
    return {scheduler, m_coro, true};
}

template <typename T, typename Derived, typename... Args>
Task<T, Derived, Args...>::Task(TaskCreateToken token, class Scheduler& scheduler, 
    uint32_t priority, bool initially_suspended, enum Affinity affinity)
    : m_scheduler{scheduler}
    , m_priority{priority}
    , m_initially_suspended{initially_suspended}
    , m_affinity{affinity}
{
    if(!m_initially_suspended && (m_affinity == Affinity::eMainThread))
        throw std::runtime_error{"Tasks with main thread affinity must be initially suspended."};
}

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

template <typename T, typename Derived, typename... Args>
bool Task<T, Derived, Args...>::InitiallySuspended() const
{
    return m_initially_suspended;
}

template <typename T, typename Derived, typename... Args>
Affinity Task<T, Derived, Args...>::Affinity() const
{
    return m_affinity;
}

Scheduler::Scheduler()
    : m_ready_queue{}
    , m_ready_lock{}
    , m_ready_cond{}
    , m_nworkers{static_cast<std::size_t>(
        std::max(1, static_cast<int>(std::thread::hardware_concurrency())-1))}
    , m_worker_pool{}
{
    if constexpr (pe::kLinux) {
        auto handle = pthread_self();
        pthread_setname_np(handle, "main");
    }
}

void Scheduler::work()
{
    while(true) {

        std::unique_lock<std::mutex> lock{m_ready_lock};
        m_ready_cond.wait(lock, [&](){ return !m_ready_queue.empty(); });

        auto coro = static_pointer_cast<UntypedCoroutine>(
            m_ready_queue.top().m_handle
        );
        m_ready_queue.pop();
        lock.unlock();

        coro->Resume();
    }
}

void Scheduler::main_work()
{
    while(true) {

        std::unique_lock<std::mutex> lock{m_ready_lock};
        m_ready_cond.wait(lock, [&](){ 
            return (!m_main_ready_queue.empty() || !m_ready_queue.empty()); 
        });

        /* Prioritize tasks from the 'main' ready queue */
        std::shared_ptr<UntypedCoroutine> coro = nullptr;
        if(!m_main_ready_queue.empty()) {
            coro = static_pointer_cast<UntypedCoroutine>(m_main_ready_queue.top().m_handle);
            m_main_ready_queue.pop();
        }else{
            coro = static_pointer_cast<UntypedCoroutine>(m_ready_queue.top().m_handle);
            m_ready_queue.pop();
        }
        lock.unlock();

        coro->Resume();
    }
}

void Scheduler::enqueue_task(Schedulable schedulable)
{
    std::unique_lock<std::mutex> lock{m_ready_lock};

    switch(schedulable.m_affinity) {
    case Affinity::eAny:
        m_ready_queue.push(schedulable);
        m_ready_cond.notify_one();
        break;
    case Affinity::eMainThread:
        m_main_ready_queue.push(schedulable);
        m_ready_cond.notify_all();
        break;
    }
}

void Scheduler::Run()
{
    m_worker_pool.reserve(m_nworkers);
    for(int i = 0; i < m_nworkers; i++) {
        m_worker_pool.emplace_back(&Scheduler::work, this);
        if constexpr (pe::kLinux) {
            auto handle = m_worker_pool[i].native_handle();
            pthread_setname_np(handle, ("worker-" + std::to_string(i)).c_str());
        }
    }

    main_work();

    for(auto& thread : m_worker_pool) {
        thread.join();
    }
}

void Scheduler::Shutdown()
{

}

} // namespace pe

