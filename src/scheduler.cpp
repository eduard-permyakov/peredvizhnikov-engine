export module scheduler;
export import <coroutine>;

import concurrency;
import logger;
import platform;

import <queue>;
import <cstdint>;
import <thread>;
import <mutex>;
import <condition_variable>;
import <memory>;
import <type_traits>;
import <optional>;

template <typename T, typename... Args>
struct std::coroutine_traits<std::shared_ptr<T>, Args...>
{
    using promise_type = typename T::promise_type;
};

namespace pe{

export using pe::VoidType;
export using pe::Void;

/*
 * Forward declarations
 */
export class Scheduler;
export template <typename ReturnType, typename Derived, typename... Args> class Task;
template <typename ReturnType, typename TaskType, typename... Args> struct TaskPromise;

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
        pe::dbgprint("Destroying:", m_name);
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
/* SCHEDULABLE                                                               */
/*****************************************************************************/

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

/*****************************************************************************/
/* TASK COND AWAITABLE                                                       */
/*****************************************************************************/
/*
 * Acts as either std::suspend_always or std::suspend_never depending 
 * on the 'suspend' argument.
 */
struct TaskCondAwaitable
{
private:
    bool m_suspend;
public:
    TaskCondAwaitable(bool suspend) : m_suspend{suspend} {}
    bool await_ready() const noexcept { return !m_suspend; }
    void await_suspend(std::coroutine_handle<>) const noexcept {}
    void await_resume() const noexcept {}
};

/*****************************************************************************/
/* TASK AWAITABLE                                                      		 */
/*****************************************************************************/
/*
 * A TaskAwaitable / TaskPromise pair implement the mechanics 
 * of awaiting on a yield from a task, be that from a co_yield 
 * or a co_return.
 */
template <typename ReturnType, typename PromiseType>
struct TaskAwaitable
{
    template <typename OtherReturnType, typename OtherTaskType, typename... OtherArgs>
    using handle_type = 
        std::coroutine_handle<TaskPromise<OtherReturnType, OtherTaskType, OtherArgs...>>;

    Scheduler&                      m_scheduler;
    SharedCoroutinePtr<PromiseType> m_coro;

    TaskAwaitable(Scheduler& scheduler, SharedCoroutinePtr<PromiseType> coro);

    bool await_ready();

    template <typename OtherReturnType, typename OtherTaskType, typename... OtherArgs>
    void await_suspend(handle_type<OtherReturnType, OtherTaskType, OtherArgs...> awaiter_handle) noexcept;

    template <typename U = ReturnType>
    requires (!std::is_void_v<U>)
    U await_resume();

    template <typename U = ReturnType>
    requires (std::is_void_v<U>)
    U await_resume();
};

/*****************************************************************************/
/* TASK TERMINATE AWAITABLE                                                  */
/*****************************************************************************/

template <typename ReturnType, typename PromiseType>
struct TaskTerminateAwaitable : public TaskAwaitable<ReturnType, PromiseType>
{
    using TaskAwaitable<ReturnType, PromiseType>::TaskAwaitable;

    template <typename U = ReturnType>
    requires (!std::is_void_v<U>)
    U await_resume();

    template <typename U = ReturnType>
    requires (std::is_void_v<U>)
    U await_resume();
};

/*****************************************************************************/
/* TASK PROMISE                                                              */
/*****************************************************************************/
/*
 * The compiler doesn't allow these two declarations to appear
 * together in one promise_type class, so we must conditonally 
 * inherit one of them.
 */
template <typename ReturnType, typename Derived>
struct TaskValuePromiseBase
{
    template <std::convertible_to<ReturnType> From>
    void return_value(From&& value)
    {
        static_cast<Derived*>(this)->m_value.Yield(std::forward<From>(value));
        static_cast<Derived*>(this)->m_exception.Yield(nullptr);
        static_cast<Derived*>(this)->m_next_state.Yield(CoroutineState::eDone);
    }
};

template <typename Derived>
struct TaskVoidPromiseBase
{
    void return_void()
    {
        static_cast<Derived*>(this)->m_exception.Yield(nullptr);
        static_cast<Derived*>(this)->m_next_state.Yield(CoroutineState::eDone);
    }
};

template <typename ReturnType, typename TaskType, typename... Args>
struct TaskPromise : public std::conditional_t<
    std::is_void_v<ReturnType>,
    TaskVoidPromiseBase<TaskPromise<ReturnType, TaskType, Args...>>,
    TaskValuePromiseBase<ReturnType, TaskPromise<ReturnType, TaskType, Args...>>
>
{
    using promise_type = TaskPromise<ReturnType, TaskType, Args...>;
    using coroutine_type = Coroutine<promise_type>;

    struct YieldAwaitable
    {
        Scheduler&  m_scheduler;
        Schedulable m_schedulable;

        bool await_ready() const noexcept { return !m_schedulable.m_handle; }
        void await_suspend(std::coroutine_handle<>) const noexcept;
        void await_resume() const noexcept {}
    };

    SynchronizedYieldValue<ReturnType> m_value;
    bool                               m_running_sync;
    bool                               m_joined;
    std::optional<Schedulable>         m_awaiter;

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
    std::shared_ptr<TaskType> m_task;

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

    std::shared_ptr<TaskType> get_return_object()
    {
        return {m_task};
    }

    void unhandled_exception()
    {
        m_exception.Yield(std::current_exception());
        m_next_state.Yield(CoroutineState::eRunning);
    }

    TaskCondAwaitable initial_suspend()
    {
        return {!m_running_sync};
    }

    YieldAwaitable final_suspend() noexcept
    {
        std::exception_ptr exc;
        if(!m_awaiter.has_value() && (exc = m_exception.Consume())) {
            std::rethrow_exception(exc);
        }
        struct Schedulable def{};
        YieldAwaitable ret{m_task->Scheduler(), m_awaiter.value_or(def)};
        m_task = nullptr;
        return ret;
    }

    template <typename OtherTaskType>
    typename OtherTaskType::awaitable_type
    await_transform(std::shared_ptr<OtherTaskType>&& value)
    {
        return {value->m_scheduler, value->m_coro};
    }

    template <typename OtherTaskType>
    typename OtherTaskType::awaitable_type
    await_transform(std::shared_ptr<OtherTaskType>& value)
    {
        return {value->m_scheduler, value->m_coro};
    }

    template <typename Awaitable>
    Awaitable
    await_transform(Awaitable&& value)
    {
        return value;
    }

    template <typename U = ReturnType, std::convertible_to<U> From>
    requires (!std::is_void_v<U>)
    YieldAwaitable yield_value(From&& value)
    {
        if(m_joined)
            return {m_task->Scheduler(), Schedulable()};
        m_value.Yield(std::forward<From>(value));
        m_exception.Yield(nullptr);
        m_next_state.Yield(CoroutineState::eRunning);
        return {m_task->Scheduler(), m_awaiter.value()};
    }

    template <typename U = ReturnType>
    requires (std::is_void_v<U>)
    YieldAwaitable yield_value(const VoidType&)
    {
        if(m_joined)
            return {m_task->Scheduler(), Schedulable()};
        m_exception.Yield(nullptr);
        m_next_state.Yield(CoroutineState::eRunning);
        return {m_task->Scheduler(), m_awaiter.value()};
    }

    TaskPromise(TaskType& task, Args&... args)
        : m_value{}
        , m_running_sync{!task.InitiallySuspended()}
        , m_joined{false}
        , m_awaiter{}
        , m_exception{}
        , m_task{static_pointer_cast<TaskType>(task.shared_from_this())}
        , m_state{CoroutineState::eRunning}
        , m_next_state{}
    {
        task.m_coro = std::make_shared<Coroutine<promise_type>>(
            std::coroutine_handle<TaskPromise<ReturnType, TaskType, Args...>>::from_promise(*this),
            typeid(task).name()
        );
    }

    Schedulable Schedulable() const
    {
        return {m_task->m_priority, m_task->m_coro, m_task->m_affinity};
    }
};

/*****************************************************************************/
/* TASK                                                                      */
/*****************************************************************************/

export using tid_t = uint32_t;
[[maybe_unused]] static std::atomic_uint32_t s_next_tid{0};

/*
 * An abstract base class for implementing user tasks. Can yield
 * different kinds of awaitables from its' methods.
 */
template <typename ReturnType, typename Derived, typename... Args>
class Task : public std::enable_shared_from_this<void>
{
private:

    using coroutine_ptr_type = SharedCoroutinePtr<TaskPromise<ReturnType, Derived, Args...>>;

    Scheduler&         m_scheduler;
    uint32_t           m_priority;
    bool               m_initially_suspended;
    Affinity           m_affinity;
    tid_t              m_tid;
    coroutine_ptr_type m_coro;

    template <typename OtherReturnType, typename OtherTaskType, typename... OtherArgs>
    friend struct TaskPromise;

    /* Uninstantiatable type to prevent constructing 
     * the type from outside the 'Create' method. 
     */
    class TaskCreateToken 
    {
        TaskCreateToken() = default;
        friend class Task<ReturnType, Derived, Args...>;
    };

public:

    using promise_type = TaskPromise<ReturnType, Derived, Args...>;
    using handle_type = std::shared_ptr<Derived>;
    using awaitable_type = TaskAwaitable<ReturnType, promise_type>;
    using terminate_awaitable_type = TaskTerminateAwaitable<ReturnType, promise_type>;

    Task(TaskCreateToken token, Scheduler& scheduler, uint32_t priority = 0, 
        bool initially_suspended = false, Affinity affinity = Affinity::eAny);
    virtual ~Task() = default;

    template <typename... ConstructorArgs>
    [[nodiscard]] static std::shared_ptr<Derived> Create(
        Scheduler& scheduler, uint32_t priority, ConstructorArgs... args)
    {
        return std::make_shared<Derived>(TaskCreateToken{}, scheduler, priority, args...);
    }

    virtual handle_type Run(Args...) = 0;

    Scheduler& Scheduler() const;
    uint32_t Priority() const;
    bool InitiallySuspended() const;
    Affinity Affinity() const;
    bool Done() const;
    tid_t TID() const;
    std::string Name() const;

    terminate_awaitable_type Terminate();
    awaitable_type Join();

    template <typename Task, typename Message, typename Response>
    awaitable_type Send(Task& task, Message m, Response& r);

    template <typename Task, typename Message>
    awaitable_type Receive(Task& task, Message& m);

    template <typename Task, typename Response>
    awaitable_type Reply(Task& task, Response& r);

    template <typename Event>
    awaitable_type Await(Event& e);

    template <typename Event>
    void Broadcast(Event& e);
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

    template <typename ReturnType, typename PromiseType>
    friend struct TaskAwaitable;

    template <typename ReturnType, typename TaskType, typename... Args>
    friend struct TaskPromise;

    template <typename ReturnType, typename Derived>
    friend struct TaskValuePromiseBase;

    template <typename Derived>
    friend struct TaskVoidPromiseBase;
    
public:
    Scheduler();
    void Run();
    void Shutdown();
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

template <typename ReturnType, typename PromiseType>
TaskAwaitable<ReturnType, PromiseType>::TaskAwaitable(Scheduler& scheduler, 
    SharedCoroutinePtr<PromiseType> coro)
    : m_scheduler{scheduler}
    , m_coro{coro}
{}

template <typename ReturnType, typename PromiseType>
bool TaskAwaitable<ReturnType, PromiseType>::await_ready()
{
    auto& promise = m_coro->Promise();

    if(promise.m_state == CoroutineState::eDone)
        throw std::runtime_error{"Cannot wait on a finished task."};

    return false;
}

template <typename ReturnType, typename PromiseType>
template <typename OtherReturnType, typename OtherTaskType, typename... OtherArgs>
void TaskAwaitable<ReturnType, PromiseType>::await_suspend(
    handle_type<OtherReturnType, OtherTaskType, OtherArgs...> awaiter_handle) noexcept
{
    auto& promise = m_coro->Promise();
    promise.m_awaiter = awaiter_handle.promise().Schedulable();

    if(promise.m_running_sync) {
        promise.m_running_sync = false;
        return;
    }
    m_scheduler.enqueue_task(promise.Schedulable());
}

template <typename ReturnType, typename PromiseType>
template <typename U>
requires (!std::is_void_v<U>)
U TaskAwaitable<ReturnType, PromiseType>::await_resume()
{
    auto& promise = m_coro->Promise();
    promise.m_state = promise.m_next_state.Consume();

    std::exception_ptr exc;
    if((exc = promise.m_exception.Consume())) {
        std::rethrow_exception(exc);
        return {};
    }

    return promise.m_value.Consume();
}

template <typename ReturnType, typename PromiseType>
template <typename U>
requires (std::is_void_v<U>)
U TaskAwaitable<ReturnType, PromiseType>::await_resume()
{
    auto& promise = m_coro->Promise();
    promise.m_state = promise.m_next_state.Consume();

    std::exception_ptr exc;
    if((exc = promise.m_exception.Consume())) {
        std::rethrow_exception(exc);
    }
}

template <typename ReturnType, typename PromiseType>
template <typename U>
requires (!std::is_void_v<U>)
U TaskTerminateAwaitable<ReturnType, PromiseType>::await_resume()
{
    auto& promise = this->m_coro->Promise();
    promise.m_state = CoroutineState::eDone;
    promise.m_task = nullptr;

    std::exception_ptr exc;
    if((exc = promise.m_exception.Consume())) {
        std::rethrow_exception(exc);
        return {};
    }

    return promise.m_value.Consume();
}

template <typename ReturnType, typename PromiseType>
template <typename U>
requires (std::is_void_v<U>)
U TaskTerminateAwaitable<ReturnType, PromiseType>::await_resume()
{
    auto& promise = this->m_coro->Promise();
    promise.m_state = CoroutineState::eDone;
    promise.m_task = nullptr;

    std::exception_ptr exc;
    if((exc = promise.m_exception.Consume())) {
        std::rethrow_exception(exc);
        return;
    }
}

template <typename ReturnType, typename TaskType, typename... Args>
void TaskPromise<ReturnType, TaskType, Args...>::YieldAwaitable::await_suspend(
    std::coroutine_handle<>) const noexcept
{
    m_scheduler.enqueue_task(m_schedulable);
}

template <typename ReturnType, typename Derived, typename... Args>
Task<ReturnType, Derived, Args...>::Task(TaskCreateToken token, class Scheduler& scheduler, 
    uint32_t priority, bool initially_suspended, enum Affinity affinity)
    : m_scheduler{scheduler}
    , m_priority{priority}
    , m_initially_suspended{initially_suspended}
    , m_affinity{affinity}
    , m_tid{s_next_tid++}
    , m_coro{nullptr}
{
    if(!m_initially_suspended && (m_affinity == Affinity::eMainThread))
        throw std::runtime_error{"Tasks with main thread affinity must be initially suspended."};
}

template <typename ReturnType, typename Derived, typename... Args>
Scheduler& Task<ReturnType, Derived, Args...>::Scheduler() const
{
    return m_scheduler;
}

template <typename ReturnType, typename Derived, typename... Args>
uint32_t Task<ReturnType, Derived, Args...>::Priority() const
{
    return m_priority;
}

template <typename ReturnType, typename Derived, typename... Args>
bool Task<ReturnType, Derived, Args...>::InitiallySuspended() const
{
    return m_initially_suspended;
}

template <typename ReturnType, typename Derived, typename... Args>
Affinity Task<ReturnType, Derived, Args...>::Affinity() const
{
    return m_affinity;
}

template <typename ReturnType, typename Derived, typename... Args>
bool Task<ReturnType, Derived, Args...>::Done() const
{
    if(!m_coro)
        return false;
    return (m_coro->Promise().m_state == CoroutineState::eDone);
}

template <typename ReturnType, typename Derived, typename... Args>
tid_t Task<ReturnType, Derived, Args...>::TID() const
{
    return m_tid;
}

template <typename ReturnType, typename Derived, typename... Args>
std::string Task<ReturnType, Derived, Args...>::Name() const
{
    return typeid(*this).name();
}

template <typename ReturnType, typename Derived, typename... Args>
typename Task<ReturnType, Derived, Args...>::terminate_awaitable_type
Task<ReturnType, Derived, Args...>::Terminate()
{
    return {m_scheduler, m_coro};
}

template <typename ReturnType, typename Derived, typename... Args>
typename Task<ReturnType, Derived, Args...>::awaitable_type
Task<ReturnType, Derived, Args...>::Join()
{
    m_coro->Promise().m_joined = true;
    return {m_scheduler, m_coro};
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

