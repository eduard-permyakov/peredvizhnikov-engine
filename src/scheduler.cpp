export module sync:scheduler;

export import <coroutine>;
export import shared_ptr;

import concurrency;
import logger;
import platform;
import lockfree_queue;
import event;
import meta;

import <queue>;
import <cstdint>;
import <thread>;
import <mutex>;
import <condition_variable>;
import <memory>;
import <type_traits>;
import <optional>;

template <typename T, typename... Args>
struct std::coroutine_traits<pe::shared_ptr<T>, Args...>
{
    using promise_type = typename T::promise_type;
};

namespace pe{

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
    eSuspended,
    eEventBlocked,
    eYieldBlocked,
    eRunning,
    eZombie,
    eJoined,
};

using UntypedCoroutine = Coroutine<void>;

template <typename PromiseType>
using SharedCoroutinePtr = pe::shared_ptr<Coroutine<PromiseType>>;

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
    uint32_t             m_priority;
    pe::shared_ptr<void> m_handle;
    Affinity             m_affinity;

    bool operator()(Schedulable lhs, Schedulable rhs)
    {
        return lhs.m_priority > rhs.m_priority;
    }
};

/*****************************************************************************/
/* VOID TYPE                                                                 */
/*****************************************************************************/
/*
 * Empty type to be used as a void yield value
 */
export struct VoidType {};
export constexpr VoidType Void = VoidType{};

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

    Scheduler&  m_scheduler;
    Schedulable m_schedulable;
    bool        m_suspend;

public:

    TaskCondAwaitable(Scheduler& scheduler, Schedulable schedulable, bool suspend)
        : m_scheduler{scheduler}
        , m_schedulable{schedulable}
        , m_suspend{suspend}
    {}

    bool await_ready() const noexcept { return !m_suspend; }
    void await_suspend(std::coroutine_handle<>) const noexcept;
    void await_resume() const noexcept {}
};

/*****************************************************************************/
/* TASK AWAITABLE                                                            */
/*****************************************************************************/
/*
 * A TaskAwaitable / TaskPromise pair implement the mechanics 
 * of awaiting on a yield from a task, be that from a co_yield 
 * or a co_return.
 */
template <typename ReturnType, typename PromiseType>
struct TaskAwaitable
{
protected:

    template <typename OtherReturnType, typename OtherTaskType, typename... OtherArgs>
    using handle_type = 
        std::coroutine_handle<TaskPromise<OtherReturnType, OtherTaskType, OtherArgs...>>;

    Scheduler&                      m_scheduler;
    SharedCoroutinePtr<PromiseType> m_coro;

public:

    TaskAwaitable(Scheduler& scheduler, SharedCoroutinePtr<PromiseType> coro);

    bool await_ready();

    template <typename OtherReturnType, typename OtherTaskType, typename... OtherArgs>
    bool await_suspend(handle_type<OtherReturnType, OtherTaskType, OtherArgs...> awaiter_handle) noexcept;

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
/* EVENT AWAITABLE                                                           */
/*****************************************************************************/

template <EventType Event>
requires requires {
    requires (std::is_copy_assignable_v<event_arg_t<Event>>);
    requires (std::is_default_constructible_v<event_arg_t<Event>>);
}
struct EventAwaitable
{
    using scheduler_ref_type = std::optional<std::reference_wrapper<Scheduler>>;

    Scheduler&         m_scheduler;
    Schedulable        m_awaiter;
    event_arg_t<Event> m_arg;

    EventAwaitable(Scheduler& scheduler, Schedulable awaiter)
        : m_scheduler{scheduler}
        , m_awaiter{awaiter}
        , m_arg{}
    {}

    bool await_ready()
    {
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> awaiter_handle) noexcept;

    event_arg_t<Event> await_resume()
    {
        return m_arg;
    }
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
        static_cast<Derived*>(this)->m_value = std::forward<From>(value);
    }
};

template <typename Derived>
struct TaskVoidPromiseBase
{
    void return_void() {}
};

template <typename ReturnType, typename TaskType, typename... Args>
struct TaskPromise : public std::conditional_t<
    std::is_void_v<ReturnType>,
    TaskVoidPromiseBase<TaskPromise<ReturnType, TaskType, Args...>>,
    TaskValuePromiseBase<ReturnType, TaskPromise<ReturnType, TaskType, Args...>>
>
{
private:

    friend struct TaskVoidPromiseBase<TaskPromise<ReturnType, TaskType, Args...>>;
    friend struct TaskValuePromiseBase<ReturnType, TaskPromise<ReturnType, TaskType, Args...>>;

    using promise_type = TaskPromise<ReturnType, TaskType, Args...>;
    using coroutine_type = Coroutine<promise_type>;
    using value_type = std::conditional_t<
        std::is_void_v<ReturnType>, 
        std::monostate, 
        ReturnType>;


    struct YieldAwaitable
    {
        Scheduler&  m_scheduler;
        Schedulable m_schedulable;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<>) const noexcept;
        void await_resume() const noexcept {}
    };

    std::atomic_flag            m_locked;
    value_type                  m_value;
    std::optional<Schedulable>  m_awaiter;
    std::exception_ptr          m_exception;
    std::atomic<CoroutineState> m_state;
    bool                        m_joining;
    /* Keep around a shared pointer to the Task instance which has 
     * the 'Run' coroutine method. This way we will prevent 
     * destruction of that instance until the 'Run' method runs to 
     * completion and the promise destructor is invoked, allowing 
     * us to safely use the 'this' pointer in the method without 
     * worrying about the instance lifetime.
     */
    pe::shared_ptr<TaskType>    m_task;

public:

    pe::shared_ptr<TaskType> get_return_object()
    {
        return {m_task};
    }

    void unhandled_exception()
    {
        m_exception = std::current_exception();
    }

    TaskCondAwaitable initial_suspend()
    {
        return {
            m_task->Scheduler(),
            Schedulable(),
            (m_state.load(std::memory_order_acquire) == CoroutineState::eSuspended)
        };
    }

    YieldAwaitable final_suspend() noexcept
    {
        AtomicScopedLock{m_locked};
        auto& scheduler = m_task->Scheduler();
        m_task.reset();

        if(!m_awaiter.has_value()) {
            m_state.store(CoroutineState::eZombie, std::memory_order_release);
            if(m_exception)
                std::rethrow_exception(m_exception);
            return {scheduler, {}};
        }

        /* We have an awaiter */
        m_state.store(CoroutineState::eJoined, std::memory_order_release);
        return {scheduler, m_awaiter.value()};
    }

    template <typename OtherTaskType>
    typename OtherTaskType::awaitable_type
    await_transform(pe::shared_ptr<OtherTaskType>&& value)
    {
        return {value->m_scheduler, value->m_coro};
    }

    template <typename OtherTaskType>
    typename OtherTaskType::awaitable_type
    await_transform(pe::shared_ptr<OtherTaskType>& value)
    {
        return {value->m_scheduler, value->m_coro};
    }

    template <typename Awaitable>
    Awaitable await_transform(Awaitable&& value)
    {
        return value;
    }

    template <typename U = ReturnType, std::convertible_to<U> From>
    requires (!std::is_void_v<U>)
    YieldAwaitable yield_value(From&& value)
    {
        AtomicScopedLock{m_locked};
        if(m_joining)
            return {m_task->Scheduler(), Schedulable()};
        m_value = std::forward<From>(value);
        if(!m_awaiter.has_value()) {
            m_state.store(CoroutineState::eYieldBlocked, std::memory_order_release);
            return {m_task->Scheduler(), {}};
        }
        m_state.store(CoroutineState::eSuspended, std::memory_order_release);
        return {m_task->Scheduler(), m_awaiter.value()};
    }

    template <typename U = ReturnType>
    requires (std::is_void_v<U>)
    YieldAwaitable yield_value(const VoidType&)
    {
        AtomicScopedLock{m_locked};
        if(m_joining)
            return {m_task->Scheduler(), Schedulable()};
        if(!m_awaiter.has_value()) {
            m_state.store(CoroutineState::eYieldBlocked, std::memory_order_release);
            return {m_task->Scheduler(), {}};
        }
        m_state.store(CoroutineState::eSuspended, std::memory_order_release);
        return {m_task->Scheduler(), m_awaiter.value()};
    }

    TaskPromise(TaskType& task, Args&... args)
        : m_locked{}
        , m_value{}
        , m_awaiter{}
        , m_exception{}
        , m_state{task.InitiallySuspended() ? CoroutineState::eSuspended 
                                            : CoroutineState::eRunning}
        , m_joining{false}
        , m_task{pe::static_pointer_cast<TaskType>(
            task.template shared_from_this<TaskType::is_traced_type>())}
    {
        task.m_coro = pe::make_shared<Coroutine<promise_type>>(
            std::coroutine_handle<promise_type>::from_promise(*this),
            typeid(task).name()
        );
    }

    Schedulable Schedulable() const
    {
        return {m_task->m_priority, m_task->m_coro, m_task->m_affinity};
    }

    bool TrySetAwaiterSafe(struct Schedulable sched)
    {
        AtomicScopedLock{m_locked};
        auto state = m_state.load(std::memory_order_acquire);
        if(state != CoroutineState::eRunning
        && state != CoroutineState::eEventBlocked)
            return false;
        m_awaiter = sched;
        return true;
    }

    void SetAwaiter(struct Schedulable sched)
    {
        m_awaiter = sched;
    }

    void SetJoinedSafe()
    {
        AtomicScopedLock{m_locked};
        m_joining = true;
    }

    CoroutineState GetState()
    {
        return m_state.load(std::memory_order_acquire);
    }

    void SetState(CoroutineState state)
    {
        m_state.store(state, std::memory_order_release);
    }

    void Terminate()
    {
        m_task.reset();
        m_state.store(CoroutineState::eJoined, std::memory_order_release);
    }

    ReturnType Value() const
    {
        return m_value;
    }

    const std::exception_ptr& Exception() const
    {
        return m_exception;
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
class Task : public pe::enable_shared_from_this<void>
{
private:

    using coroutine_ptr_type = SharedCoroutinePtr<TaskPromise<ReturnType, Derived, Args...>>;
    static constexpr bool is_traced_type = false;

    Scheduler&         m_scheduler;
    uint32_t           m_priority;
    bool               m_initially_suspended;
    Affinity           m_affinity;
    tid_t              m_tid;
    coroutine_ptr_type m_coro;

    template <typename OtherReturnType, typename OtherTaskType, typename... OtherArgs>
    friend struct TaskPromise;

protected:

    /* Uninstantiatable type to prevent constructing 
     * the type from outside the 'Create' method. 
     */
    class TaskCreateToken 
    {
        TaskCreateToken() = default;
        friend class Task<ReturnType, Derived, Args...>;
    };

    virtual pe::shared_ptr<Derived> Run(Args...) = 0;

public:

    using promise_type = TaskPromise<ReturnType, Derived, Args...>;
    using handle_type = pe::shared_ptr<Derived>;
    using awaitable_type = TaskAwaitable<ReturnType, promise_type>;
    using terminate_awaitable_type = TaskTerminateAwaitable<ReturnType, promise_type>;

    template <EventType Event>
    using event_awaitable_type = EventAwaitable<Event>;

    Task(TaskCreateToken token, Scheduler& scheduler, uint32_t priority = 0, 
        bool initially_suspended = false, Affinity affinity = Affinity::eAny);
    virtual ~Task() = default;

    template <typename... ConstructorArgs>
    [[nodiscard]] static pe::shared_ptr<Derived> Create(
        Scheduler& scheduler, uint32_t priority, bool initially_suspended = false, 
        Affinity affinity = Affinity::eAny, ConstructorArgs&&... args)
    {
        constexpr int num_cargs = sizeof...(ConstructorArgs) - sizeof...(Args);
        constexpr int num_args = sizeof...(args) - num_cargs;

        static_assert(num_cargs >= 0);
        static_assert(num_args >= 0);

        /* The last sizeof...(Args) arugments are forwarded to the Run method,
         * the ones before that are forwarded to the task constructor. Extract
         * the required arguments and forward them to the appropriate functions.
         */
        auto all_args = std::forward_as_tuple(args...);
        auto constructor_args = extract_tuple(make_seq<num_cargs, 0>(), all_args);
        auto run_args = extract_tuple(make_seq<num_args, num_cargs>(), all_args);

        static_assert(std::tuple_size_v<decltype(constructor_args)> == num_cargs);
        static_assert(std::tuple_size_v<decltype(run_args)> == num_args);

        auto callmakeshared = [&](auto&&... args){
            return pe::make_shared<Derived, Derived::is_traced_type>(
                TaskCreateToken{}, scheduler, priority, initially_suspended, affinity,
                std::forward<decltype(args)>(args)...
            );
        };
        auto&& ret = std::apply(callmakeshared, constructor_args);

        auto callrun = [&ret](auto&&... args){
            const auto& base = pe::static_pointer_cast<Task<ReturnType, Derived, Args...>>(ret);
            return base->Run(std::forward<decltype(args)>(args)...);
        };
        return std::apply(callrun, run_args);
    }

    Scheduler& Scheduler() const
    {
        return m_scheduler;
    }

    uint32_t Priority() const
    {
        return m_priority;
    }

    bool InitiallySuspended() const
    {
        return m_initially_suspended;
    }

    Affinity Affinity() const
    {
        return m_affinity;
    }

    tid_t TID() const
    {
        return m_tid;
    }

    bool Done() const;
    std::string Name() const;

    terminate_awaitable_type Terminate();
    awaitable_type Join();

    template <typename Task, typename Message, typename Response>
    awaitable_type Send(Task& task, Message m, Response& r);

    template <typename Task, typename Message>
    awaitable_type Receive(Task& task, Message& m);

    template <typename Task, typename Response>
    awaitable_type Reply(Task& task, Response& r);

    template <EventType Event, int Tag>
    void Subscribe();

    template <EventType Event, int Tag>
    void Unsubscribe();

    template <EventType Event>
    requires (Event < EventType::eNumEvents)
    event_awaitable_type<Event> Event();

    template <EventType Event>
    requires (Event < EventType::eNumEvents)
    void Broadcast(event_arg_t<Event> arg = {});
};

/*****************************************************************************/
/* EVENT SUBSCRIBER                                                          */
/*****************************************************************************/

struct EventSubscriber
{
    tid_t              m_tid;
    pe::weak_ptr<void> m_task;

    std::strong_ordering operator<=>(const EventSubscriber& rhs) const noexcept
    {
        return (m_tid <=> rhs.m_tid);
    }

    bool operator==(const EventSubscriber& rhs) const noexcept
    {
        return (m_tid == rhs.m_tid);
    }
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
    using awaitable_variant_type = EventAwaitableRefVariant<EventAwaitable>;
    using event_queue_type = LockfreeQueue<awaitable_variant_type>;

    queue_type               m_ready_queue;
    queue_type               m_main_ready_queue;
    std::mutex               m_ready_lock;
    std::condition_variable  m_ready_cond;
    std::size_t              m_nworkers;
    std::vector<std::thread> m_worker_pool;

    /* Pointer for safely publishing the completion of queue creation */
    std::atomic<event_queue_type*>           m_event_queues_base;
    std::array<event_queue_type, kNumEvents> m_event_queues;

    template <EventType Event>
    void await_event(EventAwaitable<Event>& awaitable);

    template <EventType Event>
    void notify_event(event_arg_t<Event> arg);

    template <EventType Event>
    void add_subscriber(const EventSubscriber& sub);

    template <EventType Event>
    void remove_subscriber(const EventSubscriber& sub);

    template <EventType Event>
    bool has_subscriber(const EventSubscriber& sub);

    void enqueue_task(Schedulable schedulable);
    void work();
    void main_work();

    template <typename ReturnType, typename PromiseType>
    friend struct TaskAwaitable;

    template <EventType Event>
    requires requires {
        requires (std::is_copy_assignable_v<event_arg_t<Event>>);
        requires (std::is_default_constructible_v<event_arg_t<Event>>);
    }
    friend struct EventAwaitable;
    friend struct TaskCondAwaitable;

    template <typename ReturnType, typename TaskType, typename... Args>
    friend struct TaskPromise;

    template <typename ReturnType, typename TaskType, typename... Args>
    friend class Task;

    friend class Latch;
    friend class Barrier;
    
public:
    Scheduler();
    void Run();
    void Shutdown();
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

void TaskCondAwaitable::await_suspend(std::coroutine_handle<>) const noexcept
{
    if(!m_suspend)
        m_scheduler.enqueue_task(m_schedulable);
}

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
    CoroutineState state = promise.GetState();

    switch(state) {
    case CoroutineState::eEventBlocked:
        return false;
    case CoroutineState::eYieldBlocked:
        promise.SetState(CoroutineState::eSuspended);
        return true;
    case CoroutineState::eSuspended:
        return false;
    case CoroutineState::eRunning:
        return false;
    case CoroutineState::eZombie:
        promise.SetState(CoroutineState::eJoined);
        return true;
    case CoroutineState::eJoined:
        throw std::runtime_error{"Cannot await a joined task."};
    }
}

template <typename ReturnType, typename PromiseType>
template <typename OtherReturnType, typename OtherTaskType, typename... OtherArgs>
bool TaskAwaitable<ReturnType, PromiseType>::await_suspend(
    handle_type<OtherReturnType, OtherTaskType, OtherArgs...> awaiter_handle) noexcept
{
    auto& promise = m_coro->Promise();
    CoroutineState state = promise.GetState();
    Schedulable awaiter = awaiter_handle.promise().Schedulable();

    if(state == CoroutineState::eSuspended) {
        promise.SetAwaiter(awaiter);
        promise.SetState(CoroutineState::eRunning);
        m_scheduler.enqueue_task(promise.Schedulable());
        return true;
    }
    return promise.TrySetAwaiterSafe(awaiter);
}

template <typename ReturnType, typename PromiseType>
template <typename U>
requires (!std::is_void_v<U>)
U TaskAwaitable<ReturnType, PromiseType>::await_resume()
{
    auto& promise = m_coro->Promise();
    if(promise.Exception()) {
        std::rethrow_exception(promise.Exception());
        return {};
    }
    return promise.Value();
}

template <typename ReturnType, typename PromiseType>
template <typename U>
requires (std::is_void_v<U>)
U TaskAwaitable<ReturnType, PromiseType>::await_resume()
{
    auto& promise = m_coro->Promise();
    if(promise.Exception()) {
        std::rethrow_exception(promise.Exception());
    }
}

template <typename ReturnType, typename PromiseType>
template <typename U>
requires (!std::is_void_v<U>)
U TaskTerminateAwaitable<ReturnType, PromiseType>::await_resume()
{
    auto& promise = this->m_coro->Promise();
    promise.Terminate();

    if(promise.Exception()) {
        std::rethrow_exception(promise.Exception());
        return {};
    }

    return promise.Value();
}

template <typename ReturnType, typename PromiseType>
template <typename U>
requires (std::is_void_v<U>)
U TaskTerminateAwaitable<ReturnType, PromiseType>::await_resume()
{
    auto& promise = this->m_coro->Promise();
    promise.Terminate();

    if(promise.Exception()) {
        std::rethrow_exception(promise.Exception());
    }
}

template <typename ReturnType, typename TaskType, typename... Args>
void TaskPromise<ReturnType, TaskType, Args...>::YieldAwaitable::await_suspend(
    std::coroutine_handle<>) const noexcept
{
    if(m_schedulable.m_handle) {
        m_scheduler.enqueue_task(m_schedulable);
    }
}

template <EventType Event>
requires requires {
    requires (std::is_copy_assignable_v<event_arg_t<Event>>);
    requires (std::is_default_constructible_v<event_arg_t<Event>>);
}
template <typename PromiseType>
bool EventAwaitable<Event>::await_suspend(std::coroutine_handle<PromiseType> awaiter_handle) noexcept
{
    /* The awaiter is guaranteed to be suspended at this point */
    awaiter_handle.promise().SetState(CoroutineState::eEventBlocked);
    m_scheduler.await_event(*this);
    return true;
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
}

template <typename ReturnType, typename Derived, typename... Args>
bool Task<ReturnType, Derived, Args...>::Done() const
{
    if(!m_coro)
        return false;
    CoroutineState state = m_coro->Promise().GetState();
    return (state == CoroutineState::eJoined)
        || (state == CoroutineState::eZombie);
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
    auto& promise = m_coro->Promise();
    promise.SetJoinedSafe();
    return {m_scheduler, m_coro};
}

template <typename ReturnType, typename Derived, typename... Args>
template <EventType Event>
requires (Event < EventType::eNumEvents)
typename Task<ReturnType, Derived, Args...>::template event_awaitable_type<Event>
Task<ReturnType, Derived, Args...>::Event()
{
    auto& promise = m_coro->Promise();
    return {m_scheduler, promise.Schedulable()};
}

template <typename ReturnType, typename Derived, typename... Args>
template <EventType Event>
requires (Event < EventType::eNumEvents)
void Task<ReturnType, Derived, Args...>::Broadcast(event_arg_t<Event> arg)
{
    m_scheduler.template notify_event<Event>(arg);
}

Scheduler::Scheduler()
    : m_ready_queue{}
    , m_ready_lock{}
    , m_ready_cond{}
    , m_nworkers{static_cast<std::size_t>(
        std::max(1, static_cast<int>(std::thread::hardware_concurrency())-1))}
    , m_worker_pool{}
    , m_event_queues{}
{
    if constexpr (pe::kLinux) {
        auto handle = pthread_self();
        pthread_setname_np(handle, "main");
    }
    m_event_queues_base.store(&m_event_queues[0], std::memory_order_release);
}

void Scheduler::work()
{
    while(true) {

        std::unique_lock<std::mutex> lock{m_ready_lock};
        m_ready_cond.wait(lock, [&](){ return !m_ready_queue.empty(); });

        auto coro = pe::static_pointer_cast<UntypedCoroutine>(
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
        pe::shared_ptr<UntypedCoroutine> coro = nullptr;
        if(!m_main_ready_queue.empty()) {
            coro = pe::static_pointer_cast<UntypedCoroutine>(m_main_ready_queue.top().m_handle);
            m_main_ready_queue.pop();
        }else{
            coro = pe::static_pointer_cast<UntypedCoroutine>(m_ready_queue.top().m_handle);
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

template <EventType Event>
void Scheduler::await_event(EventAwaitable<Event>& awaitable)
{
    constexpr std::size_t event = static_cast<std::size_t>(Event);
    auto event_queues_base = m_event_queues_base.load(std::memory_order_acquire);
    auto& queue = event_queues_base[event];
    queue.Enqueue(std::ref(awaitable));
}

template <EventType Event>
void Scheduler::notify_event(event_arg_t<Event> arg)
{
    constexpr std::size_t event = static_cast<std::size_t>(Event);
    auto event_queues_base = m_event_queues_base.load(std::memory_order_acquire);
    auto& queue = event_queues_base[event];

    using awaitable_type = EventAwaitable<Event>;
    using optional_ref_type = std::optional<std::reference_wrapper<awaitable_type>>;

    std::optional<awaitable_variant_type> curr;
    do{
        curr = queue.Dequeue();
        if(curr.has_value()) {
            auto& ref = std::get<optional_ref_type>(curr.value());
            auto& awaitable = ref.value().get();
            awaitable.m_arg = arg;
            enqueue_task(awaitable.m_awaiter);
        }
    }while(curr.has_value());
}

template <EventType Event>
void Scheduler::add_subscriber(const EventSubscriber& sub)
{
}

template <EventType Event>
void Scheduler::remove_subscriber(const EventSubscriber& sub)
{
}

template <EventType Event>
bool Scheduler::has_subscriber(const EventSubscriber& sub)
{
    return true;
}

void Scheduler::Run()
{
    m_worker_pool.reserve(m_nworkers);
    for(int i = 0; i < m_nworkers; i++) {
        m_worker_pool.emplace_back(&Scheduler::work, this);
        SetThreadName(m_worker_pool[i], "worker-" + std::to_string(i));
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

