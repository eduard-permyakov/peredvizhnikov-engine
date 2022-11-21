export module sync:scheduler;
export import <coroutine>;
export import shared_ptr;

import concurrency;
import logger;
import platform;
import lockfree_queue;
import lockfree_list;
import iterable_lockfree_list;
import event;
import meta;
import assert;
import lockfree_work;

import <array>;
import <queue>;
import <cstdint>;
import <thread>;
import <mutex>;
import <condition_variable>;
import <memory>;
import <type_traits>;
import <optional>;
import <bitset>;
import <unordered_set>;

template <typename T, typename... Args>
struct std::coroutine_traits<pe::shared_ptr<T>, Args...>
{
    using promise_type = typename T::promise_type;
};

namespace pe{

/*
 * Forward declarations
 */

export using tid_t = uint32_t;
export class Scheduler;

export 
template <typename ReturnType, typename Derived, typename... Args>
class Task;

template <typename ReturnType, typename TaskType>
struct TaskPromise;

template <std::integral T>
inline uint16_t u16(T val)
{
    return static_cast<uint16_t>(val);
}

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

    const std::string Name() const
    {
        return m_name;
    }
};

using UntypedCoroutine = Coroutine<void>;

template <typename PromiseType>
using SharedCoroutinePtr = pe::shared_ptr<Coroutine<PromiseType>>;

/*****************************************************************************/
/* TASK STATE                                                                */
/*****************************************************************************/

enum class TaskState : uint32_t
{
    eSuspended,
    eEventBlocked,
    eYieldBlocked,
    eRunning,
    eZombie,
    eJoined,
};

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

    template <typename OtherReturnType, typename OtherTaskType>
    using handle_type = 
        std::coroutine_handle<TaskPromise<OtherReturnType, OtherTaskType>>;

    Scheduler&                      m_scheduler;
    SharedCoroutinePtr<PromiseType> m_coro;

public:

    TaskAwaitable(Scheduler& scheduler, SharedCoroutinePtr<PromiseType> coro);

    bool await_ready();

    template <typename OtherReturnType, typename OtherTaskType>
    bool await_suspend(handle_type<OtherReturnType, OtherTaskType> awaiter_handle);

    template <typename U = ReturnType>
    requires (!std::is_void_v<U>)
    U await_resume();

    template <typename U = ReturnType>
    requires (std::is_void_v<U>)
    U await_resume();

    Schedulable Schedulable()
    {
        return m_coro->Promise().Schedulable();
    }
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
struct EventAwaitable
{
private:

    Scheduler&           m_scheduler;
    Schedulable          m_awaiter;
    pe::shared_ptr<void> m_awaiter_task;
    tid_t                m_awaiter_tid;
    event_arg_t<Event>   m_arg;
    void               (*m_advance_state)(pe::shared_ptr<void>, TaskState);

public:

    template <typename TaskType>
    EventAwaitable(TaskType& task)
        : m_scheduler{task.Scheduler()}
        , m_awaiter{task.Schedulable()}
        , m_awaiter_task{task.shared_from_this()}
        , m_awaiter_tid{task.TID()}
        , m_arg{}
        , m_advance_state(+[](pe::shared_ptr<void> ptr, TaskState state){
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            auto old = task->m_coro->Promise().PollState();
            std::size_t event = static_cast<std::size_t>(Event);
            while(true) {
                if(task->m_coro->Promise().TryAdvanceState(old,
                    {state, u16(old.m_awaiting_event_mask & ~(0b1 << event)),
                    old.m_queued_event_mask, old.m_awaiter})) {
                    break;
                }
            }
        })
    {}

    void SetArg(event_arg_t<Event> arg)
    {
        m_arg = arg;
    }

    bool await_ready()
    {
        // TODO: check if we can dequeue a task from our local event queue
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> awaiter_handle) noexcept;

    event_arg_t<Event> await_resume()
    {
        return m_arg;
    }

    void AdvanceState(TaskState state)
    {
        m_advance_state(m_awaiter_task, state);
    }

    tid_t AwaiterTID()
    {
        return m_awaiter_tid;
    }

    Schedulable Awaiter()
    {
        return m_awaiter;
    }
};

/*****************************************************************************/
/* YIELD AWAITABLE                                                           */
/*****************************************************************************/

struct YieldAwaitable
{
    Scheduler&  m_scheduler;
    Schedulable m_schedulable;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) const noexcept;
    void await_resume() const noexcept {}
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

template <typename ReturnType, typename TaskType>
struct TaskPromise : public std::conditional_t<
    std::is_void_v<ReturnType>,
    TaskVoidPromiseBase<TaskPromise<ReturnType, TaskType>>,
    TaskValuePromiseBase<ReturnType, TaskPromise<ReturnType, TaskType>>
>
{
public:

    struct alignas(16) ControlBlock
    {
        TaskState          m_state;
        uint16_t           m_awaiting_event_mask;
        uint16_t           m_queued_event_mask;
        const Schedulable *m_awaiter;
    };

private:

    friend struct TaskVoidPromiseBase<TaskPromise<ReturnType, TaskType>>;
    friend struct TaskValuePromiseBase<ReturnType, TaskPromise<ReturnType, TaskType>>;

    using promise_type = TaskPromise<ReturnType, TaskType>;
    using coroutine_type = Coroutine<promise_type>;
    using value_type = std::conditional_t<
        std::is_void_v<ReturnType>, 
        std::monostate, 
        ReturnType
    >;

    using AtomicControlBlock = DoubleQuadWordAtomic<ControlBlock>;

    AtomicControlBlock          m_state;
    value_type                  m_value;
    std::exception_ptr          m_exception;
    Schedulable                 m_awaiter;
    /* Keep around a shared pointer to the Task instance which has 
     * the 'Run' coroutine method. This way we will prevent 
     * destruction of that instance until the 'Run' method runs to 
     * completion and the promise destructor is invoked, allowing 
     * us to safely use the 'this' pointer in the method without 
     * worrying about the instance lifetime.
     */
    pe::shared_ptr<TaskType>    m_task;

public:

    bool TryAdvanceState(ControlBlock& expected, ControlBlock next)
    {
        return m_state.CompareExchange(expected, next,
            std::memory_order_release, std::memory_order_relaxed);
    }

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
            (m_state.Load(std::memory_order_acquire).m_state == TaskState::eSuspended)
        };
    }

    YieldAwaitable final_suspend() noexcept
    {
        auto state = PollState();
        bool done = false;

        while(!done) {
            switch(state.m_state) {
            case TaskState::eRunning:
                if(state.m_awaiter) {
                    if(TryAdvanceState(state,
                        {TaskState::eJoined, state.m_awaiting_event_mask, 
                        state.m_queued_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }else{
                    if(TryAdvanceState(state,
                        {TaskState::eZombie, state.m_awaiting_event_mask,
                        state.m_queued_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }
                break;
            default:
                pe::assert(0);
            }
        }

        /* We have an awaiter */
        if(state.m_awaiter) {
            auto ret = YieldAwaitable{m_task->Scheduler(), *state.m_awaiter};
            m_awaiter = {};
            return ret;
        }

        /* We terminated due to an unahndled exception, but don't 
         * have an awaiter. Propagate the exception to the main thread.
         */
        // TODO: this doesn't gel with noexcept
        // propagate it to the yield awaitable instead...
        if(m_exception)
            std::rethrow_exception(m_exception);
        return {m_task->Scheduler(), {}};
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
        m_value = std::forward<From>(value);
        auto state = PollState();
        bool done = false;

        while(!done) {
            switch(state.m_state) {
            case TaskState::eRunning:
                if(state.m_awaiter) {
                    if(TryAdvanceState(state,
                        {TaskState::eSuspended, state.m_awaiting_event_mask,
                        state.m_queued_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }else{
                    if(TryAdvanceState(state,
                        {TaskState::eYieldBlocked, state.m_awaiting_event_mask,
                        state.m_queued_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }
                break;
            default:
                pe::assert(0);
            }
        }

        /* We have an awaiter */
        if(state.m_awaiter) {
            auto ret = YieldAwaitable{m_task->Scheduler(), *state.m_awaiter};
            m_awaiter = {};
            return ret;
        }

        /* We have become yield-blocked */
        return {m_task->m_scheduler, {}};
    }

    template <typename U = ReturnType>
    requires (std::is_void_v<U>)
    YieldAwaitable yield_value(const VoidType&)
    {
        auto state = PollState();
        bool done = false;

        while(!done) {
            switch(state.m_state) {
            case TaskState::eRunning:
                if(state.m_awaiter) {
                    if(TryAdvanceState(state,
                        {TaskState::eSuspended, state.m_awaiting_event_mask,
                        state.m_queued_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }else{
                    if(TryAdvanceState(state,
                        {TaskState::eYieldBlocked, state.m_awaiting_event_mask,
                        state.m_queued_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }
                break;
            default:
                pe::assert(0);
            }
        }

        /* We have an awaiter */
        if(state.m_awaiter) {
            auto ret = YieldAwaitable{m_task->m_scheduler, *state.m_awaiter};
            m_awaiter = {};
            return ret;
        }

        /* We have become yield-blocked */
        return {m_task->m_scheduler, {}};
    }

    template <typename... Args>
    TaskPromise(TaskType& task, Args&... args)
        : m_state{{task.InitiallySuspended() ? TaskState::eSuspended : TaskState::eRunning,
            0, 0, nullptr}}
        , m_value{}
        , m_exception{}
        , m_awaiter{}
        , m_task{task.shared_from_this()}
    {
        task.m_coro = pe::make_shared<Coroutine<promise_type>>(
            std::coroutine_handle<promise_type>::from_promise(*this),
            typeid(task).name()
        );
    }

    const Schedulable Schedulable()
    {
        return m_task->Schedulable();
    }

    struct Schedulable *SetAwaiter(struct Schedulable schedulable)
    {
        m_awaiter = schedulable;
        return &m_awaiter;
    }

    pe::shared_ptr<TaskType> Task()
    {
        return m_task;
    }

    ControlBlock PollState()
    {
        return m_state.Load(std::memory_order_acquire);
    }

    /* This is only called when the task is already suspended */
    void Terminate()
    {
        m_task.reset();
        m_state.Store({TaskState::eJoined, 0, 0, nullptr}, std::memory_order_release);
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

[[maybe_unused]] static std::atomic_uint32_t s_next_tid{0};

/*
 * An abstract base class for implementing user tasks. Can yield
 * different kinds of awaitables from its' methods.
 */
template <typename ReturnType, typename Derived, typename... Args>
class Task : public pe::enable_shared_from_this<Derived>
{
private:

    using coroutine_ptr_type = SharedCoroutinePtr<TaskPromise<ReturnType, Derived>>;
    using event_queue_type = LockfreeQueue<event_variant_t>;

    static constexpr bool is_traced_type = false;
    static constexpr bool is_logged_type = false;

    Scheduler&              m_scheduler;
    uint32_t                m_priority;
    bool                    m_initially_suspended;
    Affinity                m_affinity;
    tid_t                   m_tid;
    coroutine_ptr_type      m_coro;
    std::bitset<kNumEvents> m_subscribed;

    std::array<event_queue_type, kNumEvents> m_event_queues;
    std::atomic<event_queue_type*>           m_event_queues_base;

    std::array<AtomicOperationCounter, kNumEvents> m_event_queue_counters;
    std::atomic<AtomicOperationCounter*>           m_event_queue_counters_base;

    template <typename OtherReturnType, typename OtherTaskType>
    friend struct TaskPromise;

    template <EventType Event>
    friend struct EventAwaitable;
    friend struct EventSubscriber;

    template <EventType Event>
    void clear_event_queued_bit()
    {
        std::size_t event = static_cast<std::size_t>(Event);
        auto state = m_coro->Promise().PollState();

        while(true) {
            if(m_coro->Promise().TryAdvanceState(state,
                {state.m_state, 
                state.m_awaiting_event_mask,
                u16(state.m_queued_event_mask & ~(0b1 << event)),
                state.m_awaiter})) {
                break;
            }
        }
    }

    template <EventType Event>
    void set_event_queued_bit()
    {
        std::size_t event = static_cast<std::size_t>(Event);
        auto state = m_coro->Promise().PollState();

        while(true) {
            if(m_coro->Promise().TryAdvanceState(state,
                {state.m_state, 
                state.m_awaiting_event_mask,
                u16(state.m_queued_event_mask | (0b1 << event)),
                state.m_awaiter})) {
                break;
            }
        }
    }

    template <EventType Event>
    event_arg_t<Event> next_event()
    {
        std::size_t event = static_cast<std::size_t>(Event);

        auto queues_base = m_event_queues_base.load(std::memory_order_acquire);
        auto& queue = queues_base[event];

        auto counters_base = m_event_queue_counters_base.load(std::memory_order_acquire);
        auto& counter = counters_base[event];

        /* We have a guarantee that an event is getting
         * pushed into the queue, but we have to wait
         * for the side-effects to become visible. In
         * a pathological worst-case scenario, the other
         * thread can get scheduled out in between advancing
         * the task's state and queuing the event. We have
         * to spin on the queue until we get something,
         * which very well may be a different event that
         * managed to get into the queue.
         */
        std::optional<event_variant_t> curr;
        OptimisticAccess access{100, "Waiting too long for queue item. Blocking..."};
        while(true) {
            curr = queue.Dequeue();
            if(curr.has_value())
                break;
            access.YieldIfStalled();
        }

        auto [prev, min_size] = counter.AcknowldedgeOne();
        uint32_t incomplete;

        if(min_size == 0) {

            clear_event_queued_bit<Event>();
            /* We cleared the bit, but may have overwritten
             * a bit-set operation by a different thread.
             * We are guaranteed to be able to detect this
             * using the operation counter and the temporary
             * spurrious clearing of the bit is not a hazard
             * since the code which branches on the bit status
             * is serialized through this path.
             */
            if(counter.SuccessfulOperationSinceLastAck(prev)) {
                set_event_queued_bit<Event>();
            }else if((incomplete = counter.IncompleteOperationsSinceLastAck(prev))) {
                counter.WaitCompleted(prev, incomplete, 100);
                if(counter.SuccessfulOperationSinceLastAck(prev)) {
                    set_event_queued_bit<Event>();
                }
            }
        }

        return static_event_cast<Event>(curr.value());
    }

    template <EventType Event>
    bool notify(event_arg_t<Event> arg)
    {
        constexpr std::size_t event = static_cast<std::size_t>(Event);
        auto state = m_coro->Promise().PollState();
        bool done = false;

        while(!done) {
            switch(state.m_state) {
            case TaskState::eEventBlocked:
                if(state.m_awaiting_event_mask & (0b1 << event))
                    return false;
                break;
            default:
                m_event_queue_counters[event].IncrementAttempts();

                if(m_coro->Promise().TryAdvanceState(state,
                    {state.m_state, state.m_awaiting_event_mask,
                    u16(state.m_queued_event_mask | (0b1 << event)),
                    state.m_awaiter})) {

                    m_event_queue_counters[event].IncrementSuccesses();
                    done = true;
                    break;
                }else{
                    m_event_queue_counters[event].IncrementFailures();
                }
            }
        }

        /* We know that if we managed to set the bit, 
         * the task could not have advanced to the
         * eEventBlocked state.
         */
        auto queues_base = m_event_queues_base.load(std::memory_order_acquire);
        auto& queue = queues_base[event];
        queue.Enqueue(
            event_variant_t{std::in_place_index_t<event>{}, arg});
        return true;
    }

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

    using promise_type = TaskPromise<ReturnType, Derived>;
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
            return pe::make_shared<Derived, Derived::is_traced_type, Derived::is_logged_type>(
                TaskCreateToken{}, scheduler, priority, initially_suspended, affinity,
                std::forward<decltype(args)>(args)...
            );
        };
        auto ret = std::apply(callmakeshared, constructor_args);

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

    Schedulable Schedulable() const
    {
        return {m_priority, m_coro, m_affinity};
    }

    bool Done() const;
    std::string Name() const;

    terminate_awaitable_type Terminate();

    YieldAwaitable Yield(enum Affinity affinity);

    template <typename Task, typename Message, typename Response>
    awaitable_type Send(Task& task, Message m, Response& r);

    template <typename Task, typename Message>
    awaitable_type Receive(Task& task, Message& m);

    template <typename Task, typename Response>
    awaitable_type Reply(Task& task, Response& r);

    template <EventType Event>
    void Subscribe();

    template <EventType Event>
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
    bool             (*m_notify)(pe::shared_ptr<void>, void*);

    EventSubscriber()
        : m_tid{}
        , m_task{}
        , m_notify{}
    {}

    template <EventType Event, typename Task>
    EventSubscriber(std::integral_constant<EventType, Event> type, pe::shared_ptr<Task> task)
        : m_tid{task->TID()}
        , m_task{pe::static_pointer_cast<void>(task)}
        , m_notify{+[](shared_ptr<void> ptr, void *arg) {
            auto task = pe::static_pointer_cast<Task>(ptr);
            event_arg_t<Event> *event_arg = reinterpret_cast<event_arg_t<Event>*>(arg);
            return task->template notify<Event>(*event_arg);
        }}
    {}

    template <EventType Event>
    bool Notify(event_arg_t<Event> arg)
    {
        if(auto ptr = m_task.lock()) {
            return m_notify(ptr, &arg);
        }
        return true;
    }

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
    using awaitable_variant_type = event_awaitable_ref_variant_t<EventAwaitable>;
    using event_queue_type = LockfreeQueue<awaitable_variant_type>;

    using subscriber_type = EventSubscriber;
    using subscriber_list_type = IterableLockfreeList<subscriber_type>;

    queue_type               m_ready_queue;
    queue_type               m_main_ready_queue;
    std::mutex               m_ready_lock;
    std::condition_variable  m_ready_cond;
    std::size_t              m_nworkers;
    std::vector<std::thread> m_worker_pool;

    /* 
     * Protect event queue access with a Multiple-Producer 
     * Single-Consumer guard. This will serialize the event
     * notifications, ensuring that in the presense of multiple
     * event notifiers for the same event, all subscribers will
     * see all the events in the same order.
     */
    struct EventNotificationRequest
    {
        const event_variant_t                  m_arg;
        const std::size_t                      m_num_subs;
        AtomicOperationCounter                 m_sub_dequeue_count;
        LockfreeQueue<EventSubscriber>         m_sub_queue;
        LockfreeList<tid_t>                    m_subs_set;
        LockfreeQueue<awaitable_variant_type>  m_awaiters_queue;
        LockfreeList<tid_t>                    m_awaiters_set;

        EventNotificationRequest(event_variant_t arg, std::vector<EventSubscriber> subs)
            : m_arg{arg}
            , m_num_subs{subs.size()}
            , m_sub_dequeue_count{}
            , m_sub_queue{}
            , m_subs_set{}
            , m_awaiters_queue{}
            , m_awaiters_set{}
        {
            for(auto& sub : subs) {
                m_sub_queue.Enqueue(sub);
            }
        }
    };

    using queue_guard_type = MPSCGuard<event_queue_type, EventNotificationRequest>;

    std::array<event_queue_type, kNumEvents> m_event_queues;
    std::array<queue_guard_type, kNumEvents> m_event_queue_guards;

    /* Pointer for safely publishing the completion of list creation.
     */
    std::atomic<subscriber_list_type*>           m_subscribers_base;
    std::array<subscriber_list_type, kNumEvents> m_subscribers;

    template <EventType Event>
    void await_event(EventAwaitable<Event>& awaitable);

    template <EventType Event>
    void service_notification_request(event_queue_type *queue,
        pe::shared_ptr<EventNotificationRequest> request);

    template <EventType Event>
    void notify_event(event_arg_t<Event> arg);

    template <EventType Event>
    void add_subscriber(const EventSubscriber sub);

    template <EventType Event>
    void remove_subscriber(const EventSubscriber sub);

    template <EventType Event>
    bool has_subscriber(const EventSubscriber sub);

    void enqueue_task(Schedulable schedulable);
    void work();
    void main_work();

    template <typename ReturnType, typename PromiseType>
    friend struct TaskAwaitable;

    template <EventType Event>
    friend struct EventAwaitable;
    friend struct TaskCondAwaitable;
    friend struct YieldAwaitable;

    template <typename ReturnType, typename TaskType>
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
    auto state = promise.PollState();

    while(true) {

        switch(state.m_state) {
        case TaskState::eSuspended:
            return false;
        case TaskState::eEventBlocked:
            return false;
        case TaskState::eYieldBlocked:
            if(promise.TryAdvanceState(state, 
                {TaskState::eSuspended, state.m_awaiting_event_mask,
                state.m_queued_event_mask, nullptr})) {

                return true;
            }
            break;
        case TaskState::eRunning:
            return false;
        case TaskState::eZombie:
            if(promise.TryAdvanceState(state,
                {TaskState::eJoined, state.m_awaiting_event_mask,
                state.m_queued_event_mask, nullptr})) {

                return true;
            }
            break;
        case TaskState::eJoined:
            throw std::runtime_error{"Cannot await a joined task."};
        }
    }
}

template <typename ReturnType, typename PromiseType>
template <typename OtherReturnType, typename OtherTaskType>
bool TaskAwaitable<ReturnType, PromiseType>::await_suspend(
    handle_type<OtherReturnType, OtherTaskType> awaiter_handle)
{
    auto& promise = m_coro->Promise();
    auto state = promise.PollState();
    struct Schedulable awaiter = awaiter_handle.promise().Schedulable();
    struct Schedulable *ptr = promise.SetAwaiter(awaiter);

    while(true) {
        switch(state.m_state) {
        case TaskState::eSuspended:
            if(promise.TryAdvanceState(state,
                {TaskState::eRunning, state.m_awaiting_event_mask,
                state.m_queued_event_mask, ptr})) {

                m_scheduler.enqueue_task(promise.Schedulable());
                return true;
            }
            break;
        case TaskState::eYieldBlocked:
            if(promise.TryAdvanceState(state,
                {TaskState::eSuspended, state.m_awaiting_event_mask,
                state.m_queued_event_mask, nullptr})) {
                return false;
            }
            break;
        case TaskState::eZombie:
            if(promise.TryAdvanceState(state,
                {TaskState::eJoined, state.m_awaiting_event_mask,
                state.m_queued_event_mask, nullptr})) {
                return false;
            }
            break;
        case TaskState::eJoined:
            return false;
        case TaskState::eEventBlocked:
        case TaskState::eRunning:
            if(state.m_awaiter) {
                throw std::runtime_error{
                    "Cannot await a task which already has an awaiter."};
            }
            if(promise.TryAdvanceState(state,
                {state.m_state, state.m_awaiting_event_mask,
                state.m_queued_event_mask, ptr})) {

                return true;
            }
            break;
        }
    }
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

void YieldAwaitable::await_suspend(
    std::coroutine_handle<>) const noexcept
{
    if(m_schedulable.m_handle) {
        m_scheduler.enqueue_task(m_schedulable);
    }
}

template <EventType Event>
template <typename PromiseType>
bool EventAwaitable<Event>::await_suspend(
    std::coroutine_handle<PromiseType> awaiter_handle) noexcept
{
    auto state = awaiter_handle.promise().PollState();
    std::size_t event = static_cast<std::size_t>(Event);

    while(true) {

        pe::assert(!(state.m_awaiting_event_mask & (0b1 << event)));

        if(state.m_queued_event_mask & (0b1 << event)) {

            auto task = awaiter_handle.promise().Task();
            event_arg_t<Event> event = task->template next_event<Event>();
            /* No additional synchronization is necessary since 
             * we know it's going to be read from the same thread.
             */
            SetArg(event);
            AdvanceState(TaskState::eRunning);
            return false;

        }else{
            if(awaiter_handle.promise().TryAdvanceState(state,
                {TaskState::eEventBlocked,
                u16(state.m_awaiting_event_mask | (0b1 << event)),
                state.m_queued_event_mask, state.m_awaiter})) {
                break;
            }
        }
    }

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
    , m_event_queues{}
    , m_event_queue_counters{}
{
    m_event_queues_base.store(&m_event_queues[0], std::memory_order_release);
    m_event_queue_counters_base.store(&m_event_queue_counters[0], std::memory_order_release);
}

template <typename ReturnType, typename Derived, typename... Args>
bool Task<ReturnType, Derived, Args...>::Done() const
{
    if(!m_coro)
        return false;
    auto state = m_coro->Promise().PollState();
    return (state.m_state == TaskState::eJoined)
        || (state.m_state == TaskState::eZombie);
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
YieldAwaitable
Task<ReturnType, Derived, Args...>::Yield(enum Affinity affinity)
{
    if((m_affinity == Affinity::eMainThread)
    && (affinity != Affinity::eMainThread))
        throw std::runtime_error{
            "Cannot yield with a more relaxed affinity than the task was created with."};
    return {m_scheduler, {m_priority, m_coro, affinity}};
}

template <typename ReturnType, typename Derived, typename... Args>
template <EventType Event>
void Task<ReturnType, Derived, Args...>::Subscribe()
{
    m_subscribed.set(static_cast<std::size_t>(Event));
    m_scheduler.template add_subscriber<Event>(EventSubscriber{
        std::integral_constant<EventType, Event>{},
        this->shared_from_this()
    });
}

template <typename ReturnType, typename Derived, typename... Args>
template <EventType Event>
requires (Event < EventType::eNumEvents)
typename Task<ReturnType, Derived, Args...>::template event_awaitable_type<Event>
Task<ReturnType, Derived, Args...>::Event()
{
    std::size_t event = static_cast<std::size_t>(Event);
    if(event >= 16)
        throw std::runtime_error("Can only await on events 0-15.");
    if(!m_subscribed.test(event))
        throw std::runtime_error{"Cannot await event not prior subscribed to."};
    return {*this};
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
    , m_subscribers{}
{
    if constexpr (pe::kLinux) {
        auto handle = pthread_self();
        pthread_setname_np(handle, "main");
    }
    m_subscribers_base.store(&m_subscribers[0], std::memory_order_release);

    for(int i = 0; i < kNumEvents; i++) {
        auto& guard = m_event_queue_guards[i];
        std::destroy_at(&guard);
        new (&guard) MPSCGuard<event_queue_type, EventNotificationRequest>{
            &m_event_queues[i]};
    }
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
    const auto& queue_guard = m_event_queue_guards[event];
    auto& queue = *queue_guard.AcquireProducerAccess();
    queue.Enqueue(std::ref(awaitable));
}

template <EventType Event>
void Scheduler::service_notification_request(event_queue_type *queue,
    pe::shared_ptr<EventNotificationRequest> request)
{
    using awaitable_type = EventAwaitable<Event>;
    using optional_ref_type = std::optional<std::reference_wrapper<awaitable_type>>;

restart:

    /* Phase 1: Drain the awaiters queue. Don't unblock
     * any tasks yet as this can race with a concurrent
     * subscriber Notify call.
     */
    std::optional<awaitable_variant_type> awaiter;
    do{
        awaiter = queue->Dequeue();
        if(awaiter.has_value()) {

            auto& ref = std::get<optional_ref_type>(awaiter.value());
            auto& awaitable = ref.value().get();
            tid_t tid = awaitable.AwaiterTID();

            /* Skip tasks that already got asynchronously notified
             * by the next phase
             */
            if(request->m_subs_set.Find(tid)
            || request->m_awaiters_set.Find(tid)) {

                queue->Enqueue(awaiter.value());
                break;
            }

            request->m_awaiters_set.Insert(tid);
            request->m_awaiters_queue.Enqueue(awaiter.value());
        }
    }while(awaiter.has_value());

    /* Phase 2: Asynchronously send the event to the remaining
     * subscribers which are not event blocked. This is an atomic
     * step and will fail if the subscriber has gotten event-blocked
     * since the queue was last drained.
     */
    OptimisticAccess access{100,
        "Taking too long to dequeue subscriber. Blocking..."};
    std::optional<EventSubscriber> sub;

    while(request->m_sub_dequeue_count.MinSuccesses() < request->m_num_subs) {

        request->m_sub_dequeue_count.IncrementAttempts();
        sub = request->m_sub_queue.Dequeue();

        if(!sub.has_value()) {
            request->m_sub_dequeue_count.IncrementFailures();
            access.YieldIfStalled();
            break;
        }
        /* In the presense of multiple threads, this can give a 
         * false negative (i.e. the check returns false but another
         * thread has already dequeued the corresponding awaiter for 
         * processing. This is not an issue since, in that case,
         * the subscriber is guaranteed to be in the eEventBlocked
         * state, and the subsequent Notify call will catch that.
         * We just suffer an unnecessary trip back to the awaiters
         * queue.
         */
        if(request->m_awaiters_set.Find(sub.value().m_tid)) {
            request->m_sub_dequeue_count.IncrementSuccesses();
            continue;
        }
        request->m_subs_set.Insert(sub.value().m_tid);

        if(!sub.value().template Notify<Event>(
            static_event_cast<Event>(request->m_arg))) {

            /* The subscriber has already transitioned to 
             * eEventBlocked state. Go back to the awaiters
             * queue to fetch it.
             */
            request->m_subs_set.Delete(sub.value().m_tid);
            request->m_sub_queue.Enqueue(sub.value());
            request->m_sub_dequeue_count.IncrementFailures();
            goto restart;
        }else{
            request->m_sub_dequeue_count.IncrementSuccesses();
        }
    }

    /* Phase 3: Unblock the event-blocked subscribers. At this point
     * it is guaranteed that all remaining subscribers have received
     * the event asynchronously.
     */
    do{
        awaiter = request->m_awaiters_queue.Dequeue();
        if(awaiter.has_value()) {

            auto& ref = std::get<optional_ref_type>(awaiter.value());
            auto& awaitable = ref.value().get();

            awaitable.SetArg(static_event_cast<Event>(request->m_arg));
            awaitable.AdvanceState(TaskState::eRunning);
            enqueue_task(awaitable.Awaiter());
        }
    }while(awaiter.has_value());
}

template <EventType Event>
void Scheduler::notify_event(event_arg_t<Event> arg)
{
    constexpr std::size_t event = static_cast<std::size_t>(Event);
    auto& queue_guard = m_event_queue_guards[event];
    event_queue_type *queue = nullptr;

    auto subscribers_base = m_subscribers_base.load(std::memory_order_acquire);
    auto& list = subscribers_base[event];
    auto snapshot = list.TakeSnapshot();

    auto request = pe::make_shared<EventNotificationRequest>(
        event_variant_t{std::in_place_index_t<event>{}, arg}, snapshot);

    pe::shared_ptr<EventNotificationRequest> curr;
    do{
        std::tie(curr, queue) = queue_guard.TryAcquireConsumerAccess(request);
        service_notification_request<Event>(queue, curr);
        queue_guard.TryReleaseConsumerAccess(curr);
    }while(curr != request);
}

template <EventType Event>
void Scheduler::add_subscriber(const EventSubscriber sub)
{
    constexpr std::size_t event = static_cast<std::size_t>(Event);
    auto subscribers_base = m_subscribers_base.load(std::memory_order_acquire);
    auto& list = subscribers_base[event];
    list.Insert(sub);
}

template <EventType Event>
void Scheduler::remove_subscriber(const EventSubscriber sub)
{
    constexpr std::size_t event = static_cast<std::size_t>(Event);
    auto subscribers_base = m_subscribers_base.load(std::memory_order_acquire);
    auto& list = subscribers_base[event];
    list.Delete(sub);
}

template <EventType Event>
bool Scheduler::has_subscriber(const EventSubscriber sub)
{
    constexpr std::size_t event = static_cast<std::size_t>(Event);
    auto subscribers_base = m_subscribers_base.load(std::memory_order_acquire);
    auto& list = subscribers_base[event];
    return list.Find(sub);
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

