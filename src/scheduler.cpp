export module sync:scheduler;
export import <coroutine>;
export import shared_ptr;

import :worker_pool;
import concurrency;
import logger;
import platform;
import lockfree_list;
import lockfree_iterable_list;
import lockfree_sequenced_queue;
import event;
import meta;
import assert;
import atomic_work;

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

export using ::pe::Affinity;
export using ::pe::Priority;
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

template <std::integral T>
inline uint8_t u8(T val)
{
    return static_cast<uint8_t>(val);
}

/*****************************************************************************/
/* TASK STATE                                                                */
/*****************************************************************************/

enum class TaskState : uint8_t
{
    eSuspended,
    eEventBlocked,
    eYieldBlocked,
    eRunning,
    eZombie,
    eJoined,
};
/*****************************************************************************/
/* TASK CREATE MODE                                                          */
/*****************************************************************************/

export
enum class CreateMode : uint32_t
{
    eLaunchAsync,
    eLaunchSync,
    eSuspend
};

export
CreateMode operator|(const CreateMode& lhs, const CreateMode& rhs) 
{
    return static_cast<CreateMode>(static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs));
}

export
CreateMode operator&(const CreateMode& lhs, const CreateMode& rhs) 
{
    return static_cast<CreateMode>(static_cast<uint32_t>(lhs) & static_cast<uint32_t>(rhs));
}

/*****************************************************************************/
/* VOID TYPE                                                                 */
/*****************************************************************************/
/*
 * Empty type to be used as a void yield value
 */
export struct VoidType {};
export constexpr VoidType Void = VoidType{};

/*****************************************************************************/
/* TASK INITIAL AWAITABLE                                                    */
/*****************************************************************************/
/*
 * Acts as either std::suspend_always or std::suspend_never depending 
 * on the creation flags.
 */
struct TaskInitialAwaitable
{
private:

    Scheduler&  m_scheduler;
    Schedulable m_schedulable;
    CreateMode  m_mode;

public:

    TaskInitialAwaitable(Scheduler& scheduler, Schedulable schedulable, CreateMode mode)
        : m_scheduler{scheduler}
        , m_schedulable{schedulable}
        , m_mode{mode}
    {}

    bool await_ready() const noexcept;
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
struct EventAwaitable : public pe::enable_shared_from_this<EventAwaitable<Event>>
{
private:

    Scheduler&                          m_scheduler;
    Schedulable                         m_awaiter;
    pe::shared_ptr<void>                m_awaiter_task;
    tid_t                               m_awaiter_tid;
    std::optional<event_arg_t<Event>>   m_arg;
    void                              (*m_advance_state)(pe::shared_ptr<void>, TaskState);
    bool                              (*m_is_notified)(pe::shared_ptr<void>, uint32_t);
    bool                              (*m_has_event)(pe::shared_ptr<void>);
    std::optional<event_arg_t<Event>> (*m_next_event)(pe::shared_ptr<void>);

    class EventAwaitableCreateToken 
    {
        EventAwaitableCreateToken() = default;
        friend struct EventAwaitable;
    };

public:

    EventAwaitable(EventAwaitable const&) = delete;
    EventAwaitable& operator=(EventAwaitable const&) = delete;

    template <typename TaskType>
    EventAwaitable(EventAwaitableCreateToken token, TaskType& task)
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
                    {state, old.m_unblock_counter, old.m_notify_counter, 
                    old.m_event_seqnums,
                    u16(old.m_awaiting_event_mask & ~(0b1 << event)),
                    old.m_awaiter})) {
                    break;
                }
            }
        })
        , m_is_notified(+[](pe::shared_ptr<void> ptr, uint32_t seqnum){
            pe::assert(ptr.get(), "", __FILE__, __LINE__);
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            auto old = task->m_coro->Promise().PollState();
            std::size_t event = static_cast<std::size_t>(Event);
            uint32_t read_seqnum = (old.m_event_seqnums >> event) & 0b1;
            return (read_seqnum != seqnum);
        })
        , m_has_event(+[](pe::shared_ptr<void> ptr){
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            return task->template has_event<Event>();
        })
        , m_next_event(+[](pe::shared_ptr<void> ptr){
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            return task->template next_event<Event>();
        })
    {}

    template <typename TaskType>
    static pe::shared_ptr<EventAwaitable> Create(TaskType& task)
    {
        return pe::make_shared<EventAwaitable>(EventAwaitableCreateToken{}, task);
    }

    void SetArg(event_arg_t<Event> arg)
    {
        m_arg = arg;
    }

    bool await_ready()
    {
        return m_has_event(m_awaiter_task);
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> awaiter_handle) noexcept;

    event_arg_t<Event> await_resume()
    {
        if(!m_arg.has_value()) {
            return m_next_event(m_awaiter_task).value();
        }
        return m_arg.value();
    }

    void AdvanceState(TaskState state)
    {
        m_advance_state(m_awaiter_task, state);
    }

    tid_t AwaiterTID() const
    {
        return m_awaiter_tid;
    }

    Schedulable Awaiter(uint64_t seqnum) const
    {
        return m_awaiter;
    }

    bool IsNotified(uint32_t seqnum) const
    {
        pe::assert(m_awaiter_task.get(), "", __FILE__, __LINE__);
        return m_is_notified(m_awaiter_task, seqnum);
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
        uint8_t            m_unblock_counter;
        uint8_t            m_notify_counter;
        uint16_t           m_event_seqnums;
        uint16_t           m_awaiting_event_mask;
        const Schedulable *m_awaiter;

        bool operator==(const ControlBlock& rhs) const noexcept
        {
            return m_state == rhs.m_state
                && m_unblock_counter == rhs.m_unblock_counter
                && m_notify_counter == rhs.m_notify_counter
                && m_event_seqnums == rhs.m_event_seqnums
                && m_awaiting_event_mask == rhs.m_awaiting_event_mask
                && m_awaiter == rhs.m_awaiter;
        }
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

    pe::shared_ptr<TaskType> release_task()
    {
        auto ret = m_task;
        m_task.reset();
        return ret;
    }

public:

    bool TryAdvanceState(ControlBlock& expected, ControlBlock next)
    {
        AnnotateHappensBefore(__FILE__, __LINE__, &m_state);
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

    TaskInitialAwaitable initial_suspend()
    {
        return {
            m_task->Scheduler(),
            Schedulable(),
            m_task->GetCreateMode()
        };
    }

    YieldAwaitable final_suspend() noexcept
    {
        auto state = PollState();
        bool done = false;
        auto task = release_task();

        while(!done) {
            switch(state.m_state) {
            case TaskState::eRunning:
                if(state.m_awaiter) {
                    if(TryAdvanceState(state,
                        {TaskState::eJoined, state.m_unblock_counter, state.m_notify_counter,
                        state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }else{
                    if(TryAdvanceState(state,
                        {TaskState::eZombie, state.m_unblock_counter, state.m_notify_counter,
                        state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
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
            AnnotateHappensAfter(__FILE__, __LINE__, &m_state);
            auto ret = YieldAwaitable{task->Scheduler(), *state.m_awaiter};
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

        return {task->Scheduler(), {}};
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

    template <EventType Event>
    EventAwaitable<Event>&
    await_transform(pe::shared_ptr<EventAwaitable<Event>>&& value)
    {
        return *value;
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
                        {TaskState::eSuspended, state.m_unblock_counter, state.m_notify_counter,
                        state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }else{
                    if(TryAdvanceState(state,
                        {TaskState::eYieldBlocked, state.m_unblock_counter, state.m_notify_counter,
                        state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
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
            AnnotateHappensAfter(__FILE__, __LINE__, &m_state);
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
                        {TaskState::eSuspended, state.m_unblock_counter, state.m_notify_counter,
                        state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }else{
                    if(TryAdvanceState(state,
                        {TaskState::eYieldBlocked, state.m_unblock_counter, state.m_notify_counter,
                        state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
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
            AnnotateHappensAfter(__FILE__, __LINE__, &m_state);
            auto ret = YieldAwaitable{m_task->m_scheduler, *state.m_awaiter};
            m_awaiter = {};
            return ret;
        }

        /* We have become yield-blocked */
        return {m_task->m_scheduler, {}};
    }

    template <typename... Args>
    TaskPromise(TaskType& task, Args&... args)
        : m_state{{(task.GetCreateMode() == CreateMode::eSuspend) ? TaskState::eSuspended : TaskState::eRunning,
            0, 0, 0, 0, nullptr}}
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

    const Schedulable Schedulable() const
    {
        return m_task->Schedulable();
    }

    struct Schedulable *SetAwaiter(struct Schedulable schedulable)
    {
        m_awaiter = schedulable;
        return &m_awaiter;
    }

    pe::shared_ptr<TaskType> Task() const
    {
        return m_task;
    }

    ControlBlock PollState() const
    {
        return m_state.Load(std::memory_order_acquire);
    }

    /* This is only called when the task is already suspended */
    void Terminate()
    {
        m_task.reset();
        m_state.Store({TaskState::eJoined, 0, 0, 0, 0, nullptr},
            std::memory_order_release);
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
    using event_queue_type = LockfreeSequencedQueue<event_variant_t>;

    static constexpr bool is_traced_type = false;
    static constexpr bool is_logged_type = false;

    Scheduler&              m_scheduler;
    Priority                m_priority;
    CreateMode              m_create_mode;
    Affinity                m_affinity;
    tid_t                   m_tid;
    coroutine_ptr_type      m_coro;
    std::bitset<kNumEvents> m_subscribed;

    std::array<event_queue_type, kNumEvents> m_event_queues;
    std::atomic<event_queue_type*>           m_event_queues_base;

    template <typename OtherReturnType, typename OtherTaskType>
    friend struct TaskPromise;

    template <EventType Event>
    friend struct EventAwaitable;
    friend struct EventSubscriber;

    template <EventType Event>
    std::optional<event_arg_t<Event>> next_event()
    {
        std::size_t event = static_cast<std::size_t>(Event);
        auto queues_base = m_event_queues_base.load(std::memory_order_acquire);
        auto& queue = queues_base[event];

        auto arg = queue.Dequeue();
        if(!arg.has_value())
            return std::nullopt;

        return static_event_cast<Event>(arg.value());
    }

    template <EventType Event>
    bool has_event()
    {
        std::size_t event = static_cast<std::size_t>(Event);
        auto queues_base = m_event_queues_base.load(std::memory_order_acquire);
        auto& queue = queues_base[event];

        auto result = queue.ProcessHead(+[](decltype(*this)&, uint64_t, event_variant_t){
            return event_queue_type::ProcessingResult::eIgnore;
        }, *this);
        return (std::get<1>(result) != event_queue_type::ProcessingResult::eNotFound);
    }

    template <EventType Event>
    bool notify(event_arg_t<Event> arg, uint32_t seqnum, uint32_t counter)
    {
        constexpr std::size_t event = static_cast<std::size_t>(Event);
        auto queues_base = m_event_queues_base.load(std::memory_order_acquire);
        auto& queue = queues_base[event];

        struct EnqueueState
        {
            std::size_t                m_event;
            uint32_t                   m_seqnum;
            uint32_t                   m_notify_counter;
            promise_type&              m_promise;
        };
        auto enqueue_state = pe::make_shared<EnqueueState>(event, seqnum,
            counter, m_coro->Promise());

        queue.ConditionallyEnqueue(+[](
            const pe::shared_ptr<EnqueueState> state,
            uint64_t seqnum, event_variant_t event){

            while(true) {

                /* Use the event sequence number (1 bit) and the global
                 * 'notified' counter to discriminate 'lagging' calls
                 * for notifications that have already been completed.
                 */
                auto expected = state->m_promise.PollState();
                uint8_t next_counter = state->m_notify_counter + 1;

                uint32_t read_seqnum = (expected.m_event_seqnums >> state->m_event) & 0b1;
                if(read_seqnum != state->m_seqnum)
                    return (expected.m_notify_counter == next_counter);

                switch(expected.m_state) {
                case TaskState::eEventBlocked:
                    /* We lost the race and the task already became event-blocked.
                     */
                    if(expected.m_awaiting_event_mask & (0b1 << state->m_event))
                        return false;
                    break;
                default: {

                    uint8_t next_counter = state->m_notify_counter + 1;
                    typename promise_type::ControlBlock newstate{
                        expected.m_state,
                        expected.m_unblock_counter,
                        next_counter,
                        u16(expected.m_event_seqnums ^ (0b1 << state->m_event)), 
                        expected.m_awaiting_event_mask,
                        expected.m_awaiter
                    };

                    if(state->m_promise.TryAdvanceState(expected, newstate))
                        return true;

                    return (((expected.m_event_seqnums >> state->m_event) & 0b1) == state->m_seqnum)
                        && (expected.m_notify_counter == next_counter);
                }}
            }
        }, enqueue_state, event_variant_t{std::in_place_index_t<event>{}, arg});

        auto state = m_coro->Promise().PollState();
        uint8_t next_counter = counter + 1;
        if(state.m_notify_counter != next_counter)
            return false; /* This is a 'lagging' call, let it complete. */

        if((state.m_state == TaskState::eEventBlocked)
        && (state.m_awaiting_event_mask & (0b1 << event)))
            return false; /* The task already got event-blocked */

        /* If the call to ConditionallyEnqueue has returned, it is 
         * guaranteed that the event has already been enqueued.
         */
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

    Task(TaskCreateToken token, Scheduler& scheduler, Priority priority, 
        CreateMode flags, Affinity affinity);
    virtual ~Task() = default;

    template <typename... ConstructorArgs>
    [[nodiscard]] static pe::shared_ptr<Derived> Create(
        Scheduler& scheduler, Priority priority = Priority::eNormal, 
        CreateMode mode = CreateMode::eLaunchAsync, Affinity affinity = Affinity::eAny, 
        ConstructorArgs&&... args)
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
                TaskCreateToken{}, scheduler, priority, mode, affinity,
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

    Priority Priority() const
    {
        return m_priority;
    }

    CreateMode GetCreateMode() const
    {
        return m_create_mode;
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
    pe::shared_ptr<EventAwaitable<Event>> Event();

    template <EventType Event>
    requires (Event < EventType::eNumEvents)
    std::optional<event_arg_t<Event>> PollEvent();

    template <EventType Event>
    requires (Event < EventType::eNumEvents)
    void Broadcast(event_arg_t<Event> arg = {});
};

/*****************************************************************************/
/* EVENT SUBSCRIBER                                                          */
/*****************************************************************************/

struct EventSubscriber
{
    tid_t                m_tid;
    pe::weak_ptr<void>   m_task;
    bool               (*m_notify)(pe::shared_ptr<void>, void*, uint32_t, uint32_t);
    uint32_t           (*m_get_seqnum)(pe::shared_ptr<void>);
    uint8_t            (*m_get_unblock_counter)(pe::shared_ptr<void>);
    uint8_t            (*m_get_notify_counter)(pe::shared_ptr<void>);
    bool               (*m_try_advance_state)(pe::shared_ptr<void>, TaskState, uint32_t, uint8_t);

    EventSubscriber()
        : m_tid{}
        , m_task{}
        , m_notify{}
        , m_get_seqnum{}
        , m_try_advance_state{}
    {}

    template <EventType Event, typename TaskType>
    EventSubscriber(std::integral_constant<EventType, Event> type, pe::shared_ptr<TaskType> task)
        : m_tid{task->TID()}
        , m_task{pe::static_pointer_cast<void>(task)}
        , m_notify{+[](shared_ptr<void> ptr, void *arg, uint32_t seqnum, uint32_t counter) {
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            event_arg_t<Event> *event_arg = reinterpret_cast<event_arg_t<Event>*>(arg);
            return task->template notify<Event>(*event_arg, seqnum, counter);
        }}
        , m_get_seqnum{+[](shared_ptr<void> ptr){
            constexpr std::size_t event = static_cast<std::size_t>(Event);
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            const auto& promise = task->m_coro->Promise();
            auto state = promise.PollState();
            return static_cast<uint32_t>((state.m_event_seqnums >> event) & 0b1);
        }}
        , m_get_unblock_counter{+[](pe::shared_ptr<void> ptr){
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            const auto& promise = task->m_coro->Promise();
            auto state = promise.PollState();
            return state.m_unblock_counter;
        }}
        , m_get_notify_counter{+[](pe::shared_ptr<void> ptr){
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            const auto& promise = task->m_coro->Promise();
            auto state = promise.PollState();
            return state.m_notify_counter;
        }}
        , m_try_advance_state{+[](pe::shared_ptr<void> ptr, TaskState state, 
            uint32_t seqnum, uint8_t expected_count){
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            auto old = task->m_coro->Promise().PollState();
            std::size_t event = static_cast<std::size_t>(Event);
            while(true) {
                if(old.m_unblock_counter != expected_count)
                    return false;
                uint32_t read_seqnum = (old.m_event_seqnums >> event) & 0b1;
                if(read_seqnum != seqnum)
                    return false;
                if(task->m_coro->Promise().TryAdvanceState(old,
                    {state, u8(old.m_unblock_counter + 1),
                    u8(old.m_notify_counter),
                    u16(old.m_event_seqnums ^ (0b1 << event)),
                    u16(old.m_awaiting_event_mask & ~(0b1 << event)), old.m_awaiter})) {
                    return true;
                }
            }
        }}
    {}

    template <EventType Event>
    bool Notify(event_arg_t<Event> arg, uint32_t seqnum, uint32_t counter) const noexcept
    {
        if(auto ptr = m_task.lock()) {
            return m_notify(ptr, &arg, seqnum, counter);
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

    std::optional<uint32_t> GetSeqnum() const noexcept
    {
        if(auto ptr = m_task.lock()) {
            return m_get_seqnum(ptr);
        }
        return std::nullopt;
    }

    std::optional<uint8_t> GetUnblockCounter() const noexcept
    {
        if(auto ptr = m_task.lock()) {
            return m_get_unblock_counter(ptr);
        }
        return std::nullopt;
    }

    std::optional<uint8_t> GetNotifyCounter() const noexcept
    {
        if(auto ptr = m_task.lock()) {
            return m_get_notify_counter(ptr);
        }
        return std::nullopt;
    }

    bool TryAdvanceState(TaskState state, uint32_t seqnum, uint8_t count) const
    {
        if(auto ptr = m_task.lock()) {
            return m_try_advance_state(ptr, state, seqnum, count);
        }
        return false;
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
    using awaitable_variant_type = event_awaitable_variant_t<EventAwaitable>;
    using event_queue_type = LockfreeSequencedQueue<awaitable_variant_type>;

    using subscriber_type = EventSubscriber;
    using subscriber_list_type = LockfreeIterableList<subscriber_type>;

    const std::size_t m_nworkers;
    WorkerPool        m_worker_pool;

    /* An event notification request that can be serviced by 
     * multiple threads concurrently. To ensure serialization
     * of events, other threads will help complete an existing
     * request and then race to install their own.
     */
    struct alignas(kCacheLineSize) EventNotificationRestartableRequest
    {
        using optional_sub_ref_type = std::optional<std::reference_wrapper<const EventSubscriber>>;

        struct AwaitableDescriptor
        {
            uint64_t               m_seqnum;
            awaitable_variant_type m_awaitable;
        };

        struct SharedState
        {
            const event_variant_t            m_arg;
            event_queue_type&                m_queue;
            Scheduler&                       m_scheduler;
            LockfreeList<tid_t>              m_subs_notified;
            LockfreeSet<AwaitableDescriptor> m_subs_blocked;
        };

        struct SubAsyncNotificationAttempt
        {
            uint32_t              m_seqnum;
            uint32_t              m_unblock_counter;
            uint32_t              m_notify_counter;
            optional_sub_ref_type m_sub;
        };

        struct SubUnblockAttempt
        {
            uint32_t               m_seqnum;
            optional_sub_ref_type  m_sub;
            awaitable_variant_type m_awaitable;
            uint8_t                m_unblock_counter;
        };

        SharedState m_shared_state;
        AtomicWorkPipeline<
            SharedState,
            /* phase 1: record event sequence numbers and counters
             */
            AtomicParallelWork<EventSubscriber, SubAsyncNotificationAttempt, SharedState>,
            /* phase 2: try to notify subscribers, record those that already got blocked 
             */
            AtomicParallelWork<SubAsyncNotificationAttempt, SubUnblockAttempt, SharedState>,
            /* phase 3: unblock those subscribers that already got blocked 
             */
            AtomicParallelWork<SubUnblockAttempt, std::monostate, SharedState>
        >m_pipeline;

        template <EventType Event>
        EventNotificationRestartableRequest(
            std::integral_constant<EventType, Event>, 
            event_variant_t arg, 
            event_queue_type& queue, 
            std::vector<EventSubscriber> subs, 
            Scheduler& scheduler);

        void Complete(uint64_t seqnum)
        {
            m_pipeline.Complete(seqnum);
        }
    };

    AtomicStatefulSerialWork<EventNotificationRestartableRequest> m_notifications;

    /* Pointer for safely publishing the completion of queue creation.
     */
    std::atomic<event_queue_type*>           m_event_queues_base;
    std::array<event_queue_type, kNumEvents> m_event_queues;

    /* Pointer for safely publishing the completion of list creation.
     */
    std::atomic<subscriber_list_type*>           m_subscribers_base;
    std::array<subscriber_list_type, kNumEvents> m_subscribers;

    template <EventType Event>
    void await_event(EventAwaitable<Event>& awaitable);

    template <EventType Event>
    void notify_event(event_arg_t<Event> arg);

    template <EventType Event>
    void add_subscriber(const EventSubscriber sub);

    template <EventType Event>
    void remove_subscriber(const EventSubscriber sub);

    template <EventType Event>
    bool has_subscriber(const EventSubscriber sub);

    void enqueue_task(Schedulable schedulable);
    void start_system_tasks();
    void Shutdown();

    /* Friends that can access more low-level scheduler 
     * functionality.
     */
    template <typename ReturnType, typename PromiseType>
    friend struct TaskAwaitable;

    template <EventType Event>
    friend struct EventAwaitable;

    friend struct TaskInitialAwaitable;
    friend struct YieldAwaitable;

    template <typename ReturnType, typename TaskType>
    friend struct TaskPromise;

    template <typename ReturnType, typename TaskType, typename... Args>
    friend class Task;

    friend class Latch;
    friend class Barrier;

    friend class QuitHandler;
    
public:
    Scheduler();
    void Run();
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

bool TaskInitialAwaitable::await_ready() const noexcept
{
    return !(m_mode == CreateMode::eSuspend);
}

void TaskInitialAwaitable::await_suspend(std::coroutine_handle<> coro) const noexcept
{
    switch(m_mode) {
    case CreateMode::eLaunchAsync:
        m_scheduler.enqueue_task(m_schedulable);
        break;
    case CreateMode::eLaunchSync:
        coro.resume();
        break;
    case CreateMode::eSuspend:
        break;
    }
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
                {TaskState::eSuspended, state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {

                return true;
            }
            break;
        case TaskState::eRunning:
            return false;
        case TaskState::eZombie:
            if(promise.TryAdvanceState(state,
                {TaskState::eJoined, state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {

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
                {TaskState::eRunning, state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, ptr})) {

                m_scheduler.enqueue_task(promise.Schedulable());
                return true;
            }
            break;
        case TaskState::eYieldBlocked:
            if(promise.TryAdvanceState(state,
                {TaskState::eSuspended, state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
                return false;
            }
            break;
        case TaskState::eZombie:
            if(promise.TryAdvanceState(state,
                {TaskState::eJoined, state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
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
                {state.m_state, 
                state.m_unblock_counter,
                state.m_notify_counter,
                state.m_event_seqnums,
                state.m_awaiting_event_mask, ptr})) {

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
    using awaitable_variant_type = event_awaitable_variant_t<EventAwaitable>;

    std::size_t event = static_cast<std::size_t>(Event);
    auto queues_base = m_scheduler.m_event_queues_base.load(std::memory_order_acquire);
    auto& queue = queues_base[event];

    while(true) {

        auto state = awaiter_handle.promise().PollState();
        pe::assert(!(state.m_awaiting_event_mask & (0b1 << event)));

        auto task = awaiter_handle.promise().Task();
        std::optional<event_arg_t<Event>> arg;
        if((arg = task->template next_event<Event>()).has_value()) {

            /* No additional synchronization is necessary since 
             * we know it's going to be read from the same thread.
             */
            SetArg(arg.value());
            AdvanceState(TaskState::eRunning);
            return false;

        }else{

            struct EnqueueState
            {
                std::size_t               m_event;
                PromiseType::ControlBlock m_expected;
                PromiseType&              m_promise;
            };
            auto enqueue_state = pe::make_shared<EnqueueState>(event, state,
                awaiter_handle.promise());

            bool success = queue.ConditionallyEnqueue(+[](
                const pe::shared_ptr<EnqueueState> state, 
                uint64_t seqnum, awaitable_variant_type awaitable){

                typename PromiseType::ControlBlock expected = state->m_expected;
                typename PromiseType::ControlBlock newstate{
                    TaskState::eEventBlocked,
                    state->m_expected.m_unblock_counter,
                    state->m_expected.m_notify_counter,
                    state->m_expected.m_event_seqnums,
                    u16(state->m_expected.m_awaiting_event_mask | (0b1 << state->m_event)),
                    state->m_expected.m_awaiter
                };

                if(state->m_promise.TryAdvanceState(expected, newstate))
                    return true;

                return (expected.m_state == newstate.m_state
                    && (expected.m_unblock_counter == newstate.m_unblock_counter)
                    && (expected.m_awaiting_event_mask & (0b1 << state->m_event)));

            }, enqueue_state, this->shared_from_this());

            if(success)
                return true;
        }
    }
}

template <typename ReturnType, typename Derived, typename... Args>
Task<ReturnType, Derived, Args...>::Task(TaskCreateToken token, class Scheduler& scheduler, 
    enum Priority priority, CreateMode mode, enum Affinity affinity)
    : m_scheduler{scheduler}
    , m_priority{priority}
    , m_create_mode{mode}
    , m_affinity{affinity}
    , m_tid{s_next_tid++}
    , m_coro{nullptr}
    , m_event_queues{}
{
    m_event_queues_base.store(&m_event_queues[0], std::memory_order_release);
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
void Task<ReturnType, Derived, Args...>::Unsubscribe()
{
    m_subscribed.reset(static_cast<std::size_t>(Event));
    m_scheduler.template remove_subscriber<Event>(EventSubscriber{
        std::integral_constant<EventType, Event>{},
        this->shared_from_this()
    });
}

template <typename ReturnType, typename Derived, typename... Args>
template <EventType Event>
requires (Event < EventType::eNumEvents)
pe::shared_ptr<EventAwaitable<Event>>
Task<ReturnType, Derived, Args...>::Event()
{
    std::size_t event = static_cast<std::size_t>(Event);
    if(event >= 16)
        throw std::runtime_error("Can only await on events 0-15.");
    if(!m_subscribed.test(event))
        throw std::runtime_error{"Cannot await event not prior subscribed to."};
    return EventAwaitable<Event>::Create(*this);
}

template <typename ReturnType, typename Derived, typename... Args>
template <EventType Event>
requires (Event < EventType::eNumEvents)
std::optional<event_arg_t<Event>>
Task<ReturnType, Derived, Args...>::PollEvent()
{
    std::size_t event = static_cast<std::size_t>(Event);
    auto queues_base = m_event_queues_base.load(std::memory_order_acquire);
    return queues_base[event].Dequeue();
}

template <typename ReturnType, typename Derived, typename... Args>
template <EventType Event>
requires (Event < EventType::eNumEvents)
void Task<ReturnType, Derived, Args...>::Broadcast(event_arg_t<Event> arg)
{
    m_scheduler.template notify_event<Event>(arg);
}

Scheduler::Scheduler()
    : m_nworkers{static_cast<std::size_t>(
        std::max(1, static_cast<int>(std::thread::hardware_concurrency())-1))}
    , m_worker_pool{m_nworkers}
    , m_event_queues{}
    , m_subscribers{}
{
    if constexpr (pe::kLinux) {
        auto handle = pthread_self();
        pthread_setname_np(handle, "main");
    }
    m_subscribers_base.store(&m_subscribers[0], std::memory_order_release);
    m_event_queues_base.store(&m_event_queues[0], std::memory_order_release);
    start_system_tasks();
}

void Scheduler::enqueue_task(Schedulable schedulable)
{
    m_worker_pool.PushTask(schedulable);
}

template <EventType Event>
void Scheduler::await_event(EventAwaitable<Event>& awaitable)
{
    constexpr std::size_t event = static_cast<std::size_t>(Event);
    auto queues_base = m_event_queues_base.load(std::memory_order_acquire);
    auto& queue = queues_base[event];
    queue.Enqueue(std::ref(awaitable));
}

template <EventType Event>
void Scheduler::notify_event(event_arg_t<Event> arg)
{
    constexpr std::size_t event = static_cast<std::size_t>(Event);
    auto subscribers_base = m_subscribers_base.load(std::memory_order_acquire);
    auto& list = subscribers_base[event];
    auto snapshot = list.TakeSnapshot();

    auto queues_base = m_event_queues_base.load(std::memory_order_acquire);
    auto& queue = queues_base[event];

    auto request = std::make_unique<EventNotificationRestartableRequest>(
        std::integral_constant<EventType, Event>{},
        event_variant_t{std::in_place_index_t<event>{}, arg},
        queue, snapshot, *this);

    m_notifications.PerformSerially(std::move(request), 
        [](EventNotificationRestartableRequest *request, uint64_t seqnum) {
            request->Complete(seqnum);
        }
    );
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

template <EventType Event>
Scheduler::EventNotificationRestartableRequest::EventNotificationRestartableRequest(
    std::integral_constant<EventType, Event>, event_variant_t arg, event_queue_type& queue, 
    std::vector<EventSubscriber> subs, Scheduler& scheduler)
    : m_shared_state{arg, queue, scheduler}
    , m_pipeline{
        subs, m_shared_state,
        +[](uint64_t, const EventSubscriber& sub, SharedState& state) {

            /* If this is restarted on multiple threads, it's theoretically
             * possible for one thread to get the seqnum, and for another
             * to not get it (if the subscribed task is destroyed concurrently). 
             * This is a benign race (either the task had time to get one more 
             * event queued up before it died, or it didn't) - ether way it 
             * doesn't impact the correctness of the subsequent steps.
             */
            auto seqnum = sub.GetSeqnum();
            auto unblock_counter = sub.GetUnblockCounter();
            auto notify_counter = sub.GetNotifyCounter();

            if(!seqnum.has_value() || !unblock_counter.has_value() || !notify_counter.has_value())
                return std::optional<SubAsyncNotificationAttempt>{};

            return std::optional<SubAsyncNotificationAttempt>{
                SubAsyncNotificationAttempt{seqnum.value(), unblock_counter.value(),
                notify_counter.value(), {std::ref(sub)}}};
        },
        +[](uint64_t seqnum, const SubAsyncNotificationAttempt& attempt, SharedState& state) {

            using awaitable_type = EventAwaitable<Event>;
            using awaitable_ptr_type = pe::shared_ptr<awaitable_type>;
            using queue_type = std::remove_cvref_t<decltype(state.m_queue)>;

            /* Step 1: Cooperatively process the awaiters queue. Don't 
             * unblock any tasks yet as this can race with a concurrent 
             * subscriber Notify call.
             */
            struct DequeueState
            {
                SubAsyncNotificationAttempt m_attempt;
                SharedState&                m_shared_state;
            };

            const EventSubscriber& sub = attempt.m_sub.value().get();
            auto dequeue_state = pe::make_shared<DequeueState>(attempt, state);
            std::optional<awaitable_variant_type> awaiter;
            typename queue_type::ProcessingResult result;
            uint64_t pseqnum;

        restart:
            do{
                std::tie(awaiter, result, pseqnum) = state.m_queue.ProcessHead(+[](
                    const pe::shared_ptr<DequeueState> state, uint64_t seqnum, 
                    awaitable_variant_type awaiter){

                    const auto& ptr = std::get<awaitable_ptr_type>(awaiter);
                    const auto& awaitable = *ptr;
                    tid_t tid = awaitable.AwaiterTID();

                    /* Skip tasks that already got asynchronously notified
                     * by the next step.
                     */
                    std::optional<AwaitableDescriptor> desc{};
                    if(awaitable.IsNotified(state->m_attempt.m_seqnum)
                    || ((desc = state->m_shared_state.m_subs_blocked.Get(tid))
                        && (desc.value().m_seqnum != seqnum))) {
                        return queue_type::ProcessingResult::eCycleBack;
                    }

                    state->m_shared_state.m_subs_blocked.Insert(tid, {seqnum, awaiter});
                    return queue_type::ProcessingResult::eDelete;

                }, dequeue_state);

            }while(result != queue_type::ProcessingResult::eCycleBack
                && result != queue_type::ProcessingResult::eNotFound);

            /* In the presense of multiple threads, this can give a 
             * false negative (i.e. the check returns false but another
             * thread has already accessed the corresponding awaiter for 
             * processing). This is not an issue since, in that case,
             * the subscriber is guaranteed to be in the eEventBlocked
             * state, and the subsequent Notify call will catch that.
             * We just suffer an unnecessary retrying of step 1.
             */
            if(auto awaiter_desc = state.m_subs_blocked.Get(sub.m_tid)) {

                auto unblock_count = sub.GetUnblockCounter();
                if(!unblock_count.has_value())
                    return std::optional<SubUnblockAttempt>{};

                auto& variant = awaiter_desc.value().m_awaitable;
                return std::optional<SubUnblockAttempt>{
                    {attempt.m_seqnum, attempt.m_sub, variant,
                    unblock_count.value()}};
            }

            if(!sub.template Notify<Event>(
                static_event_cast<Event>(state.m_arg), 
                attempt.m_seqnum, attempt.m_notify_counter)) {

                /* The subscriber has already transitioned to 
                 * eEventBlocked state. Go back to the awaiters
                 * queue to fetch it.
                 */
                goto restart;
            }
            return std::optional<SubUnblockAttempt>{};
        },
        +[](uint64_t seqnum, const SubUnblockAttempt& attempt, SharedState& state) {

            using awaitable_type = EventAwaitable<Event>;
            using awaitable_ptr_type = pe::shared_ptr<awaitable_type>;

            const EventSubscriber& sub = attempt.m_sub.value().get();
            if(sub.TryAdvanceState(TaskState::eRunning, attempt.m_seqnum, 
                attempt.m_unblock_counter)) {

                auto& ptr = std::get<awaitable_ptr_type>(attempt.m_awaitable);
                auto& awaitable = *ptr;

                /* Since it is protected by a CAS that will only succed
                 * in one thread, the step of enqueuing the task can be
                 * delayed for an arbitrary amount of time after the 
                 * notification request completes. This is benign - once 
                 * the task is in the eRunning state, we don't know or care 
                 * if it's executing, in the ready queue, or on its' way 
                 * there.
                 */
                awaitable.SetArg(static_event_cast<Event>(state.m_arg));
                state.m_scheduler.enqueue_task(awaitable.Awaiter(seqnum));
            }
            return std::optional<std::monostate>{};
        }
    }
{}

void Scheduler::Run()
{
    m_worker_pool.PerformMainThreadWork();
}

void Scheduler::Shutdown()
{
    pe::assert(std::this_thread::get_id() == g_main_thread_id, "", __FILE__, __LINE__);
    m_worker_pool.Quiesce();
}

} // namespace pe

