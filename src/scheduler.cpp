export module sync:scheduler;
export import <coroutine>;
export import shared_ptr;

import :worker_pool;
import :io_pool;

import concurrency;
import logger;
import platform;
import lockfree_queue;
import lockfree_list;
import lockfree_iterable_list;
import lockfree_sequenced_queue;
import event;
import meta;
import assert;
import atomic_work;
import tls;

import <array>;
import <queue>;
import <cstdint>;
import <thread>;
import <memory>;
import <type_traits>;
import <optional>;
import <bitset>;
import <unordered_set>;
import <stack>;
import <any>;

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
    eSendBlocked,
    eReceiveBlocked,
    eReplyBlocked
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

/*****************************************************************************/
/* VOID TYPE                                                                 */
/*****************************************************************************/
/*
 * Empty type to be used as a void yield value
 */
export struct VoidType {};
export constexpr VoidType Void = VoidType{};

/*****************************************************************************/
/* MESSAGE                                                                   */
/*****************************************************************************/

export
struct Message
{
    pe::weak_ptr<TaskBase> m_sender;
    uint64_t               m_header;
    std::any               m_payload;
};

/*****************************************************************************/
/* TASK INITIAL AWAITABLE                                                    */
/*****************************************************************************/
/*
 * Starts or suspends the coroutine, depending on the creation mode.
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
    template <typename PromiseType>
    void await_suspend(std::coroutine_handle<PromiseType>) const noexcept;
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
    U&& await_resume();

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
    pe::weak_ptr<void>                  m_awaiter_task;
    tid_t                               m_awaiter_tid;
    std::optional<event_arg_t<Event>>   m_arg;
    void                              (*m_advance_state)(pe::weak_ptr<void>, TaskState);
    bool                              (*m_is_notified)(pe::weak_ptr<void>, uint8_t);
    std::optional<event_arg_t<Event>> (*m_next_event)(pe::weak_ptr<void>);

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
        , m_advance_state(+[](pe::weak_ptr<void> ptr, TaskState state){
            auto task = pe::static_pointer_cast<TaskType>(ptr.lock());
            if(!task)
                return;
            auto old = task->m_coro->Promise().PollState();
            std::size_t event = static_cast<std::size_t>(Event);
            while(true) {
                if(task->m_coro->Promise().TryAdvanceState(old,
                    {state, old.m_message_seqnum,
                    old.m_unblock_counter, old.m_notify_counter, 
                    old.m_event_seqnums,
                    u16(old.m_awaiting_event_mask & ~(0b1 << event)),
                    old.m_awaiter})) {
                    break;
                }
            }
        })
        , m_is_notified(+[](pe::weak_ptr<void> ptr, uint8_t counter){
            auto task = pe::static_pointer_cast<TaskType>(ptr.lock());
            if(!task)
                return true;
            auto old = task->m_coro->Promise().PollState();
            uint8_t next_counter = counter + 1;
            if(old.m_notify_counter == counter)
                return false;
            if(old.m_notify_counter == next_counter)
                return true;
            /* It's a 'lagging' call */
            return true;
        })
        , m_next_event(+[](pe::weak_ptr<void> ptr){
            auto task = pe::static_pointer_cast<TaskType>(ptr.lock());
            if(!task)
                return std::optional<event_arg_t<Event>>{};
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
        auto event = m_next_event(m_awaiter_task);
        if(!event.has_value())
            return false;
        m_arg = event.value();
        return true;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> awaiter_handle) noexcept;

    event_arg_t<Event> await_resume()
    {
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

    std::string_view AwaiterName() const;

    Schedulable Awaiter(uint64_t seqnum) const
    {
        return m_awaiter;
    }

    bool IsNotified(uint8_t counter) const
    {
        return m_is_notified(m_awaiter_task, counter);
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
/* SEND AWAITABLE                                                            */
/*****************************************************************************/

struct SendAwaitable
{
private:

    Scheduler&          m_scheduler;
    Schedulable         m_awaiter;
    pe::weak_ptr<void>  m_awaiter_task;
    pe::weak_ptr<void>  m_receiver;
    Message             m_message;
    Message             m_response;
    void              (*m_send_message)(void*, pe::shared_ptr<void>, Scheduler&, const Message&);
    
public:

    template <typename SenderType, typename ReceiverType>
    SendAwaitable(Scheduler& scheduler, Schedulable awaiter, 
        pe::shared_ptr<ReceiverType> receiver, pe::shared_ptr<SenderType>, Message message);

    bool await_ready() noexcept;

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> awaiter) noexcept
    {
        auto receiver = m_receiver.lock();
        if(!receiver) {
            m_response = Message{};
            return false;
        }

        m_send_message(&awaiter.promise(), receiver, m_scheduler, m_message);
        return true;
    }

    Message await_resume() const noexcept
    {
        return m_response;
    }
};

/*****************************************************************************/
/* RECV AWAITABLE                                                            */
/*****************************************************************************/

struct RecvAwaitable
{
private:

    Schedulable              m_awaiter;
    pe::weak_ptr<void>       m_awaiter_task;
    Message                  m_message;

    std::optional<Message> (*m_try_pop_message)(pe::shared_ptr<void>);
    std::optional<Message> (*m_pop_message_or_block)(pe::shared_ptr<void>);

public:

    template <typename AwaiterType>
    RecvAwaitable(Schedulable awaiter, pe::shared_ptr<AwaiterType> task)
        : m_awaiter{awaiter}
        , m_awaiter_task{task}
        , m_message{}
        , m_try_pop_message{+[](pe::shared_ptr<void> awaiter){

            auto task = pe::static_pointer_cast<AwaiterType>(awaiter);
            return task->PollMessage();
        }}
        , m_pop_message_or_block{+[](pe::shared_ptr<void> awaiter){

            struct DequeueState
            {
                AwaiterType::promise_type::ControlBlock m_expected;
                AwaiterType::promise_type&              m_promise;
            };

            using queue_type = AwaiterType::message_queue_type;

            auto task = pe::static_pointer_cast<AwaiterType>(awaiter);
            auto& promise = task->m_coro->Promise();
            auto state = promise.PollState();
            auto dequeue_state = pe::make_shared<DequeueState>(state,
                task->m_coro->Promise());

            std::optional<Message> message;
            typename queue_type::ProcessingResult result;
            uint64_t pseqnum;

            auto processor = +[](const pe::shared_ptr<DequeueState> state, 
                uint64_t seqnum, Message message) {

                auto advanced = +[](uint8_t a, uint8_t b) -> bool {
                    return (static_cast<int8_t>((b) - (a)) < 0);
                };

                auto expected = state->m_expected;
                while(true) {
                    typename AwaiterType::promise_type::ControlBlock newstate{
                        expected.m_state,
                        u8(seqnum),
                        expected.m_unblock_counter,
                        expected.m_notify_counter,
                        expected.m_event_seqnums,
                        expected.m_awaiting_event_mask,
                        expected.m_awaiter
                    };
                    if(advanced(expected.m_message_seqnum, seqnum))
                        break;
                    if(state->m_promise.TryAdvanceState(expected, newstate))
                        break;
                }
                return queue_type::ProcessingResult::eDelete;
            };
            auto fallback = +[](const pe::shared_ptr<DequeueState> state, uint64_t seqnum) {

                auto advanced = +[](uint8_t a, uint8_t b) -> bool {
                    return (static_cast<int8_t>((b) - (a)) < 0);
                };

                auto expected = state->m_expected;
                while(true) {
                    typename AwaiterType::promise_type::ControlBlock newstate{
                        TaskState::eSendBlocked,
                        expected.m_message_seqnum,
                        expected.m_unblock_counter,
                        expected.m_notify_counter,
                        expected.m_event_seqnums,
                        expected.m_awaiting_event_mask,
                        expected.m_awaiter
                    };
                    if(advanced(expected.m_message_seqnum, seqnum))
                        break;
                    if(state->m_promise.TryAdvanceState(expected, newstate))
                        break;
                    if(expected.m_state == TaskState::eSendBlocked)
                        break;
                }
            };
            std::tie(message, result, pseqnum) = task->m_message_queue.ProcessHead(processor,
                fallback, dequeue_state);

            return message;
        }}
    {}

    bool await_ready() noexcept 
    {
        auto awaiter = m_awaiter_task.lock();
        pe::assert(awaiter != nullptr);

        if(auto message = m_try_pop_message(awaiter)) {
            m_message = message.value();
            return true;
        }
        return false;
    }

    template <typename PromiseType>
    bool await_suspend(std::coroutine_handle<PromiseType> awaiter) noexcept
    {
        auto receiver = m_awaiter_task.lock();
        pe::assert(receiver != nullptr);

        if(auto message = m_pop_message_or_block(receiver)) {
            m_message = message.value();
            return false;
        }
        return true;
    }

    Message await_resume() const noexcept
    {
        auto awaiter = m_awaiter_task.lock();
        pe::assert(awaiter != nullptr);

        auto uninitialized = [](pe::weak_ptr<void> const& ptr){
            using wp = pe::weak_ptr<void>;
            return !ptr.owner_before(wp{}) && !wp{}.owner_before(ptr);
        };
        if(uninitialized(m_message.m_sender)) {
            return m_try_pop_message(awaiter).value();
        }
        return m_message;
    }

    void SetMessage(Message message)
    {
        m_message = message;
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
        uint8_t            m_message_seqnum;
        uint8_t            m_unblock_counter;
        uint8_t            m_notify_counter;
        uint16_t           m_event_seqnums;
        uint16_t           m_awaiting_event_mask;
        const Schedulable *m_awaiter;

        bool operator==(const ControlBlock& rhs) const noexcept
        {
            return m_state == rhs.m_state
                && m_message_seqnum == rhs.m_message_seqnum
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
    std::vector<std::string>    m_exception_backtrace;
    Schedulable                 m_awaiter;
    /* Keep around a shared pointer to the Task instance which has 
     * the 'Run' coroutine method. This way we will prevent 
     * destruction of that instance until the 'Run' method runs to 
     * completion and the promise final_suspend method is invoked
     * or the task is terminated at the current suspend point,
     * allowing us to safely use the 'this' pointer in the method 
     * without worrying about the instance lifetime.
     */
    pe::shared_ptr<TaskType>    m_task;

    pe::shared_ptr<TaskType> release_task()
    {
        auto ret = m_task;
        m_task.reset();
        return ret;
    }

public:

    using task_type = TaskType;

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
        m_exception_backtrace = Backtrace();
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
                        {TaskState::eJoined, state.m_message_seqnum,
                        state.m_unblock_counter, state.m_notify_counter,
                        state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }else{
                    if(TryAdvanceState(state,
                        {TaskState::eZombie, state.m_message_seqnum,
                        state.m_unblock_counter, state.m_notify_counter,
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

        /* We terminated due to an unhandled exception but don't 
         * have an awaiter. Propagate the exception to the main thread.
         */
        if(m_exception) {
            TaskException exc(std::string{task->Name()}, task->TID(), 
                m_exception_backtrace, m_exception);
            task->Scheduler().template notify_event<EventType::eUnhandledTaskException>(exc);
        }
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
                        {TaskState::eSuspended, state.m_message_seqnum,
                        state.m_unblock_counter, state.m_notify_counter,
                        state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }else{
                    if(TryAdvanceState(state,
                        {TaskState::eYieldBlocked, state.m_message_seqnum,
                        state.m_unblock_counter, state.m_notify_counter,
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
                        {TaskState::eSuspended, state.m_message_seqnum,
                        state.m_unblock_counter, state.m_notify_counter,
                        state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
                        done = true;
                        break;
                    }
                }else{
                    if(TryAdvanceState(state,
                        {TaskState::eYieldBlocked, state.m_message_seqnum,
                        state.m_unblock_counter, state.m_notify_counter,
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
            0, 0, 0, 0, 0, nullptr}}
        , m_value{}
        , m_exception{}
        , m_awaiter{}
        , m_task{task.shared_from_this()}
    {
        task.m_coro = pe::make_shared<Coroutine<promise_type>>(
            std::coroutine_handle<promise_type>::from_promise(*this),
            std::string{task.Name()}
        );
    }

    const Schedulable Schedulable() const
    {
        return m_task->Schedulable();
    }

    Scheduler& Scheduler() const
    {
        return m_task->Scheduler();
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
        m_awaiter = {};
        m_state.Store({TaskState::eJoined, 0, 0, 0, 0, 0, nullptr},
            std::memory_order_release);
    }

    template <typename U = ReturnType>
    requires (!std::is_void_v<U>)
    U&& Value()
    {
        return std::move(m_value);
    }

    const std::exception_ptr& Exception() const
    {
        return m_exception;
    }
};

/*****************************************************************************/
/* TASK                                                                      */
/*****************************************************************************/

[[maybe_unused]] inline std::atomic_uint32_t s_next_tid{0};

class TaskBase
{
private:

    tid_t                                 m_tid;
    std::unique_ptr<char, void(*)(void*)> m_name;
    pe::shared_ptr<TaskBase>              m_parent;
    std::vector<pe::weak_ptr<TaskBase>>   m_children;
    std::atomic<Message*>                 m_response;
    void                                (*m_release)(TaskBase*);
    void                                (*m_unblock)(pe::shared_ptr<TaskBase>);

public:

    template <typename Derived>
    TaskBase(std::in_place_type_t<Derived>)
        : m_tid{s_next_tid.fetch_add(1, std::memory_order_relaxed)}
        , m_name{Demangle(std::string{typeid(Derived).name()})}
        , m_parent{}
        , m_children{}
        , m_response{}
        , m_release{+[](TaskBase *base){
            auto *task = static_cast<Derived*>(base);
            task->release();
        }}
        , m_unblock{+[](pe::shared_ptr<TaskBase> base){
            auto task = pe::static_pointer_cast<Derived>(base);
            auto& promise = task->m_coro->Promise();
            auto expected = promise.PollState();
            while(true) {
                typename Derived::promise_type::ControlBlock newstate{
                    TaskState::eRunning,
                    expected.m_message_seqnum,
                    expected.m_unblock_counter,
                    expected.m_notify_counter,
                    expected.m_event_seqnums,
                    expected.m_awaiting_event_mask,
                    expected.m_awaiter
                };
                if(promise.TryAdvanceState(expected, newstate))
                    break;
            }
            task->m_scheduler.enqueue_task(task->Schedulable());
        }}
    {}

    virtual ~TaskBase() = default;

    tid_t TID() const
    {
        return m_tid; 
    }

    std::string_view Name() const
    {
        if(!m_name.get())
            return {""};
        return {m_name.get()};
    }

    void SetParent(pe::shared_ptr<TaskBase> parent)
    {
        m_parent = parent;
    }

    void AddChild(pe::shared_ptr<TaskBase> child)
    {
        m_children.push_back(child);
    }

    const std::vector<pe::weak_ptr<TaskBase>>& GetChildren() const
    {
        return m_children;
    }

    void Release()
    {
        m_parent.reset();
        m_release(this);
    }

    void SetResponsePtr(Message *msg)
    {
        m_response.store(msg, std::memory_order_release);
    }

    Message *GetResponsePtr() const
    {
        return m_response.load(std::memory_order_acquire);
    }

    void Unblock(pe::shared_ptr<TaskBase> base)
    {
        m_unblock(base);
    }
};

/*
 * An abstract base class for implementing user tasks. Can yield
 * different kinds of awaitables from its' methods.
 */
template <typename ReturnType, typename Derived, typename... Args>
class Task : public TaskBase
           , public pe::enable_shared_from_this<Derived>
{
private:

    using coroutine_ptr_type = SharedCoroutinePtr<TaskPromise<ReturnType, Derived>>;
    using event_queue_type = LockfreeSequencedQueue<event_variant_t>;
    using message_queue_type = LockfreeSequencedQueue<Message>;

    static constexpr bool is_traced_type = false;
    static constexpr bool is_logged_type = false;

    Scheduler&                               m_scheduler;
    Priority                                 m_priority;
    CreateMode                               m_create_mode;
    Affinity                                 m_affinity;
    tid_t                                    m_tid;
    coroutine_ptr_type                       m_coro;
    std::bitset<kNumEvents>                  m_subscribed;
    std::array<event_queue_type, kNumEvents> m_event_queues;
    std::atomic<event_queue_type*>           m_event_queues_base;
    message_queue_type                       m_message_queue;

    template <typename OtherReturnType, typename OtherTaskType>
    friend struct TaskPromise;

    template <EventType Event>
    friend struct EventAwaitable;
    friend struct EventSubscriber;

    friend struct SendAwaitable;
    friend struct RecvAwaitable;

    template <EventType Event>
    std::optional<event_arg_t<Event>> next_event();

    template <EventType Event>
    bool notify(event_arg_t<Event> arg, uint32_t seqnum, uint32_t counter, uint32_t key);

    void release();

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

    friend class TaskBase;

    Task(TaskCreateToken token, Scheduler& scheduler, Priority priority, 
        CreateMode flags, Affinity affinity);
    virtual ~Task();

    template <typename... ConstructorArgs>
    [[nodiscard]] static pe::shared_ptr<Derived> Create(
        Scheduler& scheduler, Priority priority = Priority::eNormal, 
        CreateMode mode = CreateMode::eLaunchAsync, Affinity affinity = Affinity::eAny, 
        ConstructorArgs&&... args);

    Scheduler&  Scheduler() const     { return m_scheduler; }
    Priority    Priority() const      { return m_priority; }
    CreateMode  GetCreateMode() const { return m_create_mode; }
    Affinity    Affinity() const      { return m_affinity; }
    Schedulable Schedulable() const   { return {m_priority, m_coro, m_affinity}; }

    bool                     Done() const;
    terminate_awaitable_type Terminate();

protected:

    YieldAwaitable Yield(enum Affinity affinity);

    template <typename TaskType>
    SendAwaitable Send(pe::shared_ptr<TaskType> to, Message message);

    RecvAwaitable Receive();
    void Reply(pe::shared_ptr<TaskBase> to, Message message);

    std::optional<Message> PollMessage();

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

    template <std::invocable Callable>
    IOAwaitable<Callable> IO(Callable callable);

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
    bool               (*m_notify)(pe::shared_ptr<void>, void*, uint32_t, uint32_t, uint32_t);
    uint32_t           (*m_get_seqnum)(pe::shared_ptr<void>);
    uint8_t            (*m_get_unblock_counter)(pe::shared_ptr<void>);
    uint8_t            (*m_get_notify_counter)(pe::shared_ptr<void>);
    bool               (*m_try_unblock)(pe::shared_ptr<void>, TaskState, uint32_t, uint8_t);

    EventSubscriber()
        : m_tid{}
        , m_task{}
        , m_notify{}
        , m_get_seqnum{}
        , m_try_unblock{}
    {}

    template <EventType Event, typename TaskType>
    EventSubscriber(std::integral_constant<EventType, Event> type, pe::shared_ptr<TaskType> task)
        : m_tid{task->TID()}
        , m_task{pe::static_pointer_cast<void>(task)}
        , m_notify{+[](shared_ptr<void> ptr, void *arg, uint32_t seqnum, 
            uint32_t counter, uint32_t key) {
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            event_arg_t<Event> *event_arg = reinterpret_cast<event_arg_t<Event>*>(arg);
            return task->template notify<Event>(*event_arg, seqnum, counter, key);
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
        , m_try_unblock{+[](pe::shared_ptr<void> ptr, TaskState state, 
            uint32_t seqnum, uint8_t expected_count){
            auto task = pe::static_pointer_cast<TaskType>(ptr);
            auto old = task->m_coro->Promise().PollState();
            std::size_t event = static_cast<std::size_t>(Event);
            while(true) {
                if(old.m_unblock_counter != expected_count)
                    return false;
                if(old.m_state != TaskState::eEventBlocked)
                    return false;
                if(task->m_coro->Promise().TryAdvanceState(old,
                    {state, old.m_message_seqnum,
                    u8(old.m_unblock_counter + 1),
                    u8(old.m_notify_counter),
                    old.m_event_seqnums,
                    u16(old.m_awaiting_event_mask & ~(0b1 << event)), 
                    old.m_awaiter})) {
                    return true;
                }
            }
        }}
    {}

    template <EventType Event>
    bool Notify(event_arg_t<Event> arg, uint32_t seqnum, 
        uint32_t counter, uint32_t key) const noexcept
    {
        if(auto ptr = m_task.lock()) {
            return m_notify(ptr, &arg, seqnum, counter, key);
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

    bool TryUnblock(TaskState state, uint32_t seqnum, uint8_t count) const
    {
        if(auto ptr = m_task.lock()) {
            return m_try_unblock(ptr, state, seqnum, count);
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
    IOPool            m_io_pool;

    /* Structures for keeping track of and 
     * traversing a parent-child task hierarchy.
     */
    LockfreeIterableSet<pe::weak_ptr<TaskBase>>       m_task_roots;
    TLSAllocation<std::stack<pe::weak_ptr<TaskBase>>> m_task_stacks;
    std::optional<TaskException>                      m_unhandled_exception;

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
            uint32_t               m_unblock_counter;
        };

        uint32_t    m_version;
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

    /* A request to dequeue from the appropriate event queue.
     * The purpose is to serialize the dequeue with event
     * notifications.
     */
    struct alignas(kCacheLineSize) EventDequeueRestartableRequest
    {
        using event_queue_type = LockfreeSequencedQueue<event_variant_t>;

        event_queue_type&                                                     m_event_queue;
        pe::shared_ptr<pe::atomic_shared_ptr<std::optional<event_variant_t>>> m_out;

        static inline auto s_consumed_marker = pe::make_shared<std::optional<event_variant_t>>();

        EventDequeueRestartableRequest(event_queue_type& event_queue, decltype(m_out) out)
            : m_event_queue{event_queue}
            , m_out{out}
        {}
    };

    struct alignas(kCacheLineSize) RestartableRequest
    {
        enum class Type
        {
            eNotify,
            eDequeueEvent
        };

        using arg_type = std::variant<
            EventNotificationRestartableRequest,
            EventDequeueRestartableRequest>;

        static inline std::atomic_uint32_t s_next_version{};

        uint32_t m_version;
        Type     m_type;
        arg_type m_arg;

        template <typename RequestType, typename... Args>
        RestartableRequest(Type type, std::in_place_type_t<RequestType> reqtype, Args&&... args)
            : m_version{s_next_version.fetch_add(1, std::memory_order_relaxed)}
            , m_type{type}
            , m_arg{reqtype, std::forward<Args>(args)...}
        {}

        static void Process(RestartableRequest *request, uint64_t seqnum)
        {
            switch(request->m_type) {
            case Type::eNotify: {
                auto& req = std::get<EventNotificationRestartableRequest>(request->m_arg);
                req.Complete(seqnum);
                break;
            }
            case Type::eDequeueEvent: {
                auto& req = std::get<Scheduler::EventDequeueRestartableRequest>(request->m_arg);
                auto result = req.m_event_queue.Dequeue(seqnum);
                auto ptr = pe::make_shared<std::optional<event_variant_t>>(result);
                auto curr = req.m_out->load(std::memory_order_relaxed);
                while(!curr) {
                    req.m_out->compare_exchange_strong(curr, ptr,
                        std::memory_order_release, std::memory_order_relaxed);
                }
                break;
            }}
        }

        uint32_t Version() const
        {
            return m_version;
        }
    };

    AtomicStatefulSerialWork<RestartableRequest> m_notifications;

    /* Pointer for safely publishing the completion of queue creation.
     */
    std::atomic<event_queue_type*>           m_event_queues_base;
    std::array<event_queue_type, kNumEvents> m_event_queues;

    /* Pointer for safely publishing the completion of list creation.
     */
    std::atomic<subscriber_list_type*>           m_subscribers_base;
    std::array<subscriber_list_type, kNumEvents> m_subscribers;

    template <EventType Event>
    void notify_event(event_arg_t<Event> arg);

    template <EventType Event>
    void add_subscriber(const EventSubscriber sub);

    template <EventType Event>
    void remove_subscriber(const EventSubscriber sub);

    template <EventType Event>
    bool has_subscriber(const EventSubscriber sub);

    void update_hierarchy(pe::shared_ptr<TaskBase> child);
    void clear_root(tid_t tid);

    template <std::invocable<pe::shared_ptr<TaskBase>> Visitor>
    void dfs_helper(pe::shared_ptr<TaskBase> root, Visitor visitor,
        std::unordered_set<tid_t>& visited);

    template <std::invocable<pe::shared_ptr<TaskBase>> Visitor>
    void dfs(Visitor visitor);

    void enqueue_task(Schedulable schedulable);
    void start_system_tasks();
    void Shutdown(std::optional<TaskException> = std::nullopt);

    /* Friends that can access more low-level scheduler 
     * functionality.
     */
    template <typename ReturnType, typename PromiseType>
    friend struct TaskAwaitable;

    template <EventType Event>
    friend struct EventAwaitable;

    friend struct TaskInitialAwaitable;
    friend struct YieldAwaitable;
    friend struct SendAwaitable;

    template <typename ReturnType, typename TaskType>
    friend struct TaskPromise;

    template <typename ReturnType, typename TaskType, typename... Args>
    friend class Task;
    friend class TaskBase;

    template <typename PromiseType>
    friend class Coroutine;

    friend class Latch;
    friend class Barrier;

    friend class QuitHandler;
    friend class ExceptionForwarder;

    friend void PushCurrThreadTask(Scheduler *sched, pe::shared_ptr<TaskBase> task);
    friend void PopCurrThreadTask(Scheduler *sched);
    friend void EnqueueTask(Scheduler *sched, Schedulable task);
    
public:
    Scheduler();
    void Run();
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

template <EventType Event>
std::string_view EventAwaitable<Event>::AwaiterName() const
{
    if(auto task = m_awaiter_task.lock()) {
        auto base = pe::static_pointer_cast<TaskBase>(task);
        return base->Name();
    }
    return {};
}

bool TaskInitialAwaitable::await_ready() const noexcept
{
    return false;
}

template <typename PromiseType>
void TaskInitialAwaitable::await_suspend(std::coroutine_handle<PromiseType> coro) const noexcept
{
    switch(m_mode) {
    case CreateMode::eLaunchAsync:
        m_scheduler.enqueue_task(m_schedulable);
        break;
    case CreateMode::eLaunchSync: {
        auto sched = &coro.promise().Scheduler();
        auto task = coro.promise().Task();
        PushCurrThreadTask(sched, task);
        coro.resume();
        PopCurrThreadTask(sched);
        break;
    }
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
                {TaskState::eSuspended, state.m_message_seqnum,
                state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {

                return true;
            }
            break;
        case TaskState::eRunning:
            return false;
        case TaskState::eZombie:
            if(promise.TryAdvanceState(state,
                {TaskState::eJoined, state.m_message_seqnum,
                state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {

                return true;
            }
            break;
        case TaskState::eJoined:
            throw std::runtime_error{"Cannot await a joined task."};
        case TaskState::eSendBlocked:
            return false;
        case TaskState::eReceiveBlocked:
            return false;
        case TaskState::eReplyBlocked:
            return false;
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
                {TaskState::eRunning, state.m_message_seqnum,
                state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, ptr})) {

                m_scheduler.enqueue_task(promise.Schedulable());
                return true;
            }
            break;
        case TaskState::eYieldBlocked:
            if(promise.TryAdvanceState(state,
                {TaskState::eSuspended, state.m_message_seqnum,
                state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
                return false;
            }
            break;
        case TaskState::eZombie:
            if(promise.TryAdvanceState(state,
                {TaskState::eJoined, state.m_message_seqnum,
                state.m_unblock_counter, state.m_notify_counter,
                state.m_event_seqnums, state.m_awaiting_event_mask, nullptr})) {
                return false;
            }
            break;
        case TaskState::eJoined:
            return false;
        case TaskState::eEventBlocked:
        case TaskState::eSendBlocked:
        case TaskState::eReceiveBlocked:
        case TaskState::eReplyBlocked:
        case TaskState::eRunning:
            if(state.m_awaiter) {
                throw std::runtime_error{
                    "Cannot await a task which already has an awaiter."};
            }
            if(promise.TryAdvanceState(state,
                {state.m_state, 
                state.m_message_seqnum,
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
U&& TaskAwaitable<ReturnType, PromiseType>::await_resume()
{
    auto& promise = m_coro->Promise();
    if(promise.Exception()) {
        std::rethrow_exception(promise.Exception());
        return std::move(U{});
    }
    return std::move(promise.Value());
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
    if(!m_schedulable.m_handle.expired()) {
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
                uint32_t                  m_out_seqnum;
            };
            auto enqueue_state = pe::make_shared<EnqueueState>(event, state,
                awaiter_handle.promise());

            bool success = queue.ConditionallyEnqueue(+[](
                const pe::shared_ptr<EnqueueState> state, 
                uint64_t seqnum, awaitable_variant_type awaitable){

                state->m_out_seqnum = seqnum;
                typename PromiseType::ControlBlock expected = state->m_expected;
                typename PromiseType::ControlBlock newstate{
                    TaskState::eEventBlocked,
                    state->m_expected.m_message_seqnum,
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

template <typename SenderType, typename ReceiverType>
SendAwaitable::SendAwaitable(Scheduler& scheduler, Schedulable awaiter, 
    pe::shared_ptr<ReceiverType> receiver, pe::shared_ptr<SenderType> sender, Message message)
    : m_scheduler{scheduler}
    , m_awaiter{awaiter}
    , m_awaiter_task{sender}
    , m_receiver{receiver}
    , m_message{message}
    , m_response{}
    , m_send_message{+[](void *promise, pe::shared_ptr<void> receiver, 
        Scheduler& scheduler, const Message& message){

        struct EnqueueState
        {
            Scheduler&                  m_scheduler;
            Schedulable                 m_schedulable;
            SenderType::promise_type&   m_sender_promise;
            ReceiverType::promise_type& m_receiver_promise;
        };

        auto task = pe::static_pointer_cast<ReceiverType>(receiver);
        auto& sender_promise = *static_cast<SenderType::promise_type*>(promise);
        auto& receiver_promise = task->m_coro->Promise();
        auto enqueue_state = pe::make_shared<EnqueueState>(scheduler, task->Schedulable(),
            sender_promise, receiver_promise);

        task->m_message_queue.ConditionallyEnqueue(+[](
            const pe::shared_ptr<EnqueueState> state,
            uint64_t seqnum, Message message){

            typename SenderType::promise_type::ControlBlock sender_expected = 
                state->m_sender_promise.PollState();
            typename ReceiverType::promise_type::ControlBlock receiver_expected =
                state->m_receiver_promise.PollState();

            auto advanced = +[](uint8_t a, uint8_t b) -> bool {
                return (static_cast<int8_t>((b) - (a)) < 0);
            };

            while(true) {

                if(receiver_expected.m_state == TaskState::eSendBlocked) {

                    typename ReceiverType::promise_type::ControlBlock newstate{
                        TaskState::eRunning,
                        receiver_expected.m_message_seqnum,
                        receiver_expected.m_unblock_counter,
                        receiver_expected.m_notify_counter,
                        receiver_expected.m_event_seqnums,
                        receiver_expected.m_awaiting_event_mask,
                        receiver_expected.m_awaiter
                    };
                    if(advanced(receiver_expected.m_message_seqnum, seqnum)) {
                        return false;
                    }
                    if(state->m_receiver_promise.TryAdvanceState(receiver_expected, newstate)) {
                        state->m_scheduler.enqueue_task(state->m_schedulable);
                        return true;
                    }
                }else{
                    typename SenderType::promise_type::ControlBlock newstate{
                        TaskState::eReplyBlocked,
                        sender_expected.m_message_seqnum,
                        sender_expected.m_unblock_counter,
                        sender_expected.m_notify_counter,
                        sender_expected.m_event_seqnums,
                        sender_expected.m_awaiting_event_mask,
                        sender_expected.m_awaiter
                    };
                    auto receiver_state = state->m_receiver_promise.PollState();
                    if(advanced(receiver_state.m_message_seqnum, seqnum)) {
                        return false;
                    }
                    if(state->m_sender_promise.TryAdvanceState(sender_expected, newstate))
                        return true;
                    if(sender_expected.m_state == TaskState::eReplyBlocked)
                        return true;
                }
            }

        }, enqueue_state, message);
    }}
{}

bool SendAwaitable::await_ready() noexcept 
{
    auto task = pe::static_pointer_cast<TaskBase>(m_awaiter_task.lock());
    task->SetResponsePtr(&m_response);
    return false;
}

template <typename ReturnType, typename Derived, typename... Args>
Task<ReturnType, Derived, Args...>::Task(TaskCreateToken token, class Scheduler& scheduler, 
    enum Priority priority, CreateMode mode, enum Affinity affinity)
    : TaskBase{std::in_place_type_t<Derived>{}}
    , m_scheduler{scheduler}
    , m_priority{priority}
    , m_create_mode{mode}
    , m_affinity{affinity}
    , m_coro{}
    , m_subscribed{}
    , m_event_queues{}
    , m_event_queues_base{}
    , m_message_queue{}
{
    m_event_queues_base.store(&m_event_queues[0], std::memory_order_release);
}

template <typename ReturnType, typename Derived, typename... Args>
Task<ReturnType, Derived, Args...>::~Task()
{
    m_scheduler.clear_root(TID());
}

template <typename ReturnType, typename Derived, typename... Args>
template <EventType Event>
std::optional<event_arg_t<Event>> 
Task<ReturnType, Derived, Args...>::next_event()
{
    std::size_t event = static_cast<std::size_t>(Event);
    auto queues_base = m_event_queues_base.load(std::memory_order_acquire);
    auto& queue = queues_base[event];

    auto result = pe::make_shared<pe::atomic_shared_ptr<std::optional<event_variant_t>>>();
    auto request = std::make_unique<Scheduler::RestartableRequest>(
        Scheduler::RestartableRequest::Type::eDequeueEvent,
        std::in_place_type_t<Scheduler::EventDequeueRestartableRequest>{},
        queue, result);

    Scheduler().m_notifications.PerformSerially(std::move(request),
        Scheduler::RestartableRequest::Process);

    auto ptr = result->load(std::memory_order_acquire);
    result->store(Scheduler::EventDequeueRestartableRequest::s_consumed_marker,
        std::memory_order_release);

    if(ptr->has_value()) {
        auto ret = static_event_cast<Event>(ptr->value());
        return ret;
    }
    return std::nullopt;
}

template <typename ReturnType, typename Derived, typename... Args>
template <EventType Event>
bool Task<ReturnType, Derived, Args...>::notify(event_arg_t<Event> arg, uint32_t seqnum, 
    uint32_t counter, uint32_t key)
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

            if((expected.m_notify_counter != state->m_notify_counter)
            && (expected.m_notify_counter != next_counter))
                return false;

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
                    expected.m_message_seqnum,
                    expected.m_unblock_counter,
                    next_counter,
                    u16(expected.m_event_seqnums ^ (0b1 << state->m_event)), 
                    expected.m_awaiting_event_mask,
                    expected.m_awaiter
                };

                if(state->m_promise.TryAdvanceState(expected, newstate))
                    return true;

                if((((expected.m_event_seqnums >> state->m_event) & 0b1) == !state->m_seqnum)
                    && (expected.m_notify_counter == next_counter)) {
                    return true;
                }
            }}
        }
    }, enqueue_state, event_variant_t{std::in_place_index_t<event>{}, arg}, key);

    auto state = m_coro->Promise().PollState();
    if((state.m_state == TaskState::eEventBlocked)
    && (state.m_awaiting_event_mask & (0b1 << event))
    && (state.m_notify_counter == counter))
        return false; /* The task already got event-blocked */

    /* If the call to ConditionallyEnqueue has returned and we 
     * are not in the eEventBlocked state, it is guaranteed that 
     * the event has already been enqueued.
     */
    return true;
}

template <typename ReturnType, typename Derived, typename... Args>
void Task<ReturnType, Derived, Args...>::release()
{
    m_coro->Promise().Terminate();
}

template <typename ReturnType, typename Derived, typename... Args>
template <typename... ConstructorArgs>
[[nodiscard]] pe::shared_ptr<Derived> 
Task<ReturnType, Derived, Args...>::Create(
    class Scheduler& scheduler, enum Priority priority, enum CreateMode mode, 
    enum Affinity affinity, ConstructorArgs&&... args)
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
    scheduler.update_hierarchy(ret);

    auto callrun = [&ret](auto&&... args){
        const auto& base = pe::static_pointer_cast<Task<ReturnType, Derived, Args...>>(ret);
        return base->Run(std::forward<decltype(args)>(args)...);
    };
    return std::apply(callrun, run_args);
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
template <typename TaskType>
SendAwaitable Task<ReturnType, Derived, Args...>::Send(
    pe::shared_ptr<TaskType> to, Message message)
{
    return SendAwaitable{m_scheduler, Schedulable(), to, this->shared_from_this(), message};
}

template <typename ReturnType, typename Derived, typename... Args>
RecvAwaitable Task<ReturnType, Derived, Args...>::Receive()
{
    return RecvAwaitable{Schedulable(), this->shared_from_this()};
}

template <typename ReturnType, typename Derived, typename... Args>
void Task<ReturnType, Derived, Args...>::Reply(pe::shared_ptr<TaskBase> to, Message message)
{
    *to->GetResponsePtr() = message;
    to->Unblock(to);
}

template <typename ReturnType, typename Derived, typename... Args>
std::optional<Message> Task<ReturnType, Derived, Args...>::PollMessage()
{
    std::optional<Message> message;
    typename message_queue_type::ProcessingResult result;
    uint64_t pseqnum;

    struct ProcessState
    {
        promise_type& m_promise;
    };

    auto process_state = pe::make_shared<ProcessState>(m_coro->Promise());
    std::tie(message, result, pseqnum) = m_message_queue.ProcessHead(+[](
        const pe::shared_ptr<ProcessState> state, uint64_t seqnum, Message message){

        auto advanced = +[](uint8_t a, uint8_t b) -> bool {
            return (static_cast<int8_t>((b) - (a)) < 0);
        };

        auto expected = state->m_promise.PollState();
        while(true) {
            typename promise_type::ControlBlock newstate{
                expected.m_state,
                u8(seqnum),
                expected.m_unblock_counter,
                expected.m_notify_counter,
                expected.m_event_seqnums,
                expected.m_awaiting_event_mask,
                expected.m_awaiter
            };
            if(advanced(expected.m_message_seqnum, seqnum))
                break;
            if(state->m_promise.TryAdvanceState(expected, newstate))
                break;
        }

        return message_queue_type::ProcessingResult::eDelete;

    }, +[](const pe::shared_ptr<ProcessState> state, uint64_t seqnum){}, process_state);

    return message;
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
    return next_event<Event>();
}

template <typename ReturnType, typename Derived, typename... Args>
template <std::invocable Callable>
IOAwaitable<Callable> Task<ReturnType, Derived, Args...>::IO(Callable callable)
{
    return IOAwaitable<Callable>{m_scheduler, Schedulable(), &m_scheduler.m_io_pool, callable};
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
    , m_io_pool{}
    , m_task_roots{}
    , m_task_stacks{AllocTLS<std::stack<pe::weak_ptr<TaskBase>>>()}
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

void Scheduler::update_hierarchy(pe::shared_ptr<TaskBase> child)
{
    auto& stack = *m_task_stacks.GetThreadSpecific();
    if(!stack.empty() && !stack.top().expired()) {
        auto parent = stack.top().lock();
        parent->AddChild(child);
        child->SetParent(parent);
    }else{
        m_task_roots.Insert(child->TID(), child);
        child->SetParent(nullptr);
    }
}

void Scheduler::clear_root(tid_t tid)
{
    m_task_roots.Delete(tid);
}

template <std::invocable<pe::shared_ptr<TaskBase>> Visitor>
void Scheduler::dfs_helper(pe::shared_ptr<TaskBase> root, Visitor visitor,
    std::unordered_set<tid_t>& visited)
{
    visited.insert(root->TID());
    for(auto node : root->GetChildren()) {
        if(auto child = node.lock()) {
            if(!visited.contains(child->TID())) {
                dfs_helper(child, visitor, visited);
            }
        }
    }
    visitor(root);
}

template <std::invocable<pe::shared_ptr<TaskBase>> Visitor>
void Scheduler::dfs(Visitor visitor)
{
    for(auto weak_root : m_task_roots.TakeSnapshot()) {

        auto root = weak_root.second.lock();
        if(!root)
            continue;

        std::unordered_set<tid_t> visited{};
        dfs_helper(root, visitor, visited);
    }
}

void Scheduler::enqueue_task(Schedulable schedulable)
{
    m_worker_pool.PushTask(schedulable);
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

    auto request = std::make_unique<RestartableRequest>(
        RestartableRequest::Type::eNotify,
        std::in_place_type_t<EventNotificationRestartableRequest>{},
        std::integral_constant<EventType, Event>{},
        event_variant_t{std::in_place_index_t<event>{}, arg},
        queue, snapshot, *this);

    m_notifications.PerformSerially(std::move(request), RestartableRequest::Process);
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
                    bool notified = awaitable.IsNotified(state->m_attempt.m_notify_counter);
                    bool blocked = (desc = state->m_shared_state.m_subs_blocked.Get(tid))
                                && (desc.value().m_seqnum != seqnum);

                    if(notified || blocked)
                        return queue_type::ProcessingResult::eCycleBack;

                    state->m_shared_state.m_subs_blocked.Insert(tid, {seqnum, awaiter});
                    return queue_type::ProcessingResult::eDelete;

                }, +[](const pe::shared_ptr<DequeueState>, uint64_t){}, dequeue_state);

            }while(result != queue_type::ProcessingResult::eCycleBack
                && result != queue_type::ProcessingResult::eNotFound);

            if(auto awaiter_desc = state.m_subs_blocked.Get(sub.m_tid)) {

                auto& variant = awaiter_desc.value().m_awaitable;
                return std::optional<SubUnblockAttempt>{
                    {attempt.m_seqnum, attempt.m_sub, variant,
                    attempt.m_unblock_counter}};
            }

            if(!sub.template Notify<Event>(
                static_event_cast<Event>(state.m_arg), 
                attempt.m_seqnum, attempt.m_notify_counter, seqnum)) {

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
            if(sub.TryUnblock(TaskState::eRunning, attempt.m_seqnum, 
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

    /* It is guaranteed that all tasks will be destroyed
     * upon scheduler shutdown. Furthermore, in the absence
     * of parents retaining references to their children, it
     * is guaranteed that all children will be destroyed
     * before their parents.
     */
    dfs(+[](pe::shared_ptr<TaskBase> node){
        node->Release();
    });

    if(m_unhandled_exception.has_value())
        throw m_unhandled_exception.value();
}

void Scheduler::Shutdown(std::optional<TaskException> exc)
{
    pe::assert(std::this_thread::get_id() == g_main_thread_id);
    m_worker_pool.Quiesce();
    m_io_pool.Quiesce();
    m_unhandled_exception = exc;
}

export
void PushCurrThreadTask(Scheduler *sched, pe::shared_ptr<TaskBase> task)
{
    auto& stack = *sched->m_task_stacks.GetThreadSpecific();
    stack.push(task);
}

export
void PopCurrThreadTask(Scheduler *sched)
{
    auto& stack = *sched->m_task_stacks.GetThreadSpecific();
    stack.pop();
}

export
void EnqueueTask(Scheduler *sched, Schedulable task)
{
    sched->enqueue_task(task);
}

} // namespace pe

