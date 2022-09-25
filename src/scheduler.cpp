export module scheduler;
export import <coroutine>;

import <queue>;
import <iostream>;

/*****************************************************************************/
/* MODULE INTERFACE                                                          */
/*****************************************************************************/

namespace pe{

export using tid_t = uint32_t;

struct TaskAwaiter
{
    bool await_ready() noexcept;
    void await_suspend(std::coroutine_handle<> handle) noexcept;
    void await_resume() noexcept;
};

export
template <typename T>
class TaskHandle;

template <typename T>
struct TaskPromise
{
    T                           m_value;
    std::exception_ptr          m_exception;

    TaskHandle<T>               get_return_object();
    void                        unhandled_exception();
    std::suspend_always         initial_suspend();
    std::suspend_always         final_suspend() noexcept;
    std::suspend_always         await_transform(T&& retval);

    template<std::convertible_to<T> From>
    std::suspend_always         yield_value(From&& value);

    template<std::convertible_to<T> From>
    void                        return_value(From&& value);
};

template <typename T>
class TaskHandle final
{
public:

    using promise_type = TaskPromise<T>;
    using handle_type = std::coroutine_handle<promise_type>;

    TaskHandle(const TaskHandle&) = delete;
    TaskHandle& operator=(const TaskHandle&) = delete;

    TaskHandle(TaskHandle&& other) noexcept;
    TaskHandle& operator=(TaskHandle&& other) noexcept;

    TaskHandle() = default;
    explicit TaskHandle(const handle_type coroutine);
    ~TaskHandle<T>();

    T Value();
    void Resume();

private:

    tid_t       m_tid{0};
    handle_type m_handle{0};
};

export
template <typename T, typename... Args>
class Task
{
public:
    [[nodiscard]] virtual TaskHandle<T> Run(Args...) = 0;
};

export class Scheduler
{
public:
    void Run();
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

bool TaskAwaiter::await_ready() noexcept
{
    return true;
}

void TaskAwaiter::await_suspend(std::coroutine_handle<> handle) noexcept
{}

void TaskAwaiter::await_resume() noexcept
{}

template <typename T>
TaskHandle<T> TaskPromise<T>::get_return_object()
{
    return TaskHandle<T>{
        std::coroutine_handle<TaskPromise<T>>::from_promise(*this)
    };
}

template <typename T>
void TaskPromise<T>::unhandled_exception()
{
    m_exception = std::current_exception();
}

template <typename T>
std::suspend_always TaskPromise<T>::initial_suspend()
{
    return {};
}

template <typename T>
std::suspend_always TaskPromise<T>::final_suspend() noexcept
{
    return {};
}

template <typename T>
std::suspend_always TaskPromise<T>::await_transform(T&& retval)
{
    return {};
}

template <typename T>
template<std::convertible_to<T> From>
std::suspend_always TaskPromise<T>::yield_value(From&& value)
{
    m_value = std::forward<From>(value);
    return {};
}

template <typename T>
template<std::convertible_to<T> From>
void TaskPromise<T>::return_value(From&& value)
{
    m_value = std::forward<From>(value);
}

template <typename T>
TaskHandle<T>::TaskHandle(const handle_type coroutine)
    : m_handle{coroutine}
{}

template <typename T>
TaskHandle<T>::~TaskHandle()
{
    if(m_handle) {
        m_handle.destroy();
        m_handle = nullptr;
    }
}

template <typename T>
T TaskHandle<T>::Value()
{
    return m_handle.promise().m_value;
}

template <typename T>
void TaskHandle<T>::Resume()
{
    if(m_handle && !m_handle.done())
        m_handle.resume();
}

template <typename T>
TaskHandle<T>::TaskHandle(TaskHandle<T>&& other) noexcept
{
    if(this == &other)
        return;

    if(m_handle) {
        m_handle.destroy();
        m_handle = nullptr;
    }

    std::swap(m_handle, other.m_handle);
    std::swap(m_tid, other.m_tid);
}

template <typename T>
TaskHandle<T>& TaskHandle<T>::operator=(TaskHandle&& other) noexcept
{
    if(this == &other)
        return *this;

    if(m_handle) {
        m_handle.destroy();
        m_handle = nullptr;
    }

    std::swap(m_handle, other.m_handle);
    std::swap(m_tid, other.m_tid);

    return *this;
}

void Scheduler::Run()
{

}

} // namespace pe

