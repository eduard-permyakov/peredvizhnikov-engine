export module concurrency;

import <atomic>;

namespace pe{

/*****************************************************************************/
/* VOID TYPE                                                                 */
/*****************************************************************************/
/*
 * Empty type to be used as a void yield value
 */
export struct VoidType {};
export constexpr VoidType Void = VoidType{};

/*****************************************************************************/
/* SYNCHRONIZED YIELD VALUE                                                  */
/*****************************************************************************/
/*
 * Allows safe interleaved assignment and fetching from
 * different threads. Note that it is assumed that each
 * yielded value is always consumed exactly once.
 */
export
template <typename T>
class SynchronizedYieldValue
{
private:

    using value_type = std::conditional_t<std::is_void_v<T>, VoidType, T>;

    std::atomic_flag                 m_empty;
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
        while(!m_empty.test(std::memory_order_acquire));
        m_value = value;
        m_empty.clear(std::memory_order_release);
    }

    template <typename U = T>
    T Consume() requires (!std::is_void_v<U>)
    {
        /* Wait until the value is yielded */
        while(m_empty.test(std::memory_order_acquire));
        T ret = m_value;
        m_empty.test_and_set(std::memory_order_release);
        return ret;
    }

    /*
     * Even when there is no value to be consumed from the yielder,
     * perform the serialization so we are able to have a guarantee 
     * that all side-effects from the yielding thread are visible to 
     * the awaiter.
     */
    template <typename U = T>
    void Yield() requires (std::is_void_v<U>)
    {
        while(!m_empty.test(std::memory_order_acquire));
        m_empty.clear(std::memory_order_release);
    }

    template <typename U = T>
    T Consume() requires (std::is_void_v<U>)
    {
        while(m_empty.test(std::memory_order_acquire));
        m_empty.test_and_set(std::memory_order_release);
    }
};

/*****************************************************************************/
/* SYNCHRONIZED SINGLE YIELD VALUE                                           */
/*****************************************************************************/
/*
 * Same as SynchronizedYieldValue, but allowing the producer
 * to yield the value only a single time. All subsequent yields 
 * will be no-ops.
 */
export
template <typename T>
class SynchronizedSingleYieldValue
{
private:

    std::atomic_flag m_empty{true};
    T                m_value{};

public:

    bool Set(T&& value)
    {
        if(!m_empty.test(std::memory_order_acquire))
            return false;
        m_value = value;
        m_empty.clear(std::memory_order_release);
        return true;
    }

    T Get()
    {
        while(m_empty.test(std::memory_order_acquire));
        return m_value;
    }
};

}; // namespace pe

