export module concurrency;

import platform;
import logger;

import <atomic>;
import <string>;
import <algorithm>;
import <memory>;
import <variant>;

namespace pe{

/*****************************************************************************/
/* ATOMIC SCOPED LOCK                                                        */
/*****************************************************************************/

export
class AtomicScopedLock
{
private:

    std::atomic_flag& m_flag;

public:

    AtomicScopedLock(std::atomic_flag& flag)
        : m_flag{flag}
    {
        pe::dbgtime([&](){
            while(m_flag.test_and_set(std::memory_order_acquire));
        }, [](uint64_t delta) {
            if (delta > 5000) [[unlikely]] {
                pe::ioprint(pe::LogLevel::eWarning, "Acquiring atomic lock took", delta, "cycles.",
                    "(" + std::to_string(pe::rdtsc_usec(delta)) + " usec)");
            }
        });
    }

    ~AtomicScopedLock()
    {
        m_flag.clear(std::memory_order_release);
    }
};

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

    using value_type = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

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
        pe::dbgtime([&](){
            while(!m_empty.test(std::memory_order_acquire));
        }, [](uint64_t delta) {
            if (delta > 5000) [[unlikely]] {
                pe::ioprint(pe::LogLevel::eWarning, "Yielding took", delta, "cycles.",
                    "(" + std::to_string(pe::rdtsc_usec(delta)) + " usec)");
            }
        });
        m_value = std::forward<T>(value);
        m_empty.clear(std::memory_order_release);
    }

    template <typename U = T>
    T Consume() requires (!std::is_void_v<U>)
    {
        /* Wait until the value is yielded */
        pe::dbgtime([&](){
            while(m_empty.test(std::memory_order_acquire));
        }, [](uint64_t delta) {
            if (delta > 5000) [[unlikely]] {
                pe::ioprint(pe::LogLevel::eWarning, "Consuming took", delta, "cycles.",
                    "(" + std::to_string(pe::rdtsc_usec(delta)) + " usec)");
            }
        });
        T ret = m_value;
        m_empty.test_and_set(std::memory_order_release);
        return ret;
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
class SynchronizedSingleSetValue
{
private:

    T                m_value{};
    std::atomic_flag m_empty{true};

public:

    SynchronizedSingleSetValue(T&& initial)
        : m_value{std::forward<T>(initial)}
        , m_empty{false}
    {}

    bool Set(T&& value)
    {
        if(!m_empty.test(std::memory_order_acquire))
            return false;
        m_value = std::forward<T>(value);
        m_empty.clear(std::memory_order_release);
        return true;
    }

    T Ptr()
    {
        while(m_empty.test(std::memory_order_acquire));
        return m_value;
    }
};

/*****************************************************************************/
/* DOUBLE QUAD WORD ATOMIC                                                   */
/*****************************************************************************/

/* std::atomic holding a 128-bit 16-byte aligned value is
 * not guaranteed to be lockfree on all platforms that we
 * want to support. A lockfree implementation requires
 * the CMPXCH16B instruction to be available.
 */
export
template <typename T>
concept DoubleQuadWordAtomicCompatible = requires {

    requires (std::is_trivially_copyable_v<T>);
    requires (std::is_copy_constructible_v<T>);
    requires (std::is_move_constructible_v<T>);
    requires (std::is_copy_assignable_v<T>);
    requires (std::is_move_assignable_v<T>);

    requires (std::alignment_of_v<T> == 16);
    requires (sizeof(T) == 16);
};

inline std::atomic_flag s_supported{std::invoke(
    [](){
        uint32_t eax, ebx, ecx, edx;
        asm volatile(
            "cpuid\n"
            : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
            : "a" (0x0), "c" (0)
        );
        /* Check the CMPXCH16B feature bit */
        return !!(ecx & (0b1 << 13));
    }
)};

export
template<DoubleQuadWordAtomicCompatible T>
class DoubleQuadWordAtomic
{
private:

    T m_value{};
    std::atomic<T> m_fallback;

    static inline constexpr uint64_t *low_qword(T& value)
    {
        auto ptr = reinterpret_cast<uint64_t*>(&value);
        return std::launder(ptr + 0);
    }

    static inline constexpr uint64_t *high_qword(T& value)
    {
        auto ptr = reinterpret_cast<uint64_t*>(&value);
        return std::launder(ptr + 1);
    }

public:

    template <typename... Args>
    DoubleQuadWordAtomic(Args... args)
        : m_value{T{args...}}
        , m_fallback{T{args...}}
    {}

    DoubleQuadWordAtomic() = default;
    DoubleQuadWordAtomic(T value)
        : m_value{value}
        , m_fallback{value}
    {}

    DoubleQuadWordAtomic(DoubleQuadWordAtomic const&) = delete;
    DoubleQuadWordAtomic& operator=(DoubleQuadWordAtomic const&) = delete;

    DoubleQuadWordAtomic(DoubleQuadWordAtomic&&) = delete;
    DoubleQuadWordAtomic& operator=(DoubleQuadWordAtomic&&) = delete;

    inline void Store(T desired, 
        std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        if (!s_supported.test()) [[unlikely]] {
            m_fallback.store(desired, order);
            return;
        }

        asm volatile(
            "movq %1, %%xmm0\n\t"
            "movq %2, %%xmm1\n\t"
            "punpcklqdq %%xmm1, %%xmm0\n\t"
            "movdqa %%xmm0, %0\n"
            : "+m" (m_value)
            : "r" (*low_qword(desired)), "r" (*high_qword(desired))
            : "xmm0", "xmm1"
        );
        std::atomic_thread_fence(order);
    }

    inline T Load(std::memory_order order = std::memory_order_seq_cst) const noexcept
    {
        if (!s_supported.test(std::memory_order_relaxed)) [[unlikely]] {
            return m_fallback.load(order);
        }

        T ret;
        std::atomic_thread_fence(order);
        asm volatile(
            "movdqa %2, %%xmm0\n\t"
            "movq %%xmm0, %0\n\t"
            "movhlps %%xmm0, %%xmm0\n\t"
            "movq %%xmm0, %1\n"
            : "+r" (*low_qword(ret)), "+r" (*high_qword(ret))
            : "m" (m_value)
            : "xmm0"
        );
        return ret;
    }

    inline bool CompareExchange(T& expected, T desired,
        std::memory_order success = std::memory_order_seq_cst,
        std::memory_order failure = std::memory_order_seq_cst) noexcept
    {
        bool result;
        if (!s_supported.test(std::memory_order_relaxed)) [[unlikely]] {
            return m_fallback.compare_exchange_strong(expected, desired, 
                success, failure);
        }

        /* The CMPXCH16B instruction will atomically load the existing 
         * value into RDX:RAX if the comparison fails. Add a fence to 
         * sequence the load.
         */
        std::atomic_thread_fence(failure);
        asm volatile(
            "lock cmpxchg16b %1\n"
            : "=@ccz" (result) , "+m" (m_value)
            , "+a" (*low_qword(expected)), "+d" (*high_qword(expected))
            :  "b" (*low_qword(desired)),   "c" (*high_qword(desired))
            : "cc"
        );
        if(result) {
            /* We have successfully written the value to memory. Place
             * a fence to "publish" the new value. 
             */
            std::atomic_thread_fence(success);
        }
        return result;
    }
};

}; // namespace pe

