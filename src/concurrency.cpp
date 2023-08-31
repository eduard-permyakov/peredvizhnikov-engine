/*
 *  This file is part of Peredvizhnikov Engine
 *  Copyright (C) 2023 Eduard Permyakov 
 *
 *  Peredvizhnikov Engine is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Peredvizhnikov Engine is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

export module concurrency;

import platform;
import logger;
import assert;

import <atomic>;
import <string>;
import <algorithm>;
import <memory>;
import <variant>;
import <bit>;
import <optional>;
import <thread>;
import <chrono>;
import <algorithm>;
import <cmath>;

namespace pe{

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

    struct alignas(16) QWords
    {
        uint64_t m_lower;
        uint64_t m_upper;
    };

    struct alignas(16) DWords
    {
        uint32_t m_lower_low;
        uint32_t m_lower_high;
        uint32_t m_upper_low;
        uint32_t m_upper_high;
    };

    T m_value{};
    std::atomic<T> m_fallback;

    static inline constexpr std::byte *low_qword(T& value)
    {
        auto ptr = reinterpret_cast<std::byte*>(&value);
        return (ptr + 0);
    }

    static inline constexpr std::byte *high_qword(T& value)
    {
        auto ptr = reinterpret_cast<std::byte*>(&value);
        return (ptr + sizeof(uint64_t));
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

        auto words = std::bit_cast<QWords>(desired);
        asm volatile(
            "movq %1, %%xmm0\n\t"
            "movq %2, %%xmm1\n\t"
            "punpcklqdq %%xmm1, %%xmm0\n\t"
            "movdqa %%xmm0, %0\n"
            : "=m" (m_value)
            : "r" (words.m_lower), "r" (words.m_upper)
            : "xmm0", "xmm1"
        );
        std::atomic_thread_fence(order);
    }

    inline void StoreLower(uint64_t desired, 
        std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        if (!s_supported.test()) [[unlikely]] {
            T expected = Load(std::memory_order_relaxed);
            QWords newval;
            do{
                newval = std::bit_cast<QWords>(expected);
                newval.m_lower = desired;
            }while(!m_fallback.compare_exchange_weak(expected, std::bit_cast<T>(newval),
                order, std::memory_order_relaxed));
            return;
        }

        asm volatile(
            "movq %1, (%0)\n"
            : /* none */
            : "r" (low_qword(m_value)), "r" (desired)
            : "memory"
        );
        std::atomic_thread_fence(order);
    }

    inline void StoreUpper(uint64_t desired, 
        std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        if (!s_supported.test()) [[unlikely]] {
            T expected = Load(std::memory_order_relaxed);
            QWords newval;
            do{
                newval = std::bit_cast<QWords>(expected);
                newval.m_upper = desired;
            }while(!m_fallback.compare_exchange_weak(expected, std::bit_cast<T>(newval),
                order, std::memory_order_relaxed));
            return;
        }

        asm volatile(
            "movq %1, (%0)\n"
            : /* none */
            : "r" (high_qword(m_value)), "r" (desired)
            : "memory"
        );
        std::atomic_thread_fence(order);
    }

    inline uint64_t FetchAddLower(int64_t delta,
        std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        if (!s_supported.test()) [[unlikely]] {
            T expected = Load(std::memory_order_relaxed);
            QWords newval;
            do{
                newval = std::bit_cast<QWords>(expected);
                newval.m_lower += delta;
            }while(!m_fallback.compare_exchange_weak(expected, std::bit_cast<T>(newval),
                order, std::memory_order_relaxed));
            return std::bit_cast<QWords>(expected).m_lower;
        }

        asm volatile(
            "lock xadd %0, (%1)\n"
            : "+r" (delta)
            : "r" (low_qword(m_value))
            : "memory"
        );
        std::atomic_thread_fence(order);
        return delta;
    }

    inline uint64_t FetchAddUpper(int64_t delta,
        std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        if (!s_supported.test()) [[unlikely]] {
            T expected = Load(std::memory_order_relaxed);
            T newval;
            do{
                newval = std::bit_cast<QWords>(expected);
                newval.m_upper += delta;
            }while(!m_fallback.compare_exchange_weak(expected, std::bit_cast<T>(newval),
                order, std::memory_order_relaxed));
            return std::bit_cast<QWords>(expected).m_upper;
        }

        asm volatile(
            "lock xadd %0, (%1)\n"
            : "+r" (delta)
            : "r" (high_qword(m_value))
            : "memory"
        );
        std::atomic_thread_fence(order);
        return delta;
    }

    inline T FetchAdd(int64_t lower_delta, int64_t upper_delta,
        std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        if (!s_supported.test()) [[unlikely]] {
            T expected = Load(std::memory_order_relaxed);
            QWords newval;
            do{
                newval = std::bit_cast<QWords>(expected);
                newval.m_lower += lower_delta;
                newval.m_upper += upper_delta;
            }while(!m_fallback.compare_exchange_weak(expected, std::bit_cast<T>(newval),
                order, std::memory_order_relaxed));
            return expected;
        }

        T expected = Load(std::memory_order_relaxed);
        QWords newval;
        do{
            newval = std::bit_cast<QWords>(expected);
            newval.m_lower += lower_delta;
            newval.m_upper += upper_delta;
        }while(!CompareExchange(expected, std::bit_cast<T>(newval),
            order, std::memory_order_relaxed));
        return expected;
    }

    inline T FetchAdd(int64_t lower_low_delta, uint32_t lower_high_delta,
        int32_t upper_low_delta, int32_t upper_high_delta,
        std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        if (!s_supported.test()) [[unlikely]] {
            T expected = Load(std::memory_order_relaxed);
            DWords newval;
            do{
                newval = std::bit_cast<DWords>(expected);
                newval.m_lower_low += lower_low_delta;
                newval.m_lower_high += lower_high_delta;
                newval.m_upper_low += upper_low_delta;
                newval.m_upper_high += upper_high_delta;
            }while(!m_fallback.compare_exchange_weak(expected, std::bit_cast<T>(newval),
                order, std::memory_order_relaxed));
            return expected;
        }

        T expected = Load(std::memory_order_relaxed);
        DWords newval;
        do{
            newval = std::bit_cast<DWords>(expected);
            newval.m_lower_low += lower_low_delta;
            newval.m_lower_high += lower_high_delta;
            newval.m_upper_low += upper_low_delta;
            newval.m_upper_high += upper_high_delta;
        }while(!CompareExchange(expected, std::bit_cast<T>(newval),
            order, std::memory_order_relaxed));
        return expected;
    }

    inline T Exchange(T desired, std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        if (!s_supported.test()) [[unlikely]] {
            return m_fallback.exchange(desired, order);
        }

        T expected = Load(std::memory_order_relaxed);
        while(!CompareExchange(expected, desired, order, std::memory_order_relaxed));
        return expected;
    }

    inline T Load(std::memory_order order = std::memory_order_seq_cst) const noexcept
    {
        if (!s_supported.test(std::memory_order_relaxed)) [[unlikely]] {
            return m_fallback.load(order);
        }

        struct QWords ret;
        std::atomic_thread_fence(order);
        asm volatile(
            "movdqa (%2), %%xmm0\n\t"
            "movq %%xmm0, %0\n\t"
            "movhlps %%xmm0, %%xmm0\n\t"
            "movq %%xmm0, %1\n"
            : "=r" (ret.m_lower), "=r" (ret.m_upper)
            : "r" (&m_value)
            : "xmm0"
        );
        return std::bit_cast<T>(ret);
    }

    inline uint64_t LoadLower(std::memory_order order = std::memory_order_seq_cst) const noexcept
    {
        if (!s_supported.test(std::memory_order_relaxed)) [[unlikely]] {
            T value = m_fallback.load(order);
            return std::bit_cast<QWords>(value).m_lower;
        }

        uint64_t ret;
        std::atomic_thread_fence(order);
        asm volatile(
            "movq (%1), %0\n"
            : "=r" (ret)
            : "r" (low_qword(const_cast<T&>(m_value)))
        );
        return ret;
    }

    inline uint64_t LoadUpper(std::memory_order order = std::memory_order_seq_cst) const noexcept
    {
        if (!s_supported.test(std::memory_order_relaxed)) [[unlikely]] {
            T value = m_fallback.load(order);
            return std::bit_cast<QWords>(value).m_upper;
        }

        uint64_t ret;
        std::atomic_thread_fence(order);
        asm volatile(
            "movq (%1), %0\n"
            : "=r" (ret)
            : "r" (high_qword(const_cast<T&>(m_value)))
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

        QWords expected_qwords = std::bit_cast<QWords>(expected);
        QWords desired_qwords = std::bit_cast<QWords>(desired);

        /* The CMPXCH16B instruction will atomically load the existing 
         * value into RDX:RAX if the comparison fails. Add a fence to 
         * sequence the load.
         */
        std::atomic_thread_fence(failure);
        asm volatile(
            "lock cmpxchg16b %1\n"
            : "=@ccz" (result) , "+m" (m_value)
            , "+a" (expected_qwords.m_lower), "+d" (expected_qwords.m_upper)
            :  "b" (desired_qwords.m_lower),   "c" (desired_qwords.m_upper)
            : "cc"
        );
        expected = std::bit_cast<T>(expected_qwords);
        if(result) {
            /* We have successfully written the value to memory. Place
             * a fence to "publish" the new value. 
             */
            std::atomic_thread_fence(success);
        }
        return result;
    }
};

/*****************************************************************************/
/* BACKOFF                                                                   */
/*****************************************************************************/

export
class Backoff
{
private:

    using TimestampType = std::chrono::time_point<std::chrono::steady_clock>;
    using Microseconds = std::chrono::microseconds;

    /* Moot to go to a grandularity any lower than this... */
    static constexpr std::size_t s_min_sleep = 100;

    const std::size_t  m_poll_count;
    const Microseconds m_max_sleep;
    const Microseconds m_timeout;

    std::size_t        m_curr_polls;
    std::size_t        m_num_backoffs;
    TimestampType      m_start_time;

public:

    Backoff(std::size_t poll_count, std::size_t max_sleep, std::size_t timeout)
        : m_poll_count{poll_count}
        , m_max_sleep{max_sleep}
        , m_timeout{timeout}
        , m_curr_polls{0}
        , m_num_backoffs{0}
        , m_start_time{std::chrono::steady_clock::now()}
    {}

    void BackoffMaybe()
    {
        /* At first, optimistically do polling for m_poll_count retries. */
        if(m_curr_polls++ == m_poll_count) {
            m_curr_polls = 0;
            if(m_num_backoffs++ == 0) {
                std::this_thread::yield();
            }else{
                /* Truncated exponential binary backoff */
                std::size_t delay = s_min_sleep * std::exp2(m_num_backoffs - 1);
                delay = std::min(delay, static_cast<std::size_t>(m_max_sleep.count()));
                std::this_thread::sleep_for(Microseconds{delay});
            }
        }
    }

    bool TimedOut() const
    {
        auto now = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<Microseconds>(now - m_start_time);
        return (delta >= m_timeout);
    }

    void Reset()
    {
        m_curr_polls = 0;
        m_num_backoffs = 0;
        m_start_time = std::chrono::steady_clock::now();
    }
};

}; // namespace pe

