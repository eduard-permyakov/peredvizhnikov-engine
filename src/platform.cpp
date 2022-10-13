export module platform;

import <cstdint>;
import <utility>;

namespace pe{

enum class OS
{
    eLinux,
    eWindows,
};

#ifdef __linux__
export constexpr OS kOS = OS::eLinux;
#elif _WIN32
export constexpr OS kOS = OS::eWindows;
#else
#error "Unsupported Platform"
#endif

#ifdef NDEBUG
export constexpr bool kDebug = false;
#else
export constexpr bool kDebug = true;
#endif

export constexpr bool kLinux = (kOS == OS::eLinux);
export constexpr bool kWindows = (kOS == OS::eWindows);

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eLinux))
inline uint64_t rdtsc()
{
    unsigned int lo, hi;
    __asm__ __volatile__(
        "mfence\n"
        "lfence\n"
        "rdtsc" : "=a" (lo), "=d" (hi)
    );
    return ((uint64_t)hi << 32) | lo;
}

export
template <int Platform = static_cast<int>(kOS)>
inline uint64_t rdtsc()
{
    return 0;
}

/* This isn't "precise" for a number of reasons, but sufficient 
 * for some approximations. 
 */
export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eLinux))
inline uint32_t tscfreq_mhz()
{
    uint32_t eax, ebx, ecx, edx;
    __asm__ __volatile__(
        "cpuid" : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx) 
            : "a" (0x0), "c" (0)
    );
    if(eax < 0x16)
        return 0;
    __asm__ __volatile__(
        "cpuid" : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx) 
            : "a" (0x16), "c" (0)
    );
    return eax;
}

export
template <int Platform = static_cast<int>(kOS)>
inline uint32_t tscfreq_mhz()
{
    return 0;
}

export
inline uint32_t rdtsc_usec(uint64_t delta)
{
    uint32_t freq_mhz = tscfreq_mhz();
    if(freq_mhz == 0) [[unlikely]] {
        return 0;
    }
    return ((float)delta) / freq_mhz;
}

export
template <typename Op, typename PostOp>
inline void dbgtime(Op&& op, PostOp&& post)
{
    if constexpr (kDebug) {
        uint64_t before = rdtsc();
        std::forward<Op&&>(op)();
        uint64_t after = rdtsc();
        uint64_t delta = after - before;
        std::forward<PostOp&&>(post)(delta);
    }else{
        std::forward<Op&&>(op)();
    }
}

}; // namespace pe

/* 
 * Appears to give false positives any time an exception
 * is thrown within a coroutine context, including the
 * most elementary examples. Valgrind doesn't have any
 * complaints.
 */
extern "C" const char *__asan_default_options() 
{
  return "alloc_dealloc_mismatch=0";
}

