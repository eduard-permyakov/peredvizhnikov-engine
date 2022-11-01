module;
#if __has_include(<cxxabi.h>)
#include <cxxabi.h>
#endif
export module platform;

#ifdef __linux__
import execinfo;
#endif

import <cstdint>;
import <utility>;
import <string>;
import <vector>;
import <thread>;

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
static_assert(false, "Unsupported platform.");
#endif

#ifdef NDEBUG
export constexpr bool kDebug = false;
#else
export constexpr bool kDebug = true;
#endif

export constexpr bool kLinux = (kOS == OS::eLinux);
export constexpr bool kWindows = (kOS == OS::eWindows);

/*****************************************************************************/
/* LINUX                                                                     */
/*****************************************************************************/

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eLinux))
inline uint64_t rdtsc()
{
    unsigned int lo, hi;
    asm volatile(
        "mfence\n\t"
        "lfence\n\t"
        "rdtsc\n"
        : "=a" (lo), "=d" (hi)
    );
    return ((uint64_t)hi << 32) | lo;
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
    asm volatile(
        "cpuid\n"
        : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
        : "a" (0x0), "c" (0)
    );
    if(eax < 0x16)
        return 0;
    asm volatile(
        "cpuid\n"
        : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
        : "a" (0x16), "c" (0)
    );
    return eax;
}

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eLinux))
inline std::vector<std::string> Backtrace()
{
    void *callstack[64];
    int ncalls = backtrace(callstack, std::size(callstack));
    std::shared_ptr<char*[]> symbols{backtrace_symbols(callstack, ncalls), free};
    std::vector<std::string> ret{};
    ret.reserve(ncalls);
    for(int i = 0; i < ncalls; i++) {
        char *symbol = symbols[i];
        ret.emplace_back(symbol);
    }
    return ret;
}

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eLinux))
inline void SetThreadName(std::thread& thread, std::string_view name)
{
    auto handle = thread.native_handle();
    pthread_setname_np(handle, name.data());
}

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eLinux))
inline std::string GetThreadName()
{
    auto handle = pthread_self();
    char name[64];
    pthread_getname_np(handle, name, sizeof(name));
    return std::string{name};
}

/*****************************************************************************/
/* WINDOWS                                                                   */
/*****************************************************************************/

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eWindows))
inline uint64_t rdtsc()
{
    return 0;
}

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eWindows))
inline uint32_t tscfreq_mhz()
{
    return 0;
}

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eWindows))
inline std::vector<std::string> Backtrace()
{
    return {};
}

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eWindows))
inline void SetThreadName(std::thread& thread, std::string_view name)
{
}

export
template <int Platform = static_cast<int>(kOS)>
requires (Platform == static_cast<int>(OS::eWindows))
inline std::string GetThreadName()
{
    return "";
}

/*****************************************************************************/
/* COMMON                                                                    */
/*****************************************************************************/

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

export
template <typename T>
auto Demangle(T arg)
{
    return std::unique_ptr<char, void(*)(void*)>{nullptr, std::free};
}

#if __has_include(<cxxabi.h>)
export
template <>
auto Demangle<std::string>(std::string name)
{
    int status;
    return std::unique_ptr<char, void(*)(void*)>{
        abi::__cxa_demangle(name.c_str(), NULL, NULL, &status),
        std::free
    };
}
#endif

export
void AnnotateHappensBefore(const char *f, int l, void *addr)
{
#if defined(__SANITIZE_THREAD__) || __has_feature(thread_sanitizer)
	extern "C" void AnnotateHappensBefore(const char* f, int l, void* addr);
	::AnnotateHappensBefore(f, l, addr);
#endif
}

export
void AnnotateHappensAfter(const char *f, int l, void *addr)
{
#if defined(__SANITIZE_THREAD__) || __has_feature(thread_sanitizer)
	extern "C" void AnnotateHappensAfter(const char* f, int l, void* addr);
	::AnnotateHappensAfter(f, l, addr);
#endif
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

