export module platform;

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

