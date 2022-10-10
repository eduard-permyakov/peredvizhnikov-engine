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

