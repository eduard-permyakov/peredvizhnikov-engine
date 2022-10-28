export module logger;

import platform;

import <ostream>;
import <iostream>;
import <mutex>;
import <thread>;
import <chrono>;
import <atomic>;
import <iomanip>;
import <unordered_map>;
import <optional>;

namespace pe{

export std::mutex iolock{};
export std::mutex errlock{};

export enum class LogLevel{
    eInfo,
    eNotice,
    eWarning,
    eError,
    eNumValues
};

export enum class TextColor{
    eWhite,
    eGreen,
    eYellow,
    eRed,
    eBlue,
    eMagenta,
    eCyan,
    eBrightGreen,
    eBrightYellow,
    eBrightRed,
    eBrightBlue,
    eBrightMagenta,
    eBrightCyan,
    eNumValues
};

struct ANSIEscapeCode{
    static constexpr char eWhite[]         = "\033[37m";
    static constexpr char eGreen[]         = "\033[32m";
    static constexpr char eYellow[]        = "\033[33m";
    static constexpr char eRed[]           = "\033[31m";
    static constexpr char eBlue[]          = "\033[34m";
    static constexpr char eMagenta[]       = "\033[35m";
    static constexpr char eCyan[]          = "\033[36m";
    static constexpr char eBrightGreen[]   = "\033[32;1m";
    static constexpr char eBrightYellow[]  = "\033[33;1m";
    static constexpr char eBrightRed[]     = "\033[31;1m";
    static constexpr char eBrightBlue[]    = "\033[34;1m";
    static constexpr char eBrightMagenta[] = "\033[35;1m";
    static constexpr char eBrightCyan[]    = "\033[36;1m";
    static constexpr char eReset[]         = "\033[0m";
};

template <typename T>
concept Printable = requires(T t) {
    { std::cout << t } -> std::same_as<std::ostream&>;
};

template <Printable T, typename Stream>
requires (std::derived_from<Stream, std::ostream>)
Stream& colortext(Stream& stream, T printable, TextColor color)
{
    if constexpr (pe::kLinux && std::derived_from<Stream, decltype(std::cout)>) {
        static constexpr const char *color_code_map[static_cast<int>(TextColor::eNumValues)] = {
            ANSIEscapeCode::eWhite,
            ANSIEscapeCode::eGreen,
            ANSIEscapeCode::eYellow,
            ANSIEscapeCode::eRed,
            ANSIEscapeCode::eBlue,
            ANSIEscapeCode::eMagenta,
            ANSIEscapeCode::eCyan,
            ANSIEscapeCode::eBrightGreen,
            ANSIEscapeCode::eBrightYellow,
            ANSIEscapeCode::eBrightRed,
            ANSIEscapeCode::eBrightBlue,
            ANSIEscapeCode::eBrightMagenta,
            ANSIEscapeCode::eBrightCyan,
        };
        stream << color_code_map[static_cast<int>(color)];
        stream << printable;
        stream << ANSIEscapeCode::eReset;
    }else{
        stream << printable;
    }
    return stream;
}

static std::atomic_int s_thread_idx{0};
static thread_local TextColor t_thread_color{
    s_thread_idx++ % static_cast<int>(TextColor::eNumValues)
};

export
template <typename... Args>
void log_ex(std::ostream& stream, std::mutex *mutex, LogLevel level, 
    const char *separator, bool prefix, bool newline, Args... args)
{
    auto lock = (mutex) ? std::unique_lock<std::mutex>(*mutex) 
                        : std::unique_lock<std::mutex>();

    if(prefix) {
        std::thread::id tid = std::this_thread::get_id();
        TextColor thread_color = t_thread_color;

        std::ios old_state(nullptr);
        old_state.copyfmt(stream);

        stream << "[";
        if constexpr (pe::kLinux) {
            char name[16], aligned[16];
            auto handle = pthread_self();
            pthread_getname_np(handle, name, sizeof(name));
            snprintf(aligned, sizeof(aligned), "%9s", name);
            colortext(stream, aligned, thread_color);
            stream << " ";
        }
        colortext(stream, "0x", thread_color);
        stream << std::hex;
        colortext(stream, tid, thread_color);
        stream << "]";
        stream << " ";

        std::cout.copyfmt(old_state);

        auto tp = std::chrono::system_clock::now();
        auto dp = std::chrono::floor<std::chrono::days>(tp);
        std::chrono::hh_mm_ss time{std::chrono::floor<std::chrono::milliseconds>(tp-dp)};

        char format[64];
        std::snprintf(format, sizeof(format), "[%02ld:%02ld:%02lld.%03lld]",
            time.hours().count(), 
            time.minutes().count(), 
            time.seconds().count(), 
            time.subseconds().count()
        );
        stream << format;
        stream << " ";
    }

    static constexpr TextColor level_color_map[static_cast<int>(LogLevel::eNumValues)] = {
        TextColor::eWhite,
        TextColor::eGreen,
        TextColor::eYellow,
        TextColor::eRed
    };

    const char *sep = "";
    TextColor color = level_color_map[static_cast<int>(level)];
    ((stream << sep, colortext(stream, std::forward<Args>(args), color), sep = separator), ...);

    if(newline)
        stream << std::endl;
}

export
template <typename... Args>
void log(std::ostream& stream, std::mutex *mutex, LogLevel level, Args... args)
{
    log_ex(stream, mutex, level, " ", true, true, args...);
}

export
template <typename... Args>
void ioprint(LogLevel level, Args... args)
{
    log(std::cout, &iolock, level, args...);
    std::cout << std::flush;
}

export
template <typename... Args>
void dbgprint(Args... args)
{
    log(std::cout, &iolock, LogLevel::eInfo, args...);
    std::cout << std::flush;
}

} // namespace pe

