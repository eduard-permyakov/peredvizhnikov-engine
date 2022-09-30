export module logger;

import <ostream>;
import <iostream>;
import <mutex>;
import <thread>;
import <chrono>;
import <atomic>;

namespace pe{

export std::mutex iolock{};
export std::mutex errlock{};

export enum class LogLevel{
    eInfo,
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
    if constexpr (__linux__ && std::derived_from<Stream, decltype(std::cout)>) {
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

export
template <typename... Args>
void log(std::ostream& stream, std::mutex& mutex, LogLevel level, Args... args)
{
    std::lock_guard<std::mutex> lock{mutex};
    std::thread::id tid = std::this_thread::get_id();

    std::ios old_state(nullptr);
    old_state.copyfmt(stream);

    static std::atomic_int thread_color_idx{0};
    thread_local TextColor thread_color{
        static_cast<TextColor>(thread_color_idx++ % static_cast<int>(TextColor::eNumValues))
    };

    stream << "[";
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

    static constexpr TextColor level_color_map[static_cast<int>(LogLevel::eNumValues)] = {
        TextColor::eWhite,
        TextColor::eYellow,
        TextColor::eRed
    };

    const char *sep = "";
    TextColor color = level_color_map[static_cast<int>(level)];
    ((stream << sep, colortext(stream, std::forward<Args>(args), color), sep = " "), ...);
    stream << std::endl;
}

export
template <typename... Args>
void ioprint(LogLevel level, Args... args)
{
    log(std::cout, iolock, level, args...);
    std::cout << std::flush;
}

export
template <typename... Args>
void dbgprint(Args... args)
{
    log(std::cout, iolock, LogLevel::eInfo, args...);
    std::cout << std::flush;
}

} // namespace pe

