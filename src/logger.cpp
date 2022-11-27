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
import <any>;
import <sstream>;

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

static constexpr TextColor s_level_color_map[static_cast<int>(LogLevel::eNumValues)] = {
    TextColor::eWhite,
    TextColor::eGreen,
    TextColor::eYellow,
    TextColor::eRed
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

std::ostream& operator<<(std::ostream& stream, std::monostate) 
{
    return (stream << "{EMPTY}");
}

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

export
namespace fmt{

template <Printable T>
struct hex
{
    const T& m_arg;

    hex(T&& arg)
        : m_arg{std::forward<T>(arg)}
    {}

    operator std::string() const
    {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }

    friend std::ostream& operator<<(std::ostream& os, const hex& hex)
    {
        std::ios old_state(nullptr);
        old_state.copyfmt(os);
        os << "0x";
        os << std::hex;
        os << hex.m_arg;
        os.copyfmt(old_state);
        return os;
    }
};

template <Printable T>
hex(T&& arg) -> hex<T>;

template <Printable T>
struct colored
{
    TextColor m_color;
    const T&  m_arg;

    colored(TextColor color, T&& arg)
        : m_color{color}
        , m_arg{std::forward<T>(arg)}
    {}

    friend std::ostream& operator<<(std::ostream& os, const colored& c)
    {
        return colortext(os, c.m_arg, c.m_color);
    }
};

template <Printable T>
colored(TextColor color, T&& arg) -> colored<T>;

/* Concatenates the preceding and following arg 
 * so no separator is printed between them.
 */
struct cat final
{
    friend std::ostream& operator<<(std::ostream& os, const cat& nts)
    {
        return os;
    }
};

enum class Justify
{
    eRight,
    eLeft
};

template <Printable T>
struct justified
{
    const T&    m_arg;
    Justify     m_type;
    std::size_t m_width;
    char        m_pad;

    justified(T&& arg, std::size_t width, Justify type = Justify::eRight, char pad = ' ')
        : m_arg{std::forward<T>(arg)}
        , m_type{type}
        , m_width{width}
        , m_pad{pad}
    {}

    friend std::ostream& operator<<(std::ostream& os, const justified& just)
    {
        std::ios old_state(nullptr);
        old_state.copyfmt(os);
        os << (just.m_type == Justify::eLeft ? std::left : std::right);
        os << std::setw(just.m_width);
        os << std::setfill(just.m_pad);
        os << just.m_arg;
        os.copyfmt(old_state);
        return os;
    }
};

template <Printable T, typename... Args>
justified(T&& arg, Args... args) -> justified<T>;

} // namespace fmt

inline std::atomic_int s_thread_idx{0};
inline auto next_idx = []() { return s_thread_idx++; };
static thread_local TextColor t_thread_color{
    next_idx() % static_cast<int>(TextColor::eNumValues)
};

export
template <typename... Args>
void log_ex(std::ostream& stream, std::mutex *mutex, TextColor color, 
    const char *separator, bool prefix, bool newline, Args... args)
{
    using namespace std::string_literals;
    auto now = std::chrono::high_resolution_clock::now();
    auto nanosec = now.time_since_epoch();
    auto lock = (mutex) ? std::unique_lock<std::mutex>(*mutex) 
                        : std::unique_lock<std::mutex>();

    if(prefix) {
        std::thread::id tid = std::this_thread::get_id();
        TextColor thread_color = t_thread_color;

        stream << "[";
        if constexpr (pe::kLinux) {
            char name[16], aligned[16];
            auto handle = pthread_self();
            pthread_getname_np(handle, name, sizeof(name));
            snprintf(aligned, sizeof(aligned), "%9s", name);
            colortext(stream, aligned, thread_color);
            stream << " ";
        }

        stream << fmt::colored{thread_color, std::string{fmt::hex{tid}}};
        stream << "]";

        stream << "[";
        stream << fmt::justified{nanosec.count(), 16, fmt::Justify::eRight, '0'};
        stream << "]";

        stream << " ";
    }

    bool prev_cat = false;
    bool first = true;

    auto getsep = [&prev_cat, &first, &separator](auto&& arg) mutable{
        bool is_cat = std::is_same_v<std::remove_cvref_t<decltype(arg)>, fmt::cat>;
        const char *ret = (first || is_cat || prev_cat) ? "" : separator;
        prev_cat = is_cat;
        first = false;
        return ret;
    };

    ((stream << getsep(args), colortext(stream, std::forward<Args>(args), color)), ...);

    if(newline)
        stream << std::endl;
}

export
template <typename... Args>
void log(std::ostream& stream, std::mutex *mutex, LogLevel level, Args... args)
{
    TextColor color = s_level_color_map[static_cast<int>(level)];
    log_ex(stream, mutex, color, " ", true, true, args...);
}

export
template <typename... Args>
void log(std::ostream& stream, std::mutex *mutex, TextColor color, Args... args)
{
    log_ex(stream, mutex, color, " ", true, true, args...);
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
void ioprint(TextColor color, Args... args)
{
    log_ex(std::cout, &iolock, color, " ", true, true, args...);
}

export
template <typename... Args>
void ioprint_unlocked(TextColor color, const char *separator, bool prefix,
    bool newline, Args... args)
{
    log_ex(std::cout, nullptr, color, separator, prefix, newline, args...);
}

export
template <typename... Args>
void dbgprint(Args... args)
{
    log(std::cout, &iolock, LogLevel::eInfo, args...);
    std::cout << std::flush;
}

} // namespace pe

