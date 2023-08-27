export module event;
export import SDL2;

import shared_ptr;
import logger;

import <cstddef>;
import <any>;
import <iostream>;
import <optional>;
import <vector>;

namespace pe{

export enum class EventType
{
    eUser,
    eNewFrame,
    eQuit,
    eDisplay,
    eWindow,
    eWindowManager,
    eUnhandledTaskException,
    eNumEvents
};

export struct UserEvent
{
    uint64_t m_header;
    std::any m_payload;
};

export
class TaskException
{
private:

    std::string              m_name;
    uint32_t                 m_tid;
    std::vector<std::string> m_backtrace;
    std::exception_ptr       m_exception;

    const std::string unwrap(std::exception_ptr ptr) const;

public:

    TaskException(std::string name, uint32_t tid, 
        std::vector<std::string> backtrace, std::exception_ptr exc);
    void Print() const;
};

export 
constexpr std::size_t kNumEvents = static_cast<std::size_t>(EventType::eNumEvents);

/* event_arg_t
 */
template <EventType Event>
requires (static_cast<std::size_t>(Event) < kNumEvents)
struct event_arg
{};

template <> struct event_arg<EventType::eUser>                  { using type = UserEvent;        };
template <> struct event_arg<EventType::eNewFrame>              { using type = uint64_t;         };
template <> struct event_arg<EventType::eQuit>                  { using type = std::monostate;   };
template <> struct event_arg<EventType::eDisplay>               { using type = SDL_DisplayEvent; };
template <> struct event_arg<EventType::eWindow>                { using type = SDL_WindowEvent;  };
template <> struct event_arg<EventType::eWindowManager>         { using type = SDL_SysWMEvent;   };
template <> struct event_arg<EventType::eUnhandledTaskException>{ using type = TaskException;    };

export
template <EventType Event>
requires (static_cast<std::size_t>(Event) < kNumEvents)
using event_arg_t = typename event_arg<Event>::type;

/* event_variant_t:
 *     A single variant for every possible event_arg type
 */
template <typename Sequence>
struct make_event_variant;

template <std::size_t... Is>
struct make_event_variant<std::index_sequence<Is...>>
{
    using type = std::variant<
        event_arg_t<static_cast<EventType>(Is)>...
    >;
};

export 
using event_variant_t = make_event_variant<std::make_index_sequence<kNumEvents>>::type;

export 
template <EventType Event>
auto static_event_cast(event_variant_t variant)
{
    return std::get<static_cast<std::size_t>(Event)>(variant);
}

/* Event:
 *     Generic type-erased event descriptor.
 */

export
struct Event
{
    EventType       m_type;
    event_variant_t m_arg;
};

/* event_awaitable_variant_t:
 *     A variant capable of holding an instance of the provided 'Awaitable'
 *     template specialized for every possible event type.
 */
template <template<EventType> class Awaitable, typename Sequence>
struct make_event_awaitable_variant;

template <template<EventType> class Awaitable, std::size_t... Is> 
struct make_event_awaitable_variant<Awaitable, std::index_sequence<Is...>>
{
    using type = std::variant<
        pe::shared_ptr<Awaitable<static_cast<EventType>(Is)>>...
    >;
};

export
template <template<EventType> class Awaitable>
using event_awaitable_variant_t =
    typename make_event_awaitable_variant<
        Awaitable, std::make_index_sequence<kNumEvents>>::type;

/* Printing for all event types 
 */
export
std::ostream& operator<<(std::ostream& out, EventType value)
{
    if(value == EventType::eNumEvents) [[unlikely]]
        throw std::range_error{"Invalid event type."};

    static const char *strings[kNumEvents] = {
        "User",
        "New Frame",
        "Quit",
        "Display",
        "Window",
        "Window Manager",
        "Unhandled Task Exception",
    };
    static_assert(std::size(strings) == kNumEvents,
        "Unknown name for some event.");
    return (out << strings[static_cast<std::size_t>(value)]);
}

/* Task Exception
 */
const std::string TaskException::unwrap(std::exception_ptr ptr) const
{
    try{
        std::rethrow_exception(ptr);
    }catch (std::exception const& e) {
        return e.what();
    }catch (...) {
        return "Unknown exception.";
    }
}

TaskException::TaskException(std::string name, uint32_t tid, 
    std::vector<std::string> backtrace, std::exception_ptr exc)
    : m_name{name}
    , m_tid{tid}
    , m_backtrace{backtrace}
    , m_exception{exc}
{}

void TaskException::Print() const
{
    using namespace std::string_literals;
    pe::ioprint(LogLevel::eError, "Unhandled exception in task", m_name, 
        fmt::cat{}, "<"s + std::to_string(m_tid) + ">"s, fmt::cat{}, ":");
    pe::ioprint(LogLevel::eError, unwrap(m_exception));
    pe::ioprint(LogLevel::eError);
    for(auto& entry : m_backtrace) {
        pe::ioprint(LogLevel::eError, "    ", fmt::cat{}, entry);
    }
    pe::ioprint(LogLevel::eError);
}

}; // namespace pe

