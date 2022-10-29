export module event;

export import SDL2;

import <cstddef>;
import <any>;
import <iostream>;
import <optional>;

namespace pe{

export enum class EventType
{
    eNewFrame,
    eQuit,
    eDisplay,
    eWindow,
    eWindowManager,
    eNumEvents
};

export 
constexpr std::size_t kNumEvents = static_cast<std::size_t>(EventType::eNumEvents);

template <EventType Event>
requires (static_cast<std::size_t>(Event) < kNumEvents)
struct event_arg
{
};

template <> struct event_arg<EventType::eNewFrame>     { using type = std::monostate;   };
template <> struct event_arg<EventType::eQuit>         { using type = std::monostate;   };
template <> struct event_arg<EventType::eDisplay>      { using type = SDL_DisplayEvent; };
template <> struct event_arg<EventType::eWindow>       { using type = SDL_WindowEvent;  };
template <> struct event_arg<EventType::eWindowManager>{ using type = SDL_SysWMEvent;   };

export
template <EventType Event>
requires (static_cast<std::size_t>(Event) < kNumEvents)
using event_arg_t = typename event_arg<Event>::type;

template <template<EventType> class Awaitable, typename Sequence>
struct make_event_awaitable_ref_variant;

template <template<EventType> class Awaitable, std::size_t... Is> 
struct make_event_awaitable_ref_variant<Awaitable, std::index_sequence<Is...>>
{
    template <typename T>
    using optional_ref_type = std::optional<std::reference_wrapper<T>>;

    using type = std::variant<
        optional_ref_type<Awaitable<static_cast<EventType>(Is)>>...
    >;
};

export
template <template<EventType> class Awaitable>
using EventAwaitableRefVariant =
    typename make_event_awaitable_ref_variant<
        Awaitable, std::make_index_sequence<kNumEvents>>::type;

export
std::ostream& operator<<(std::ostream& out, EventType value)
{
    if(value == EventType::eNumEvents) [[unlikely]]
        throw std::range_error{"Invalid event type."};

    static const char *strings[kNumEvents] = {
        "New Frame",
        "Quit",
        "Display",
        "Window",
        "WindowManager",
    };
    static_assert(std::size(strings) == kNumEvents,
        "Unknown name for some event.");
    return (out << strings[static_cast<std::size_t>(value)]);
}

}; // namespace pe

