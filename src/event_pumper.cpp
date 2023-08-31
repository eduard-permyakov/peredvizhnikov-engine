export module event_pumper;

import sync;
import event;
import logger;

import <thread>;
import <chrono>;

namespace pe{

export 
class EventPumper : public Task<void, EventPumper, Barrier&>
{
    using base = Task<void, EventPumper, Barrier&>;
    using base::base;

    void pump_events()
    {
        SDL_Event e;
        while(SDL_PollEvent(&e)) {
            switch(e.type) {
            case SDL_QUIT:
                Broadcast<EventType::eQuit>();
                break;
            case SDL_DISPLAYEVENT:
                Broadcast<EventType::eDisplay>(e.display);
                break;
            case SDL_WINDOWEVENT:
                Broadcast<EventType::eWindow>(e.window);
                break;
            case SDL_SYSWMEVENT:
                Broadcast<EventType::eWindowManager>(e.syswm);
                break;
            default:
                break;
            }
        }
    }

    virtual EventPumper::handle_type Run(Barrier& barrier)
    {
        Subscribe<EventType::eNewFrame>();

        while(true) {
            co_await Event<EventType::eNewFrame>();
            pump_events();
            barrier.Arrive();
        }
    }

public:

    EventPumper(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity)
        : base{token, scheduler, priority, mode, affinity}
    {
        if(affinity != Affinity::eMainThread)
            throw std::invalid_argument{"Task must have main thread affinity."};
    }
};

} // namespace pe

