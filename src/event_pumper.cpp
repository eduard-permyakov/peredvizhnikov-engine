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
    using Task<void, EventPumper, Barrier&>::Task;

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
            auto frame = co_await Event<EventType::eNewFrame>();
            pe::dbgprint("Event Pumper is running!", frame);
            pump_events();
            barrier.Arrive();
        }
    }
};

} // namespace pe

