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

    virtual EventPumper::handle_type Run(Barrier& barrier)
    {
        Subscribe<EventType::eNewFrame>();

        while(true) {
            pe::dbgprint("Event Pumper is running!");
            co_await Event<EventType::eNewFrame>();
            barrier.Arrive();
        }
    }
};

} // namespace pe

