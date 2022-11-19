export module engine;

import sync;
import event;
import event_pumper;
import logger;

namespace pe{

export 
class Engine : public Task<void, Engine>
{
    using Task<void, Engine>::Task;

    uint64_t m_frame_idx;

    virtual Engine::handle_type Run()
    {
        Barrier barrier{Scheduler(), 2};
        // TODO: start event_pumper synchronously...
        // this way we are guaranteed that it Subscribes to the 
        // necessary events before we send them out...
        auto event_pumper = EventPumper::Create(Scheduler(), 0, 
            false, pe::Affinity::eMainThread, barrier);

        while(true) {
            pe::dbgprint("Engine is running!");
            Broadcast<EventType::eNewFrame>(m_frame_idx++);
            co_await barrier.ArriveAndWait();
        }
    }
};

} // namespace pe

