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
        auto event_pumper = EventPumper::Create(Scheduler(), Priority::eNormal, 
            CreateMode::eLaunchSync, Affinity::eMainThread, barrier);

        while(true) {
            Broadcast<EventType::eNewFrame>(m_frame_idx++);
            pe::dbgprint("Engine is running!");
            co_await barrier.ArriveAndWait();
        }
    }
};

} // namespace pe

