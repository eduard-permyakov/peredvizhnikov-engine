export module sync:system_tasks;

import :scheduler;
import logger;
import event;

import <stdexcept>;

namespace pe{

export
class QuitHandler : public Task<void, QuitHandler>
{
private:

    using base = Task<void, QuitHandler>;
    using base::base;

    virtual QuitHandler::handle_type Run()
    {
        Subscribe<EventType::eQuit>();

        while(true) {
            co_await Event<EventType::eQuit>();
            Scheduler().Shutdown();
        }
    }

public:

    QuitHandler(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity)
        : base{token, scheduler, priority, mode, affinity}
    {
        if(affinity != Affinity::eMainThread)
            throw std::invalid_argument{"Task must have main thread affinity."};
    }
};

} // namespace pe

