/*
 *  This file is part of Peredvizhnikov Engine
 *  Copyright (C) 2023 Eduard Permyakov 
 *
 *  Peredvizhnikov Engine is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Peredvizhnikov Engine is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

export module sync:system_tasks;

import :scheduler;
import logger;
import event;

import <stdexcept>;
import <optional>;

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
            co_return;
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

export
class ExceptionForwarder : public Task<void, ExceptionForwarder>
{
private:

    using base = Task<void, ExceptionForwarder>;
    using base::base;

    virtual ExceptionForwarder::handle_type Run()
    {
        Subscribe<EventType::eUnhandledTaskException>();

        while(true) {
            auto exc = co_await Event<EventType::eUnhandledTaskException>();
            Scheduler().Shutdown(exc);
            co_return;
        }
    }

public:

    ExceptionForwarder(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity)
        : base{token, scheduler, priority, mode, affinity}
    {
        if(affinity != Affinity::eMainThread)
            throw std::invalid_argument{"Task must have main thread affinity."};
    }
};

} // namespace pe

