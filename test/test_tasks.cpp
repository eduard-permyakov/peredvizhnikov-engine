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

import sync;
import logger;
import event;
import assert;

import <cstdlib>;
import <iostream>;
import <thread>;
import <chrono>;
import <variant>;
import <any>;
import <vector>;


constexpr int kNumEventProducers = 10;
constexpr int kNumEventConsumers = 10;
constexpr int kNumEventsProduced = 100;
constexpr int kNumMessagesSend = 100;
constexpr int kNumMessageSendReceivePairs = 10;

class Yielder : public pe::Task<int, Yielder>
{
    using Task<int, Yielder>::Task;

    virtual Yielder::handle_type Run()
    {
        pe::dbgprint("Yielding 69");
        co_yield 69;

        pe::dbgprint("Yielding 42");
        co_yield 42;

        pe::dbgprint("Returning 0");
        co_return 0;
    }
};

class PongerMaster : public pe::Task<void, PongerMaster>
{
    using Task<void, PongerMaster>::Task;

    virtual PongerMaster::handle_type Run()
    {
        constexpr int niters = 5;
        for(int i = 0; i < niters; i++) {

            pe::dbgprint(i, "Pong");

            if(i < niters-1)
                co_yield pe::Void;
            else
                co_return;
        }
    }
};

class PingerSlave : public pe::Task<void, PingerSlave>
{
    using Task<void, PingerSlave>::Task;

    virtual PingerSlave::handle_type Run()
    {
        auto ponger = PongerMaster::Create(Scheduler(), pe::Priority::eNormal,
            pe::CreateMode::eSuspend);

        int i = 0;
        while(!ponger->Done()) {

            pe::dbgprint(i++, "Ping");
            co_await ponger;
        }
        co_return;
    }
};

class PongerSlave : public pe::Task<void, PongerSlave>
{
    using Task<void, PongerSlave>::Task;

    virtual PongerSlave::handle_type Run()
    {
        int i = 0;
        while(true) {
            pe::dbgprint(i++, "Pong");
            co_yield pe::Void;
        }
    }
};

class PingerMaster : public pe::Task<void, PingerMaster>
{
    using Task<void, PingerMaster>::Task;

    virtual PingerMaster::handle_type Run()
    {
        auto ponger = PongerSlave::Create(Scheduler(), pe::Priority::eNormal,
            pe::CreateMode::eSuspend);

        constexpr int niters = 5;
        for(int i = 0; i < niters; i++) {
            pe::dbgprint(i, "Ping");
            if(i == niters-1)
                co_await ponger->Terminate();
            else
                co_await ponger;
        }
    }
};

class MainAffine : public pe::Task<void, MainAffine>
{
    using Task<void, MainAffine>::Task;

    virtual MainAffine::handle_type Run()
    {
        pe::dbgprint("We are in the main thread");
        co_await Yield(Affinity());
        pe::dbgprint("We are in the main thread");
        co_await Yield(Affinity());
        pe::dbgprint("We are in the main thread");
        co_return;
    }
};

class ExceptionThrower : public pe::Task<void, ExceptionThrower>
{
    using Task<void, ExceptionThrower>::Task;

    virtual ExceptionThrower::handle_type Run()
    {
        throw std::runtime_error{"Oops"};
        co_return;
    }
};

class EventListener : public pe::Task<void, EventListener>
{
    using Task<void, EventListener>::Task;

    virtual EventListener::handle_type Run()
    {
        Subscribe<pe::EventType::eNewFrame>();
        auto arg = co_await Event<pe::EventType::eNewFrame>();
        pe::dbgprint("Received event:", arg);
        Unsubscribe<pe::EventType::eNewFrame>();
        co_return;
    }
};

class EventProducer : public pe::Task<void, EventProducer>
{
private:

    using base = Task<void, EventProducer>;
    using base::base;

    uint32_t m_id;

    virtual EventProducer::handle_type Run()
    {
        uint32_t curr = 1;
        while(curr <= kNumEventsProduced) {
            uint64_t qword = (static_cast<uint64_t>(m_id) << 32) | curr;
            Broadcast<pe::EventType::eNewFrame>(qword);
            co_await Yield(Affinity());
            curr++;
        }
        co_return;
    }

public:

    EventProducer(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity, 
        uint32_t id)
        : base{token, scheduler, priority, mode, affinity}
        , m_id{id}
    {}
};

class EventConsumer : public pe::Task<void, EventConsumer>
{
    using Task<void, EventConsumer>::Task;

    virtual EventConsumer::handle_type Run()
    {
        Subscribe<pe::EventType::eNewFrame>();

        uint64_t counters[kNumEventProducers] = {};
        std::fill(std::begin(counters), std::end(counters), 1);
        int num_received = 0;

        while(num_received < (kNumEventProducers * kNumEventsProduced)) {

            uint64_t event = co_await Event<pe::EventType::eNewFrame>();
            uint32_t id = static_cast<uint32_t>(event >> 32);
            uint32_t seq = static_cast<uint32_t>(event);

            pe::assert(counters[id] == seq, "Unexpected event sequence number!");

            counters[id]++;
            num_received++;
        }
    }
};

class Receiver : public pe::Task<void, Receiver>
{
    using Task<void, Receiver>::Task;

    virtual Receiver::handle_type Run()
    {
        std::size_t received = 0;
        while(received++ < kNumMessagesSend) {
            auto msg = co_await Receive();
			pe::assert(any_cast<std::string>(msg.m_payload) == "Hello World!");
            Reply(msg.m_sender.lock(), pe::Message{
                this->shared_from_this(), 0, std::string{"Hey, I'm not the World!"}});
        }
    }
};

class Sender : public pe::Task<void, Sender>
{
    using base = Task<void, Sender>;
    using base::base;

    pe::weak_ptr<Receiver> m_receiver;

    virtual Sender::handle_type Run()
    {
        for(int i = 0; i < kNumMessagesSend; i++) {
            auto response = co_await Send(m_receiver.lock(), 
                pe::Message{this->shared_from_this(), 0, std::string{"Hello World!"}});
			pe::assert(any_cast<std::string>(response.m_payload) == "Hey, I'm not the World!");
        }
    }

public:

    Sender(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity, 
        pe::shared_ptr<Receiver> receiver)
        : base{token, scheduler, priority, mode, affinity}
        , m_receiver{receiver}
    {}
};

class Tester : public pe::Task<void, Tester>
{
    using Task<void, Tester>::Task;

    virtual Tester::handle_type Run()
    {
        pe::ioprint(pe::TextColor::eGreen, "Testing Yielder");
        auto yielder = Yielder::Create(Scheduler(), pe::Priority::eNormal,
            pe::CreateMode::eSuspend);

        int ret = co_await yielder;
        pe::dbgprint(ret);

        ret = co_await yielder;
        pe::dbgprint(ret);

        ret = co_await yielder;
        pe::dbgprint(ret);

        /* The task is joined now, we can't await it anymore */
        try{
            ret = co_await yielder;
            pe::dbgprint(ret);
        }catch(std::exception& exc) {
            pe::dbgprint("Caught exception:", exc.what());
        }

        pe::ioprint(pe::TextColor::eGreen, "Testing ExceptionThrower");
        auto thrower = ExceptionThrower::Create(Scheduler(), pe::Priority::eNormal,
            pe::CreateMode::eSuspend);
        try{
            co_await thrower;
        }catch(std::exception& exc) {
            pe::dbgprint("Caught exception:", exc.what());
        }

        pe::ioprint(pe::TextColor::eGreen, "Testing SlavePinger / MasterPonger");
        auto spinger = PingerSlave::Create(Scheduler());
        co_await spinger;

        pe::ioprint(pe::TextColor::eGreen, "Testing MasterPinger / SlavePonger");
        auto mpinger = PingerMaster::Create(Scheduler());
        co_await mpinger;

        pe::ioprint(pe::TextColor::eGreen, "Testing MainAffine");
        auto main_affine = MainAffine::Create(Scheduler(), pe::Priority::eNormal,
            pe::CreateMode::eSuspend, pe::Affinity::eMainThread);
        co_await main_affine;

        pe::ioprint(pe::TextColor::eGreen, "Testing EventListener");
        auto event_listener = EventListener::Create(Scheduler(), pe::Priority::eNormal,
            pe::CreateMode::eLaunchSync, pe::Affinity::eAny);

        Broadcast<pe::EventType::eNewFrame>(69);
        co_await event_listener;

        pe::ioprint(pe::TextColor::eGreen, 
            "Starting Event Multiple Producer, Multiple Consumer testing.");

        std::vector<pe::shared_ptr<EventConsumer>> consumers;
        std::vector<pe::shared_ptr<EventProducer>> producers;

        for(int i = 0; i < kNumEventConsumers; i++) {
            consumers.push_back(EventConsumer::Create(Scheduler()));
        }
        for(int i = 0; i < kNumEventProducers; i++) {
            producers.push_back(EventProducer::Create(Scheduler(), pe::Priority::eNormal,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, static_cast<uint32_t>(i)));
        }
        for(int i = 0; i < kNumEventConsumers; i++) {
            co_await consumers[i];
        }
        for(int i = 0; i < kNumEventProducers; i++) {
            co_await producers[i];
        }

        pe::ioprint(pe::TextColor::eGreen, "Starting Message Passing testing...");

        std::vector<pe::shared_ptr<Sender>> senders;
        std::vector<pe::shared_ptr<Receiver>> receivers;

        for(int i = 0; i < kNumMessageSendReceivePairs; i++) {
            auto receiver = Receiver::Create(Scheduler());
            auto sender = Sender::Create(Scheduler(), pe::Priority::eNormal,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, receiver);

            receivers.push_back(receiver);
            senders.push_back(sender);
        }

        for(int i = 0; i < kNumMessageSendReceivePairs; i++) {
            co_await receivers[i];
            co_await senders[i];
        }

        pe::ioprint(pe::TextColor::eGreen, "Testing finished");
        Broadcast<pe::EventType::eQuit>();
        co_return;
    }
};

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::Scheduler scheduler{};
        auto tester = Tester::Create(scheduler);
        scheduler.Run();

    }catch(pe::TaskException &e) {

        e.Print();
        ret = EXIT_FAILURE;

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

