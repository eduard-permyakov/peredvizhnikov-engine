import sync;
import logger;
import event;
import assert;

import <cstdlib>;
import <iostream>;
import <thread>;
import <chrono>;
import <variant>;


constexpr int kNumEventProducers = 10;
constexpr int kNumEventConsumers = 10;
constexpr int kNumEventsProduced = 1000;

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
        auto ponger = PongerMaster::Create(Scheduler(), 0, true);

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
        auto ponger = PongerSlave::Create(Scheduler(), 0, true);

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

class Sleeper : public pe::Task<void, Sleeper>
{
    using Task<void, Sleeper>::Task;

    virtual Sleeper::handle_type Run()
    {
        using namespace std::chrono_literals;
        pe::dbgprint("Starting sleeping");
        std::this_thread::sleep_for(1000ms);
        pe::dbgprint("Finished sleeping");
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
        co_return;
    }
};

class EventProducer : public pe::Task<void, EventProducer>
{
private:

    using base = Task<void, EventProducer>;

    uint32_t m_id;

    virtual EventProducer::handle_type Run()
    {
        uint32_t curr = 0;
        while(curr < kNumEventsProduced) {
            uint64_t qword = (static_cast<uint64_t>(m_id) << 32) | curr;
            Broadcast<pe::EventType::eNewFrame>(qword);
            co_await Yield(Affinity());
            curr++;
        }
        co_return;
    }

public:

    EventProducer(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        uint32_t priority, bool initially_suspended, pe::Affinity affinity, 
        uint32_t id)
        : base{token, scheduler, priority, initially_suspended, affinity}
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
        int num_received = 0;

        while(num_received < (kNumEventProducers * kNumEventsProduced)) {

            uint64_t event = co_await Event<pe::EventType::eNewFrame>();
            uint32_t id = static_cast<uint32_t>(event >> 32);
            uint32_t seq = static_cast<uint32_t>(event);

            pe::assert(counters[id] == seq, "Unexpected event sequence number!",
                __FILE__, __LINE__);

            counters[id]++;
            num_received++;
        }
    }
};

class Tester : public pe::Task<void, Tester>
{
    using Task<void, Tester>::Task;

    virtual Tester::handle_type Run()
    {
        pe::ioprint(pe::TextColor::eGreen, "Testing Yielder");
        auto yielder = Yielder::Create(Scheduler(), 0, true);

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
        auto thrower = ExceptionThrower::Create(Scheduler(), 0, true);
        try{
            co_await thrower;
        }catch(std::exception& exc) {
            pe::dbgprint("Caught exception:", exc.what());
        }

        pe::ioprint(pe::TextColor::eGreen, "Testing Sleeper");
        co_await Sleeper::Create(Scheduler(), 0, true);

        pe::ioprint(pe::TextColor::eGreen, "Testing SlavePinger / MasterPonger");
        auto spinger = PingerSlave::Create(Scheduler(), 0);
        co_await spinger;

        pe::ioprint(pe::TextColor::eGreen, "Testing MasterPinger / SlavePonger");
        auto mpinger = PingerMaster::Create(Scheduler(), 0);
        co_await mpinger;

        pe::ioprint(pe::TextColor::eGreen, "Testing MainAffine");
        auto main_affine = MainAffine::Create(Scheduler(), 0, true, pe::Affinity::eMainThread);
        co_await main_affine;

        pe::ioprint(pe::TextColor::eGreen, "Testing EventListener");
        auto event_listener = EventListener::Create(Scheduler(), 0);

        Broadcast<pe::EventType::eNewFrame>(69);
        co_await event_listener;

        pe::ioprint(pe::TextColor::eGreen, 
            "Starting Event Multiple Producer, Multiple Consumer testing.");

        std::vector<pe::shared_ptr<EventConsumer>> consumers;
        std::vector<pe::shared_ptr<EventProducer>> producers;

        for(int i = 0; i < kNumEventConsumers; i++) {
            consumers.push_back(EventConsumer::Create(Scheduler(), 0));
        }
        for(int i = 0; i < kNumEventProducers; i++) {
            producers.push_back(EventProducer::Create(Scheduler(), 0,
                false, pe::Affinity::eAny, static_cast<uint32_t>(i)));
        }
        for(int i = 0; i < kNumEventConsumers; i++) {
            co_await consumers[i];
        }
        for(int i = 0; i < kNumEventProducers; i++) {
            co_await producers[i];
        }

        pe::ioprint(pe::TextColor::eGreen, "Testing finished");
        co_return;
    }
};

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::Scheduler scheduler{};
        auto tester = Tester::Create(scheduler, 0);
        scheduler.Run();

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }

    return ret;
}

