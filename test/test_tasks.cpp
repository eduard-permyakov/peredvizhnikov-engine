import sync;
import logger;
import event;

import <cstdlib>;
import <iostream>;
import <thread>;
import <chrono>;
import <variant>;


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
        co_yield pe::Void;
        pe::dbgprint("We are in the main thread");
        co_yield pe::Void;
        pe::dbgprint("We are in the main thread");
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
        auto arg = co_await Event<pe::EventType::eNewFrame>();
        pe::dbgprint("Received event:", arg);
        co_return;
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
        co_await main_affine->Join();

        pe::ioprint(pe::TextColor::eGreen, "Testing EventListener");
        auto event_listener = EventListener::Create(Scheduler(), 0);

        Broadcast<pe::EventType::eNewFrame>();
        co_await event_listener->Join();

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

