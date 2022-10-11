import scheduler;
import logger;
import SDL2;

import <cstdlib>;
import <iostream>;


class Yielder : public pe::Task<int, Yielder>
{
public:

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
public:

    using Task<void, PongerMaster>::Task;

    virtual PongerMaster::handle_type Run()
    {
        constexpr int niters = 10;
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
public:

    using Task<void, PingerSlave>::Task;

    virtual PingerSlave::handle_type Run()
    {
        auto ponger = PongerMaster::Create(Scheduler(), 0, true)->Run();

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
public:

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
public:

    using Task<void, PingerMaster>::Task;

    virtual PingerMaster::handle_type Run()
    {
        auto ponger = PongerSlave::Create(Scheduler(), 0, true)->Run();

        constexpr int niters = 10;
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
public:

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
public:

    using Task<void, ExceptionThrower>::Task;

    virtual ExceptionThrower::handle_type Run()
    {
        throw std::runtime_error{"Oops"};
    }
};

class Tester : public pe::Task<void, Tester>
{
public:

    using Task<void, Tester>::Task;

    virtual Tester::handle_type Run()
    {
        pe::ioprint(pe::LogLevel::eWarning, "Testing Yielder");
        auto yielder = Yielder::Create(Scheduler(), 0)->Run();

        int ret = co_await yielder;
        pe::dbgprint(ret);

        ret = co_await yielder;
        pe::dbgprint(ret);

        ret = co_await yielder;
        pe::dbgprint(ret);

        /* The task is finished now, we can't await it anymore */
        try{
            ret = co_await yielder;
            pe::dbgprint(ret);
        }catch(std::exception& exc) {
            pe::dbgprint("Caught exception:", exc.what());
        }

        pe::ioprint(pe::LogLevel::eWarning, "Testing ExceptionThrower");
        auto thrower = ExceptionThrower::Create(Scheduler(), 0);
        try{
            co_await thrower->Run();
        }catch(std::exception& exc) {
            pe::dbgprint("Caught exception:", exc.what());
        }

        pe::ioprint(pe::LogLevel::eWarning, "Testing SlavePinger / MasterPonger");
        auto spinger = PingerSlave::Create(Scheduler(), 0);
        co_await spinger->Run();

        pe::ioprint(pe::LogLevel::eWarning, "Testing MasterPinger / SlavePonger");
        auto mpinger = PingerMaster::Create(Scheduler(), 0);
        co_await mpinger->Run();

        pe::ioprint(pe::LogLevel::eWarning, "Testing MainAffine");
        auto main_affine = MainAffine::Create(Scheduler(), 0, true, pe::Affinity::eMainThread)->Run();
        co_await main_affine;
        co_await main_affine;
        co_await main_affine;

        pe::ioprint(pe::LogLevel::eWarning, "Testing finished");
    }
};

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::Scheduler scheduler{};
        auto tester = Tester::Create(scheduler, 0)->Run();
        scheduler.Run();

    }catch(std::exception &e){

        std::cerr << "Unhandled std::exception: " << e.what() << std::endl;
        ret = EXIT_FAILURE;

    }catch(...){

        std::cerr << "Unknown unhandled exception." << std::endl;
        ret = EXIT_FAILURE;
    }

    return ret;
}

