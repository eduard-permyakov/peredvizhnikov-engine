import scheduler;
import logger;

import <cstdlib>;
import <iostream>;


class Yielder : public pe::Task<int, Yielder>
{
public:

    using Task<int, Yielder>::Task;

    [[nodiscard]] virtual Yielder::handle_type Run()
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

    [[nodiscard]] virtual PongerMaster::handle_type Run()
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

    [[nodiscard]] virtual PingerSlave::handle_type Run()
    {
        auto ponger = PongerMaster::Create(Scheduler(), 0, true);
        auto task = ponger->Run();

        int i = 0;
        while(!task.Done()) {

            pe::dbgprint(i++, "Ping");
            co_await task;
        }
        co_return;
    }
};

class PongerSlave : public pe::Task<void, PongerSlave>
{
public:

    using Task<void, PongerSlave>::Task;

    [[nodiscard]] virtual PongerSlave::handle_type Run()
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

    [[nodiscard]] virtual PingerMaster::handle_type Run()
    {
        auto ponger = PongerSlave::Create(Scheduler(), 0, true);
        auto task = ponger->Run();

        constexpr int niters = 10;
        for(int i = 0; i < niters; i++) {
            pe::dbgprint(i, "Ping");
            if(i == niters-1)
                co_await task.Terminate(Scheduler());
            else
                co_await task;
        }
    }
};

class Tester : public pe::Task<void, Tester>
{
public:

    using Task<void, Tester>::Task;

    [[nodiscard]] virtual Tester::handle_type Run()
    {
        pe::ioprint(pe::LogLevel::eWarning, "Testing Yielder");
        static auto yielder = Yielder::Create(Scheduler(), 0);
        auto yielder_task = yielder->Run();

        int ret = co_await yielder_task;
        pe::dbgprint(ret);

        ret = co_await yielder_task;
        pe::dbgprint(ret);

        ret = co_await yielder_task;
        pe::dbgprint(ret);

        /* The task is finished now, we can't await it anymore */
        try{
            ret = co_await yielder_task;
            pe::dbgprint(ret);
        }catch(std::exception& exc) {
            pe::dbgprint("Caught exception:", exc.what());
        }

        pe::ioprint(pe::LogLevel::eWarning, "Testing SlavePinger / MasterPonger");
        auto ipinger = PingerSlave::Create(Scheduler(), 0);
        co_await ipinger->Run();

        pe::ioprint(pe::LogLevel::eWarning, "Testing MasterPinger / SlavePonger");
        auto fpinger = PingerMaster::Create(Scheduler(), 0);
        co_await fpinger->Run();

        pe::ioprint(pe::LogLevel::eWarning, "Testing finished");
    }
};

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::Scheduler scheduler{};
        auto tester = Tester::Create(scheduler, 0);
        auto task = tester->Run();
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

