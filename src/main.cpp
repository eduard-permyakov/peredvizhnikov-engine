import scheduler;
import logger;

import <cstdlib>;
import <iostream>;
import <queue>;
import <mutex>;


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

class Ponger : public pe::Task<void, Ponger>
{
public:

    using Task<void, Ponger>::Task;

    [[nodiscard]] virtual Ponger::handle_type Run()
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

class Pinger : public pe::Task<void, Pinger>
{
public:

    using Task<void, Pinger>::Task;

    [[nodiscard]] virtual Pinger::handle_type Run()
    {
        auto ponger = Ponger::Create(Scheduler(), 0);
        auto task = ponger->Run();

        int i = 0;
        while(!task.Done()) {

            pe::dbgprint(i++, "Ping");
            co_await task;
        }
        co_return;
    }
};

class Tester : public pe::Task<int, Tester>
{
public:

    using Task<int, Tester>::Task;

    [[nodiscard]] virtual Tester::handle_type Run()
    {
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

        auto pinger = Pinger::Create(Scheduler(), 0);
        co_await pinger->Run();

        co_return 0;
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

