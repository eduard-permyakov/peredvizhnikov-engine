import scheduler;
import task;

import <cstdlib>;
import <iostream>;
import <queue>;
import <mutex>;

static std::mutex iolock{};

template <typename... Args>
void println(Args... args)
{
    std::lock_guard<std::mutex> lock{iolock};
    (std::cout << ... << args) << std::endl;
}

class Chatter : public pe::Task<int, Chatter>
{
public:

    using Task<int, Chatter>::Task;

    [[nodiscard]] virtual Chatter::handle_type Run()
    {
        println("we here 1");
        co_yield 69;

        println("we here 2");
        co_yield 42;

        println("we here 3");
        co_return 0;
    }
};

class Ponger : public pe::Task<int, Ponger>
{
public:

    using Task<int, Ponger>::Task;

    [[nodiscard]] virtual Ponger::handle_type Run()
    {
        for(int i = 0; i < 10; i++) {

            println("Pong");
            co_yield 0;
        }
        co_return 0;
    }
};

class Pinger : public pe::Task<int, Pinger>
{
public:

    using Task<int, Pinger>::Task;

    [[nodiscard]] virtual Pinger::handle_type Run()
    {
        auto ponger = Ponger::Create(Scheduler(), 0);
        auto task = ponger->Run();

        for(int i = 0; i < 10; i++) {

            println("Ping");
            co_await task;
        }
        co_return 0;
    }
};

class Tester : public pe::Task<int, Tester>
{
public:

    using Task<int, Tester>::Task;

    [[nodiscard]] virtual Tester::handle_type Run()
    {
        static auto chatter = Chatter::Create(Scheduler(), 0);
        auto chatter_task = chatter->Run();

        int ret = co_await chatter_task;
        println(ret);

        ret = co_await chatter_task;
        println(ret);

        ret = co_await chatter_task;
        println(ret);

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

