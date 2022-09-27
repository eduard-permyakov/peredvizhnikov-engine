import scheduler;
import task;

import <cstdlib>;
import <iostream>;
import <queue>;
import <mutex>;

std::mutex iolock{};

class Chatter : public pe::Task<int, Chatter>
{
public:

    using Task<int, Chatter>::Task;

    [[nodiscard]] virtual Chatter::handle_type Run()
    {
        std::unique_lock<std::mutex> lock{iolock};
        std::cout << "we here 1" << std::endl;
        lock.unlock();

        co_yield 69;

        lock.lock();
        std::cout << "we here 2" << std::endl;
        lock.unlock();

        co_yield 42;

        lock.lock();
        std::cout << "we here 3" << std::endl;
        lock.unlock();

        co_return 0;

        lock.lock();
        std::cout << "we here NEVER" << std::endl;
        lock.unlock();
    }
};

class Ponger : public pe::Task<int, Ponger>
{
public:

    using Task<int, Ponger>::Task;

    [[nodiscard]] virtual Ponger::handle_type Run()
    {
        for(int i = 0; i < 10; i++) {

            std::unique_lock<std::mutex> lock{iolock};
            std::cout << "Pong" << std::endl;
            lock.unlock();

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

            std::unique_lock<std::mutex> lock{iolock};
            std::cout << "Ping" << std::endl;
            lock.unlock();

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

        std::unique_lock<std::mutex> lock{iolock};
        std::cout << ret << std::endl;
        lock.unlock();

        ret = co_await chatter_task;

        lock.lock(); 
        std::cout << ret << std::endl;
        lock.unlock();

        ret = co_await chatter_task;

        lock.lock(); 
        std::cout << ret << std::endl;
        lock.unlock();

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

