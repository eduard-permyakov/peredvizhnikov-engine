import scheduler;
import task;

import <cstdlib>;
import <iostream>;
import <queue>;

class Chatter : public pe::Task<int>
{
public:

    using Task<int>::Task;

    [[nodiscard]] virtual Chatter::handle_type Run()
    {
        std::cout << "we here 1\n";
        co_yield 69;
        std::cout << "we here 2\n";
        co_yield 42;
        std::cout << "we here 3\n";
        co_return 0;
        std::cout << "we here NEVER\n";
    }
};

// TODO: Add 'void' specialization to task types
// so we can just have pe::Task<> without being
// forced to add a return type

class Ponger : public pe::Task<int>
{
public:

    using Task<int>::Task;

    [[nodiscard]] virtual Ponger::handle_type Run()
    {
        for(int i = 0; i < 10; i++) {
            std::cout << "Pong\n";
            // TODO: define what happens when we yield a value but
            // there is no consumer of the value. In theory, the task
            // should be blocked until someone consumes the yielded
            // value
            co_yield 0;
        }
        co_return 0;
    }
};

class Pinger : public pe::Task<int>
{
public:

    using Task<int>::Task;

    [[nodiscard]] virtual Pinger::handle_type Run()
    {
        auto ponger = Ponger{Scheduler(), 0}.Run();
        for(int i = 0; i < 10; i++) {
            std::cout << "Ping\n";
            // TODO: define what should happen when we co_await 
            // a task that has already ran to completion
            co_await ponger;
        }
        co_return 0;
    }
};

class Tester : public pe::Task<int>
{
public:

    using Task<int>::Task;

    [[nodiscard]] virtual Tester::handle_type Run()
    {
        auto c1 = Chatter{Scheduler(), 0}.Run();
        auto c2 = Chatter{Scheduler(), 1}.Run();

        // This waits only on the very first suspension of 
        // the coroutine, However, we need a mechanism to wait 
        // until the awaited task exits
        // ex. co_await c1.Join()
        co_await c1;
        co_await c2;

        co_await Pinger{Scheduler(), 0}.Run();
        co_return 0;
    }
};

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::Scheduler scheduler{};
        auto task = Tester{scheduler, 0}.Run();
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

