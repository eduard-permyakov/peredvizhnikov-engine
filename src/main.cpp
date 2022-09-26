import scheduler;
import task;

import <cstdlib>;
import <iostream>;
import <queue>;

class TestTask : public pe::Task<int>
{
public:

    using Task<int>::Task;

    [[nodiscard]] virtual TestTask::handle_type Run()
    {
        std::cout << "we here 1" << std::endl;
        co_yield 69;
        std::cout << "we here 2" << std::endl;
        co_yield 42;
        std::cout << "we here 3" << std::endl;
        co_return 0;
        std::cout << "we here NEVER" << std::endl;
    }
};

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::Scheduler scheduler{};
        auto t1 = TestTask{scheduler, 0}.Run();
        auto t2 = TestTask{scheduler, 1}.Run();
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

