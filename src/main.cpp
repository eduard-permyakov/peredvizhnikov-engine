import scheduler;
import task;

import <cstdlib>;
import <iostream>;

class TestTask : public pe::Task<int>
{
public:

    [[nodiscard]] virtual pe::TaskHandle<int> Run()
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
        scheduler.Run();

        auto task = TestTask{}.Run();
        task.Resume();
        std::cout << task.Value() << std::endl;
        task.Resume();
        std::cout << task.Value() << std::endl;
        task.Resume();
        std::cout << task.Value() << std::endl;
        task.Resume();

    }catch(std::exception &e){

        std::cerr << "Unhandled std::exception: " << e.what() << std::endl;
        ret = EXIT_FAILURE;

    }catch(...) {

        std::cerr << "Unknown unhandled exception." << std::endl;
        ret = EXIT_FAILURE;
    }

    return ret;
}

