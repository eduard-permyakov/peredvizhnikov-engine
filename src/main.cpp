import sync;
import logger;

import <cstdlib>;
import <exception>;

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::Scheduler scheduler{};
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

