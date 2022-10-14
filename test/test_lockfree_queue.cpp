import logger;

import <cstdlib>;
import <iostream>;

int main()
{
    int ret = EXIT_SUCCESS;

    try{

		pe::dbgprint("Testing lockfree queue...");

    }catch(std::exception &e){

        std::cerr << "Unhandled std::exception: " << e.what() << std::endl;
        ret = EXIT_FAILURE;

    }catch(...){

        std::cerr << "Unknown unhandled exception." << std::endl;
        ret = EXIT_FAILURE;
    }

    return ret;
}

