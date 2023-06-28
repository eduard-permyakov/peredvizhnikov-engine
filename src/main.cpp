import sync;
import engine;
import logger;
import SDL2;

import <cstdlib>;
import <exception>;
import <stdexcept>;

struct SDL2Context
{
    SDL2Context()
    {
        if(SDL_Init(SDL_INIT_VIDEO) < 0)
            throw std::runtime_error{"Failed to initialize SDL2."};
    }

    ~SDL2Context()
    {
        SDL_Quit();
    }
};

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        SDL2Context sdl;
        pe::Scheduler scheduler{};
        auto engine = pe::Engine::Create(scheduler, pe::Priority::eCritical,
            pe::CreateMode::eLaunchAsync, pe::Affinity::eAny);
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

