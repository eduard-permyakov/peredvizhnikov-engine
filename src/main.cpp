import sync;
import engine;
import logger;
import event;
import SDL2;
import alloc;

import <new>;
import <cstdlib>;
import <exception>;
import <stdexcept>;
import <algorithm>;

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

void *operator new(std::size_t size) noexcept(false)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    return alloc.Allocate(size);
}

void *operator new[](std::size_t size) noexcept(false)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    return alloc.Allocate(size);
}

void *operator new(std::size_t size, std::align_val_t align) noexcept(false)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = std::max(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    return alloc.AllocateAligned(size, align);
}

void *operator new[](std::size_t size, std::align_val_t align) noexcept(false)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = std::max(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    return alloc.AllocateAligned(size, align);
}

void operator delete(void *ptr) noexcept
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.Free(ptr);
}

void operator delete[](void *ptr) noexcept
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.Free(ptr);
}

void operator delete(void* ptr, std::align_val_t align) noexcept
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.FreeAligned(ptr, align);
}

void operator delete[](void* ptr, std::align_val_t align) noexcept
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.FreeAligned(ptr, align);
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        SDL2Context sdl;
        pe::Scheduler scheduler{};
        auto engine = pe::Engine::Create(scheduler, pe::Priority::eCritical,
            pe::CreateMode::eLaunchAsync, pe::Affinity::eAny);
        scheduler.Run();

    }catch(pe::TaskException &e) {

        e.Print();
        ret = EXIT_FAILURE;

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

