/*
 *  This file is part of Peredvizhnikov Engine
 *  Copyright (C) 2023 Eduard Permyakov 
 *
 *  Peredvizhnikov Engine is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Peredvizhnikov Engine is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#include <SDL.h>

import sync;
import engine;
import logger;
import event;
import SDL2;
import alloc;
import unistd;

import <new>;
import <cstdlib>;
import <exception>;
import <stdexcept>;
import <algorithm>;

struct SDLContext
{
    SDLContext()
    {
        if(SDL_Init(SDL_INIT_VIDEO) < 0)
            throw std::runtime_error{"Failed to initialize SDL."};
    }

    ~SDLContext()
    {
        SDL_Quit();
    }
};

#if !(defined(__SANITIZE_ADDRESS__) || __has_feature(address_sanitizer))
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
    return alloc.AllocateAligned(size, align);
}

void *operator new[](std::size_t size, std::align_val_t align) noexcept(false)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
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

void *malloc(size_t size)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    return alloc.Allocate(size);
}

void *calloc(size_t num, size_t size)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(num * size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    auto ret = alloc.Allocate(size);
    std::memset(ret, 0, size);
    return ret;
}

void *realloc(void *ptr, size_t size)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    auto ret = alloc.Allocate(size);
    if(ptr) {
        std::size_t copy_size = std::min(size, alloc.AllocationSize(ptr));
        std::memcpy(ret, ptr, copy_size);
        alloc.Free(ptr);
    }
    return ret;
}

void *memalign(size_t alignment, size_t size)
{
    if(alignment > pe::kMaxBlockSize)
        return nullptr;
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, alignment);
    return alloc.Allocate(size);
}

int posix_memalign(void **memptr, size_t alignment, size_t size)
{
    if(size == 0) {
        *memptr = nullptr;
        return 0;
    }

    void *ret = memalign(alignment, size);
    if(!ret)
        return -ENOMEM;

    *memptr = ret;
    return 0;
}

void *aligned_alloc(size_t alignment, size_t size)
{
    return memalign(alignment, size);
}

void *valloc(size_t size)
{
    return memalign(getpagesize(), size);
}

void *pvalloc(size_t size)
{
    size_t page_size = getpagesize();
    size = std::ceil(size / ((float)page_size)) * page_size;
    return memalign(page_size, size);
}

size_t malloc_usable_size(void *ptr)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    return alloc.AllocationSize(ptr);
}

void free(void *ptr)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.Free(ptr);
}
#endif

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        SDLContext sdl;
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

