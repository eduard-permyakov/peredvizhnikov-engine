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

import logger;
import assert;
import alloc;
import lockfree_queue;

import <cstdlib>;
import <vector>;
import <future>;
import <optional>;
import <array>;
import <random>;


constexpr int kNumDescriptors = 1024;
constexpr int kNumAllocators = 8;
constexpr int kNumDeallocators = 8;
constexpr int kNumBlocks = 32768;
constexpr int kNumAllocations = 65536;

using OptionalDescriptor = std::optional<std::reference_wrapper<pe::SuperblockDescriptor>>;

void descriptor_allocator(
    pe::DescriptorFreelist& list,
    std::vector<OptionalDescriptor> &descs,
    std::size_t offset)
{
    const std::size_t nalloc = kNumDescriptors / kNumAllocators;
    for(int i = 0; i < nalloc; i++) {
        descs[offset + i] = list.Allocate();
    }
}

void descriptor_deallocator(
    pe::DescriptorFreelist& list,
    std::vector<OptionalDescriptor> &descs,
    std::size_t offset)
{
    const std::size_t nalloc = kNumDescriptors / kNumAllocators;
    for(int i = 0; i < nalloc; i++) {
        list.Retire(descs[offset + i].value());
    }
}

void descriptor_allocator_deallocator(
    pe::DescriptorFreelist& list,
    std::vector<OptionalDescriptor> &descs,
    std::size_t offset)
{
    const std::size_t nalloc = kNumDescriptors / kNumAllocators;
    for(int i = 0; i < nalloc; i++) {
        descs[offset + i] = list.Allocate();
    }

    for(int i = nalloc - 1; i >= 0; i--) {
        list.Retire(descs[offset + i].value());
    }
}

void test_descriptor_list()
{
    std::vector<std::future<void>> tasks{};
    std::vector<OptionalDescriptor> descs(kNumDescriptors);
    pe::DescriptorFreelist list{};

    for(int i = 0; i < kNumAllocators; i++) {
        tasks.push_back(
            std::async(std::launch::async, descriptor_allocator, 
            std::ref(list),
            std::ref(descs),
            i * (kNumDescriptors / kNumAllocators)));
    }

    for(const auto& task : tasks) {
        task.wait();
    }
    tasks.clear();

    for(int i = 0; i < kNumDeallocators; i++) {
        tasks.push_back(
            std::async(std::launch::async, descriptor_allocator, 
            std::ref(list),
            std::ref(descs),
            i * (kNumDescriptors / kNumDeallocators)));
    }

    for(const auto& task : tasks) {
        task.wait();
    }
    tasks.clear();

    for(int i = 0; i < kNumAllocators; i++) {
        tasks.push_back(
            std::async(std::launch::async, descriptor_allocator_deallocator, 
            std::ref(list),
            std::ref(descs),
            i * (kNumDescriptors / kNumAllocators)));
    }

    for(const auto& task : tasks) {
        task.wait();
    }
}

void test_pagemap()
{
    pe::Pagemap pagemap{};
    std::array<void*, kNumBlocks> blocks{};
    std::array<size_t, kNumBlocks> block_sizes{};
    std::array<pe::SuperblockDescriptor, kNumBlocks> block_descriptors{};
    std::array<std::size_t, 10> possible_sizes{
        16, 64, 128, 256, 512, 1024, 2048,
        4096, 8192, 16384
    };

    /* Allocate kNumBlocks blocks of random size */
    for(int i = 0; i < kNumBlocks; i++) {

        size_t idx = std::rand() % std::size(possible_sizes);
        size_t size = possible_sizes[idx];
        size_t sc = std::distance(std::begin(possible_sizes),
            std::find(std::begin(possible_sizes), std::end(possible_sizes), size)) + 1;

        blocks[i] = malloc(pe::kSuperblockSize);
        block_sizes[i] = size;
        block_descriptors[i].m_superblock = reinterpret_cast<uintptr_t>(blocks[i]);
        block_descriptors[i].m_blocksize = block_sizes[i];
        block_descriptors[i].m_maxcount = 1;
        block_descriptors[i].m_sizeclass = sc;

        pagemap.RegisterDescriptor(&block_descriptors[i]);
    }

    for(int i = 0; i < kNumBlocks; i++) {

        std::byte *block = reinterpret_cast<std::byte*>(blocks[i]);
        pe::assert(pagemap.GetDescriptor(block) == &block_descriptors[i]);
        pe::assert(pagemap.GetSizeClass(block) == block_descriptors[i].m_sizeclass);

        pagemap.UnregisterDescriptor(&block_descriptors[i]);
        free(block);
    }
}

void test_allocator_single_thread(pe::Allocator& alloc)
{
    std::array<void*, kNumAllocations> allocations{nullptr};

    std::default_random_engine generator;
    std::poisson_distribution<int> distribution(1.0);

    std::array<std::size_t, 20> possible_sizes{
        4,      8,      16,     22, 
        31,     32,     64,     128, 
        256,    312,    512,    1024, 
        2048,   4096,   6032,   8192, 
        12128,  16384,  65536,  1048576 
    };

    for(int i = 0; i < kNumAllocations; i++) {
        size_t idx;
        do{
            idx = distribution(generator);
        }while(idx >= std::size(possible_sizes));

        size_t size = possible_sizes[idx];
        allocations[i] = alloc.Allocate(size);
        pe::assert(allocations[i] != nullptr);
        pe::assert(allocations[i] != reinterpret_cast<void*>(-1ull));
    }

    for(int i = 0; i < kNumAllocations; i++) {
        alloc.Free(allocations[i]);
    }
}

void allocator(pe::Allocator& alloc, pe::LockfreeQueue<void*>& queue)
{
    std::default_random_engine generator;
    std::poisson_distribution<int> distribution(1.0);

    std::array<std::size_t, 20> possible_sizes{
        4,      8,      16,     22, 
        31,     32,     64,     128, 
        256,    312,    512,    1024, 
        2048,   4096,   6032,   8192, 
        12128,  16384,  65536,  1048576 
    };

    for(int i = 0; i < kNumAllocations; i++) {
        size_t idx;
        do{
            idx = distribution(generator);
        }while(idx >= std::size(possible_sizes));

        size_t size = possible_sizes[idx];
        void *allocation = alloc.Allocate(size);
        pe::assert(allocation != nullptr);
        pe::assert(allocation != reinterpret_cast<void*>(-1ull));
        queue.Enqueue(allocation);
    }
}

void deallocator(pe::Allocator& alloc, 
    pe::LockfreeQueue<void*>& queue, std::atomic_uint32_t& num_deallocs)
{
    while(num_deallocs.load(std::memory_order_relaxed) < kNumAllocations) {
        auto val = queue.Dequeue();
        if(!val.has_value())
            continue;
        alloc.Free(val.value());
        num_deallocs.fetch_add(1, std::memory_order_release);
    }
}

void test_allocator_multi_thread(pe::Allocator& alloc)
{
    std::vector<std::future<void>> tasks{};
    auto queue = pe::LockfreeQueue<void*>{};
    std::atomic_uint32_t num_deallocs{};

    for(int i = 0; i < kNumAllocators; i++) {
        tasks.push_back(
            std::async(std::launch::async, allocator, 
            std::ref(alloc), std::ref(queue)));
    }

    for(int i = 0; i < kNumDeallocators; i++) {
        tasks.push_back(
            std::async(std::launch::async, deallocator,
            std::ref(alloc), std::ref(queue), std::ref(num_deallocs)));
    }

    for(const auto& task : tasks) {
        task.wait();
    }
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting memory allocation test.");

        pe::Allocator& alloc = pe::Allocator::Instance();

        test_descriptor_list();
        test_pagemap();
        test_allocator_single_thread(alloc);
        test_allocator_multi_thread(alloc);

        pe::ioprint(pe::TextColor::eGreen, "Finished memory allocation test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

