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

import lockfree_sequenced_queue;
import assert;
import logger;
import concurrency;
import shared_ptr;

import <cstdlib>;
import <atomic>;
import <ranges>;
import <optional>;
import <future>;
import <vector>;
import <array>;
import <numeric>;

constexpr int kNumValues = 500;
constexpr int kNumEnqueuers = 4;
constexpr int kNumDequeuers = 4;
constexpr int kNumRequests = kNumValues * (kNumEnqueuers + kNumDequeuers);

struct alignas(16) QueueSize
{
    uint64_t m_seqnum;
    uint64_t m_size;
};

using AtomicQueueSize = pe::DoubleQuadWordAtomic<QueueSize>;

void enqueuer(pe::LockfreeSequencedQueue<int>& queue, pe::shared_ptr<AtomicQueueSize> size,
    std::atomic_uint& num_enqueued, const std::ranges::input_range auto&& input)
{
    for(const auto& value : input) {
        bool result = queue.ConditionallyEnqueue(+[](pe::shared_ptr<AtomicQueueSize> size, uint64_t seqnum, int value){
            auto expected = size->Load(std::memory_order_relaxed);
            if(expected.m_seqnum >= seqnum)
                return true;
            size->CompareExchange(expected, {seqnum, expected.m_size + 1},
                std::memory_order_relaxed, std::memory_order_relaxed);
            return true;
        }, size, value);
        if(result) {
            num_enqueued.fetch_add(1, std::memory_order_relaxed);
        }else{
            pe::assert(0, "Enqueue unexpectedly failed");
        }
    }
}

void dequeuer(pe::LockfreeSequencedQueue<int>& queue, pe::shared_ptr<AtomicQueueSize> size,
    std::atomic_uint& num_dequeued, std::atomic_uint(&result)[kNumValues], std::atomic_bool(&seqnums)[kNumRequests])
{
    while(num_dequeued.load(std::memory_order_relaxed) < kNumValues * kNumEnqueuers) {
        auto ret = queue.ConditionallyDequeue(+[](pe::shared_ptr<AtomicQueueSize> size, uint64_t seqnum, int value){
            auto expected = size->Load(std::memory_order_relaxed);
            if(expected.m_seqnum >= seqnum)
                return true;
            size->CompareExchange(expected, {seqnum, expected.m_size - 1},
                std::memory_order_relaxed, std::memory_order_relaxed);
            return true;
        }, size);
        if(ret.first.has_value()) {
            uint64_t req_seqnum = ret.second;
            pe::assert(req_seqnum >= 1 && req_seqnum <= kNumRequests, "Unexpected sequence number");
            bool seen = seqnums[req_seqnum - 1].load(std::memory_order_relaxed);
            if(!seen && seqnums[req_seqnum - 1].compare_exchange_strong(seen, true,
                std::memory_order_relaxed, std::memory_order_relaxed)) {

                num_dequeued.fetch_add(1, std::memory_order_relaxed);
                result[ret.first.value()].fetch_add(1, std::memory_order_relaxed);

                auto nd = result[ret.first.value()].load(std::memory_order_relaxed);
                pe::assert(nd <= kNumEnqueuers, "Unexpected number of dequeues");
            }
        }
    }
}

void test(pe::LockfreeSequencedQueue<int>& queue, pe::shared_ptr<AtomicQueueSize> size,
    const std::ranges::input_range auto&& input)
{
    std::vector<std::future<void>> tasks{};
    std::atomic_uint result[kNumValues];
    std::atomic_bool seqnums[kNumRequests];
    std::atomic_uint num_dequeued{};
    std::atomic_uint num_enqueued{};

    for(int i = 0; i < kNumEnqueuers; i++) {
        tasks.push_back(std::async(std::launch::async, enqueuer<decltype(input)>,
            std::ref(queue), size, std::ref(num_enqueued), input));
    }
    for(int i = 0; i < kNumDequeuers; i++) {
        tasks.push_back(std::async(std::launch::async, dequeuer,
            std::ref(queue), size, std::ref(num_dequeued), std::ref(result),
            std::ref(seqnums)));
    }
    for(const auto& task : tasks) {
        task.wait();
    }

    auto final_size = size->Load(std::memory_order_relaxed);
    pe::assert(final_size.m_size == 0, "Unexpected queue size");

    auto dequeued = num_dequeued.load(std::memory_order_relaxed);
    pe::assert(dequeued == (kNumValues * kNumEnqueuers), "Unexpected number enqueued.");

    auto enqueued = num_enqueued.load(std::memory_order_relaxed);
    pe::assert(enqueued == (kNumValues * kNumEnqueuers), "Unexpected nnumber enqueued.");

    for(int i = 0; i < std::size(result); i++) {
        auto dequeued = result[i].load(std::memory_order_relaxed);
        pe::assert(dequeued == kNumEnqueuers, 
            "Unexpected number of dequeued values.");
    }
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting Lockfree Sequenced Queue test.");

		auto size = pe::make_shared<AtomicQueueSize>();
        pe::LockfreeSequencedQueue<int> sequenced_queue{};
        test(sequenced_queue, size, std::views::iota(0, kNumValues));

        pe::ioprint(pe::TextColor::eGreen, "Finished Lockfree Sequenced Queue test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

