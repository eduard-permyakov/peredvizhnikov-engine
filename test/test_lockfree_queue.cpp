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

import lockfree_queue;
import concurrency;
import platform;
import logger;
import assert;

import <cstdlib>;
import <iostream>;
import <atomic>;
import <future>;
import <exception>;
import <set>;
import <mutex>;
import <queue>;


constexpr int kProducerCount = 16;
constexpr int kConsumerCount = 16;
constexpr int kNumValues = 100'000;

static std::atomic_int produced{};
static std::atomic_int consumed{};

template <typename Q, typename T>
concept Queue = requires(Q queue, T value)
{
    {queue.Enqueue(value)} -> std::same_as<void>;
    {queue.Dequeue()} -> std::same_as<std::optional<T>>;
};

template <typename T>
class BlockingQueue
{
private:

    std::mutex    m_mutex{};
    std::queue<T> m_queue{};

public:

    template <typename U>
    void Enqueue(U&& value)
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        m_queue.push(std::forward<U>(value));
    }

    std::optional<T> Dequeue()
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        if(m_queue.empty())
            return std::nullopt;
        auto&& ret = std::make_optional(m_queue.front());
        m_queue.pop();
        return ret;
    }
};

template <Queue<int> QueueType>
void producer(QueueType& queue)
{
    std::atomic_thread_fence(std::memory_order_acquire);
    while(produced.load(std::memory_order_relaxed) < kNumValues) {

        int expected = produced.load(std::memory_order_relaxed);
        do{
            if(expected == kNumValues)
                return;
        }while(!produced.compare_exchange_weak(expected, expected + 1,
            std::memory_order_release, std::memory_order_relaxed));

        queue.Enqueue(expected);
    }
}

template <Queue<int> InQueue, Queue<int> OutQueue>
void consumer(InQueue& queue, OutQueue& result)
{
    std::atomic_thread_fence(std::memory_order_acquire);
    while(consumed.load(std::memory_order_relaxed) < kNumValues) {

        auto elem = queue.Dequeue();
        if(!elem.has_value())
            continue;
        result.Enqueue(*elem);

        int expected = consumed.load(std::memory_order_relaxed);
        while(!consumed.compare_exchange_weak(expected, consumed + 1,
            std::memory_order_release, std::memory_order_relaxed));
    }
}

template <Queue<int> InQueue, Queue<int> OutQueue>
void test(InQueue& queue, OutQueue& result)
{
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < kProducerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, producer<decltype(queue)>, 
            std::ref(queue)));
    }
    for(int i = 0; i < kConsumerCount; i++) {
        tasks.push_back(std::async(std::launch::async, 
            consumer<decltype(queue), decltype(result)>, 
            std::ref(queue), std::ref(result)));
    }

    for(const auto& task : tasks) {
        task.wait();
    }
}

template <Queue<int> QueueType>
void verify(QueueType& result)
{
    std::set<int> set{};
    std::optional<int> elem;
    do{
        elem = result.Dequeue();
        if(elem.has_value())
            set.insert(elem.value());
    }while(elem.has_value());

    pe::assert(set.size() == kNumValues, "set size");

    int current = 0;
    for(auto it = set.cbegin(); it != set.cend(); it++) {
        pe::assert(*it == current, "set value");
        current++;
    }
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting multi-producer multi-consumer test.");

        auto lockfree_queue = pe::LockfreeQueue<int>{};
        auto lockfree_result = pe::LockfreeQueue<int>{};

        pe::dbgtime<true>([&](){
            test(lockfree_queue, lockfree_result);
        }, [&](uint64_t delta) {
            verify(lockfree_result);
            pe::dbgprint("Lockfree queue test with", kProducerCount, "producer(s),",
                kConsumerCount, "consumer(s) and", kNumValues, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        produced.store(0, std::memory_order_relaxed);
        consumed.store(0, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);

        BlockingQueue<int> blocking_queue{};
        BlockingQueue<int> blocking_result{};

        pe::dbgtime<true>([&](){
            test(blocking_queue, blocking_result);
        }, [&](uint64_t delta) {
            verify(blocking_result);
            pe::dbgprint("Blocking queue test with", kProducerCount, "producer(s),",
                kConsumerCount, "consumer(s) and", kNumValues, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        pe::ioprint(pe::TextColor::eGreen, "Finished multi-producer multi-consumer test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

