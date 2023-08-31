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

import lockfree_stack;
import concurrency;
import platform;
import logger;
import assert;

import <cstdlib>;
import <stack>;
import <optional>;
import <mutex>;
import <future>;
import <set>;
import <vector>;


constexpr int kProducerCount = 16;
constexpr int kConsumerCount = 16;
constexpr int kNumValues = 100'000;

static std::atomic_int produced{};
static std::atomic_int consumed{};

template <typename S, typename T>
concept Stack = requires(S stack, T value)
{
    {stack.Push(value)} -> std::same_as<bool>;
    {stack.Pop()} -> std::same_as<std::optional<T>>;
};

template <std::size_t Capacity, typename T>
class BlockingStack
{
private:

    std::mutex    m_mutex{};
    std::stack<T> m_stack{};

public:

    template <typename U>
    bool Push(U&& value)
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        if(m_stack.size() == Capacity)
            return false;
        m_stack.push(std::forward<U>(value));
        return true;
    }

    std::optional<T> Pop()
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        if(m_stack.empty())
            return std::nullopt;
        auto&& ret = std::make_optional(m_stack.top());
        m_stack.pop();
        return ret;
    }
};

template <Stack<int> StackType>
void producer(StackType& stack)
{
    std::atomic_thread_fence(std::memory_order_acquire);
    while(produced.load(std::memory_order_relaxed) < kNumValues) {

        int expected = produced.load(std::memory_order_relaxed);
        do{
            if(expected == kNumValues)
                return;
        }while(!produced.compare_exchange_weak(expected, expected + 1,
            std::memory_order_release, std::memory_order_relaxed));

        stack.Push(expected);
    }
}

template <Stack<int> InStack, Stack<int> OutStack>
void consumer(InStack& stack, OutStack& result)
{
    std::atomic_thread_fence(std::memory_order_acquire);
    while(consumed.load(std::memory_order_relaxed) < kNumValues) {

        auto elem = stack.Pop();
        if(!elem.has_value())
            continue;
        result.Push(*elem);

        int expected = consumed.load(std::memory_order_relaxed);
        while(!consumed.compare_exchange_weak(expected, consumed + 1,
            std::memory_order_release, std::memory_order_relaxed));
    }
}

template <Stack<int> InStack, Stack<int> OutStack>
void test(InStack& stack, OutStack& result)
{
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < kProducerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, producer<decltype(stack)>, 
            std::ref(stack)));
    }
    for(int i = 0; i < kConsumerCount; i++) {
        tasks.push_back(std::async(std::launch::async, 
            consumer<decltype(stack), decltype(result)>, 
            std::ref(stack), std::ref(result)));
    }

    for(const auto& task : tasks) {
        task.wait();
    }
}

template <Stack<int> StackType>
void verify(StackType& result)
{
    std::set<int> set{};
    std::optional<int> elem;
    do{
        elem = result.Pop();
        if(elem.has_value()) {
            set.insert(elem.value());
        }
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

        static auto lockfree_stack = pe::LockfreeStack<kNumValues, int>{};
        static auto lockfree_result = pe::LockfreeStack<kNumValues, int>{};

        pe::dbgtime<true>([&](){
            test(lockfree_stack, lockfree_result);
        }, [&](uint64_t delta) {
            verify(lockfree_result);
            pe::dbgprint("Lockfree stack test with", kProducerCount, "producer(s),",
                kConsumerCount, "consumer(s) and", kNumValues, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        produced.store(0, std::memory_order_relaxed);
        consumed.store(0, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);

        BlockingStack<kNumValues, int> blocking_stack{};
        BlockingStack<kNumValues, int> blocking_result{};

        pe::dbgtime<true>([&](){
            test(blocking_stack, blocking_result);
        }, [&](uint64_t delta) {
            verify(blocking_result);
            pe::dbgprint("Blocking stack test with", kProducerCount, "producer(s),",
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

