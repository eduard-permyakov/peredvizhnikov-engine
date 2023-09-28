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

import lockfree_deque;
import platform;
import logger;
import assert;
import atomic_trace;

import <cstdlib>;
import <optional>;
import <mutex>;
import <deque>;
import <set>;
import <limits>;
import <numeric>;
import <future>;
import <vector>;


constexpr int kLeftProducerCount = 4;
constexpr int kLeftConsumerCount = 4;
constexpr int kRightProducerCount = 4;
constexpr int kRightConsumerCount = 4;
constexpr int kNumValues = 10'000;

static std::atomic_int consumed{};

template <typename D, typename T>
concept Deque = requires(D deque, T value)
{
    {deque.PushLeft(value)} -> std::same_as<void>;
    {deque.PushRight(value)} -> std::same_as<void>;
    {deque.PopLeft()} -> std::same_as<std::optional<T>>;
    {deque.PopRight()} -> std::same_as<std::optional<T>>;
};

template <typename T>
class BlockingDeque
{
private:

    std::mutex    m_mutex{};
    std::deque<T> m_deque{};

public:

    template <typename U>
    void PushLeft(U&& value)
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        m_deque.push_front(std::forward<U>(value));
    }

    template <typename U>
    void PushRight(U&& value)
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        m_deque.push_back(std::forward<U>(value));
    }

    std::optional<T> PopLeft()
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        if(m_deque.empty())
            return std::nullopt;
        auto&& ret = std::make_optional(m_deque.front());
        m_deque.pop_front();
        return ret;
    }

    std::optional<T> PopRight()
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        if(m_deque.empty())
            return std::nullopt;
        auto&& ret = std::make_optional(m_deque.back());
        m_deque.pop_back();
        return ret;
    }
};

static bool in_sets(auto&& sets, int value)
{
    for(int i = 0; i < std::size(sets); i++) {
        if(sets[i].contains(value))
            return true;
    }
    return false;
}

template <Deque<int> DequeType>
void left_producer(DequeType& deque, const std::set<int, std::greater<int>>& input)
{
    for(int item : input) {
        deque.PushLeft(item);
    }
}

template <Deque<int> DequeType>
void right_producer(DequeType& deque, const std::set<int, std::less<int>>& input)
{
    for(int item : input) {
        deque.PushRight(item);
    }
}

template <Deque<int> DequeType>
void left_consumer(DequeType& deque, DequeType& output,
    const std::set<int, std::greater<int>> (&left_jobs)[kLeftProducerCount],
    const std::set<int, std::less<int>>    (&right_jobs)[kRightProducerCount])
{
    while(consumed.load(std::memory_order_relaxed) < kNumValues) {
        auto elem = deque.PopLeft();
        if(!elem.has_value())
            continue;

        int value = elem.value();
        consumed.fetch_add(1, std::memory_order_relaxed);

        if(in_sets(left_jobs, value)) {
            output.PushLeft(value);
            goto advance;
        }
        if(in_sets(right_jobs, value)) {
            output.PushRight(value);
            goto advance;
        }
        pe::assert(false);
        advance:;
    }
}

template <Deque<int> DequeType>
void right_consumer(DequeType& deque, DequeType& output,
    const std::set<int, std::greater<int>> (&left_jobs)[kLeftProducerCount],
    const std::set<int, std::less<int>>    (&right_jobs)[kRightProducerCount])
{
    while(consumed.load(std::memory_order_relaxed) < kNumValues) {
        auto elem = deque.PopRight();
        if(!elem.has_value())
            continue;

        int value = elem.value();
        consumed.fetch_add(1, std::memory_order_relaxed);

        if(in_sets(left_jobs, value)) {
            output.PushLeft(value);
            goto advance;
        }
        if(in_sets(right_jobs, value)) {
            output.PushRight(value);
            goto advance;
        }
        pe::assert(false);
        advance:;
    }
}

template <Deque<int> DequeType>
void validate(DequeType& deque,
    const std::set<int, std::greater<int>> (&left_jobs)[kLeftProducerCount],
    const std::set<int, std::less<int>>    (&right_jobs)[kRightProducerCount])
{
    std::vector<int> values;
    std::optional<int> curr;
    do{
        curr = deque.PopLeft();
        if(curr.has_value()) {
            values.push_back(curr.value());
        }
    }while(curr.has_value());

    std::vector<int> values_set{values.begin(), values.end()};
    pe::assert(values_set.size() == values.size());
    pe::assert(values_set.size() == kNumValues);

    /* Check that all items on the left side are from the left sets
     * and all items from the right side are from the right set
     */
    bool right_seen = false;
    for(int value : values) {
        bool left = in_sets(left_jobs, value);
        bool right = in_sets(right_jobs, value);

        pe::assert(left ^ right);
        if(left) {
            pe::assert(!right_seen);
        }else{
            right_seen = true;
        }
    }
}

template <Deque<int> DequeType>
void test(DequeType& deque, DequeType& output,
    const std::set<int, std::greater<int>> (&left_jobs)[kLeftProducerCount],
    const std::set<int, std::less<int>>    (&right_jobs)[kRightProducerCount])
{
    std::vector<std::future<void>> tasks{};
    for(int i = 0; i < kLeftProducerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, 
            left_producer<decltype(deque)>, 
            std::ref(deque), 
            std::ref(left_jobs[i])));
    }
    for(int i = 0; i < kRightProducerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, 
            right_producer<decltype(deque)>, 
            std::ref(deque), 
            std::ref(right_jobs[i])));
    }
    for(int i = 0; i < kLeftConsumerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, 
            left_consumer<decltype(deque)>, 
            std::ref(deque),
            std::ref(output),
            std::ref(left_jobs),
            std::ref(right_jobs)));
    }
    for(int i = 0; i < kRightConsumerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, 
            right_consumer<decltype(deque)>, 
            std::ref(deque),
            std::ref(output),
            std::ref(left_jobs),
            std::ref(right_jobs)));
    }
    for(const auto& task : tasks) {
        task.wait();
    }
    validate(output, left_jobs, right_jobs);
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        /* Prepare the input data. Generate a sequential series 
         * and split it between two equal-sized sets, with the
         * second set having the top half and the first set having
         * the bottom half. Some tasks will push to the left side of 
         * the deque from a reversed first set and some tasks will 
         * push to the right side of the deque from an in-order right 
         * set.
         */
        std::vector<int> items(kNumValues);
        std::iota(std::begin(items), std::end(items), 0);

        std::vector<int> left{std::begin(items), std::begin(items) + kNumValues / 2};
        std::vector<int> right{std::begin(items) + kNumValues / 2, std::end(items)};
        std::reverse(std::begin(left), std::end(left));

        pe::assert(std::size(left) + std::size(right) == std::size(items));

        /* Further subdivide the left and right sets between different
         * workers. 
         */
        std::set<int, std::greater<int>> left_jobs[kLeftProducerCount];
        for(int i = 0; i < kLeftProducerCount; i++) {
            std::size_t items_per_worker = (std::size(left) / kLeftProducerCount);
            auto begin = std::begin(left) + (items_per_worker * i);
            auto end = begin + items_per_worker;
            if(i == kLeftProducerCount-1)
                end = std::end(left);
            left_jobs[i] = std::set<int, std::greater<int>>{begin, end};
        }

        std::set<int> right_jobs[kRightProducerCount];
        for(int i = 0; i < kRightProducerCount; i++) {
            std::size_t items_per_worker = (std::size(right) / kLeftProducerCount);
            auto begin = std::begin(right) + (items_per_worker * i);
            auto end = begin + items_per_worker;
            if(i == kLeftProducerCount-1)
                end = std::end(right);
            right_jobs[i] = std::set<int>{begin, end};
        }

        pe::ioprint(pe::TextColor::eGreen, "Starting multi-producer multi-consumer test.");

        auto lockfree_deque = pe::LockfreeDeque<int>{};
        auto lockfree_output = pe::LockfreeDeque<int>{};

        pe::dbgtime<true>([&](){
            test(lockfree_deque, lockfree_output, left_jobs, right_jobs);
        }, [&](uint64_t delta) {
            pe::dbgprint("Lockfree deque test with", kNumValues, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        consumed.store(0);
        auto blocking_deque = BlockingDeque<int>{};
        auto blocking_ouput = BlockingDeque<int>{};

        pe::dbgtime<true>([&](){
            test(blocking_deque, blocking_ouput, left_jobs, right_jobs);
        }, [&](uint64_t delta) {
            pe::dbgprint("Blocking deque test with", kNumValues, "value(s) took",
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

