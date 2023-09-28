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

import atomic_work;
import lockfree_iterable_list;
import logger;
import assert;

import <cstdlib>;
import <array>;
import <future>;
import <optional>;
import <numeric>;
import <span>;
import <vector>;


constexpr int kNumSteps = 100;
constexpr int kNumParallelWorkItems = 100;
constexpr int kNumParallelWorkers = 10;

struct Object
{
    int m_a;
    int m_b;
    int m_c;
    int m_d;
    int m_e;
    int m_f;

    void step(int i)
    {
        m_a += i;
        m_b = m_a + 1;
        m_c = m_b + 2;
        m_d = m_c + 3;
        m_e = m_d + 4;
        m_f = m_e + 5;
    }

    bool operator==(const Object& rhs) const
    {
        return (m_a == rhs.m_a)
            && (m_b == rhs.m_b)
            && (m_c == rhs.m_c)
            && (m_d == rhs.m_d)
            && (m_e == rhs.m_e)
            && (m_f == rhs.m_f);
    }

    std::strong_ordering operator<=>(const Object& rhs) const
    {
        return (m_a <=> rhs.m_a);
    }
};

void lfsw_worker(int i, pe::AtomicFunctionalSerialWork<Object>& work)
{
    work.PerformSerially(+[](Object& obj, int i) {
        obj.step(i);
    }, i);
}

void test_lfsw()
{
    Object test{0, 1, 2, 3, 4, 5};
    pe::AtomicFunctionalSerialWork work{test};
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < kNumSteps; i++) {
        tasks.push_back(
            std::async(std::launch::async, lfsw_worker, 
            i, std::ref(work)));
    }
    for(const auto& task : tasks) {
        task.wait();
    }

    Object result = work.GetResult();
    Object expected = test;
    for(int i = 0; i < kNumSteps; i++) {
        expected.step(i);
    }
    pe::assert(result == expected);
}

template <typename Work>
void lfpw_worker(Work& work)
{
    work.Complete(0);
}

void test_lfpw()
{
    std::array<Object, kNumParallelWorkItems> objects{};
    for(int i = 0; i < std::size(objects); i++) {
        objects[i] = {i, i + 1, i + 2, i + 3, i + 4, i + 5};
    }

    struct ObjectStepWorkItem
    {
        Object m_object;
        int    m_i;
    };

    std::vector<std::future<void>> tasks{};
    std::array<ObjectStepWorkItem, std::size(objects)> input{};
    for(int i = 0; i < std::size(input); i++) {
        input[i] = {objects[i], i};
    }

    std::atomic_int retry_count{};
    pe::AtomicParallelWork work{
        input, retry_count,
        +[](uint64_t, const ObjectStepWorkItem& work, std::atomic_int& retries) {
            retries.fetch_add(1, std::memory_order_relaxed);
            Object result = work.m_object;
            result.step(work.m_i);
            return std::optional{result};
        }
    };

    for(int i = 0; i < kNumParallelWorkers; i++) {
        tasks.push_back(
            std::async(std::launch::async, lfpw_worker<decltype(work)>, 
            std::ref(work)));
    }

    /* We don't have to wait for the threads to complete.
     * The 'awaiting' thread will help out in the work
     * and is able to 'steal' work from any preempted
     * thread.
     */
    auto results = work.GetResult(0);
    pe::dbgprint("Completed", results.size(), "work items with",
        retry_count.load(std::memory_order_relaxed), "retries.");

    for(int i = 0; i < std::size(objects); i++) {
        objects[i].step(input[i].m_i);
    }

    pe::assert(results.size() == objects.size());

    for(int i = 0; i < kNumParallelWorkItems; i++) {
        pe::assert(objects[i] == results[i]);
    }

    for(const auto& task : tasks) {
        task.wait();
    }
}

template <typename Work>
void lfw_pipeline_worker(Work& work)
{
    work.Complete(0);
}

void test_lfw_pipeline()
{
    std::array<int, kNumParallelWorkItems> items;
    std::iota(std::begin(items), std::end(items), 0);

    struct SharedState
    {
        std::atomic_int m_first_phase_retries{};
        std::atomic_int m_second_phase_retries{};
        std::atomic_int m_third_phase_retries{};
    }state{};

    pe::AtomicWorkPipeline<
        SharedState,
        pe::AtomicParallelWork<int, int, SharedState>,
        pe::AtomicParallelWork<int, int, SharedState>,
        pe::AtomicParallelWork<int, int, SharedState>
    > pipeline{
        items, state,
        +[](uint64_t, const int& item, SharedState& state) {
            state.m_first_phase_retries.fetch_add(1, std::memory_order_relaxed);
            return std::optional{item * 2};
        },
        +[](uint64_t, const int& item, SharedState& state) {
            state.m_second_phase_retries.fetch_add(1, std::memory_order_relaxed);
            if(item > 10)
                return std::optional{item};
            return std::optional<int>{};
        },
        +[](uint64_t, const int& item, SharedState& state) {
            state.m_third_phase_retries.fetch_add(1, std::memory_order_relaxed);
            return std::optional{item * 2};
        }
    };

    std::vector<int> expected;
    for(int i = 0; i < std::size(items); i++) {
        items[i] *= 2;
    }
    for(int i = 0; i < std::size(items); i++) {
        if(items[i] > 10)
            expected.push_back(items[i]);
    }
    for(int i = 0; i < expected.size(); i++) {
        expected[i] *= 2;
    }

    std::vector<std::future<void>> tasks{};
    for(int i = 0; i < kNumParallelWorkers; i++) {
        tasks.push_back(
            std::async(std::launch::async, lfw_pipeline_worker<decltype(pipeline)>, 
            std::ref(pipeline)));
    }

    auto result = pipeline.GetResult(0);

    for(const auto& task : tasks) {
        task.wait();
    }

    pe::assert(result.size() == expected.size());
    for(int i = 0; i < result.size(); i++) {
        pe::assert(result[i] == expected[i]);
    }
    pe::dbgprint("Completed lockfree work pipeline. [Phase 1 retries:"
        + std::to_string(state.m_first_phase_retries.load(std::memory_order_relaxed))
        + ", Phase 2 retries:"
        + std::to_string(state.m_second_phase_retries.load(std::memory_order_relaxed))
        + ", Phase 3 retries:"
        + std::to_string(state.m_third_phase_retries.load(std::memory_order_relaxed))
        + "]");
}

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::ioprint(pe::TextColor::eGreen, "Testing Lock-Free Serial Work.");
        test_lfsw();
        pe::ioprint(pe::TextColor::eGreen, "Finished Lock-Free Serial Work test.");

        pe::ioprint(pe::TextColor::eGreen, "Testing Lock-Free Parallel Work.");
        test_lfpw();
        pe::ioprint(pe::TextColor::eGreen, "Finished Lock-Free Parallel Work test.");

        pe::ioprint(pe::TextColor::eGreen, "Testing Lock-Free Work Pipeline.");
        test_lfw_pipeline();
        pe::ioprint(pe::TextColor::eGreen, "Finished Lock-Free Work Pipeline test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }

    return ret;
}

