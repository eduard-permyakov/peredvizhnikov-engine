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

import assert;
import logger;
import atomic_struct;

import <random>;
import <array>;
import <cstdlib>;
import <future>;
import <string>;


constexpr int kNumLoaders = 8;
constexpr int kNumStorers = 8;
constexpr int kNumLoads = 5000;
constexpr int kNumStores = 5000;
constexpr int kMaxLockstepCountNumber = 5000;
constexpr int kNumExchangers = 8;
constexpr int kMaxExchangeCountNumber = 5000;
constexpr int kNumAdders = 8;
constexpr int kAddStep = 2;
constexpr int kNumAddSteps = 1000;

template <std::size_t Size>
requires (Size >= 1)
struct Series
{
private:

    int                   m_base;
    int                   m_delta;
    std::array<int, Size> m_series;

public:

    Series() = default;

    Series(int base, int delta)
        : m_base{base}
        , m_delta{delta}
        , m_series{}
    {
        m_series[0] = base;
        for(int i = 1; i < Size; i++) {
            m_series[i] = m_series[i - 1] + delta;
        }
    }

    bool CheckConsistent() const
    {
        if(m_series[0] != m_base)
            return false;
        for(int i = 1; i < Size; i++) {
            if(m_series[i] != m_series[i - 1] + m_delta)
                return false;
        }
        return true;
    }
};

using AtomicSeries = pe::AtomicStruct<Series<32>>;

struct StringNumber
{
    int                   m_integer;
    std::array<char, 256> m_string;

    StringNumber() = default;

    StringNumber(int i)
        : m_integer{i}
        , m_string{}
    {
        auto str = std::to_string(i);
        const char *cstr = str.c_str();
        std::copy(cstr, cstr + str.size() + 1, std::begin(m_string));
    }

    bool CheckConsistent() const
    {
        auto expected = std::to_string(m_integer);
        auto actual = std::string{std::begin(m_string)};
        return (expected == actual);
    }
};

using AtomicStringNumber = pe::AtomicStruct<StringNumber>;

void storer(AtomicSeries& series)
{
    std::uniform_int_distribution<int> base_dist{-100, 100};
    std::uniform_int_distribution<int> delta_dist{-10, 10};
    std::default_random_engine re{};

    for(int i = 0; i < kNumStores; i++) {

        int base = base_dist(re); 
        int delta = delta_dist(re);

        Series<32> newval{base, delta};
        series.Store(newval);
    }
}

void loader(AtomicSeries& series)
{
    for(int i = 0; i < kNumLoads; i++) {

        auto val = series.Load();
        pe::assert(val.CheckConsistent(), "Loaded series is in an inconsistent state!");
    }
}

void lockstep_counter(AtomicStringNumber& number, bool even)
{
    while(true) {
        auto value = number.Load();
        pe::assert(value.CheckConsistent(), "Unexpected string value!");

        if(value.m_integer == kMaxLockstepCountNumber)
            break;

        StringNumber next{value.m_integer + 1};
        if(even) {
            if(value.m_integer % 2 == 0){
                number.Store(next);
            }
        }else{
            if(value.m_integer % 2 == 1){
                number.Store(next);
            }
        }
    }
}

void test_load_store()
{
    /* Perform loads and stores from different threads and ensure that
     * the data is always consistent.
     */
    AtomicSeries series{};
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < kNumStorers; i++) {
        tasks.push_back(std::async(std::launch::async, storer, std::ref(series)));
    }
    for(int i = 0; i < kNumLoaders; i++) {
        tasks.push_back(std::async(std::launch::async, loader, std::ref(series)));
    }
    for(const auto& task : tasks) {
        task.wait();
    }

    /* Use a polling protocol to advance the state of the number
     * by two different threads.
     */
    StringNumber zero{0};
    AtomicStringNumber number{zero};
    auto even = std::async(std::launch::async, lockstep_counter, std::ref(number), true);
    auto odd = std::async(std::launch::async, lockstep_counter, std::ref(number), false);

    even.wait();
    odd.wait();
}

void exchanger(AtomicStringNumber& number, std::atomic_int& counter)
{
    for(int next = 0; next < kMaxExchangeCountNumber;) {

        StringNumber newval{next};
        auto current = number.Exchange(newval);
        pe::assert(current.CheckConsistent(), "Unexpected string value!");

        counter.fetch_add(1, std::memory_order_relaxed);
        next = std::max(current.m_integer, next) + 1;
    }
}

void test_exchange()
{
    StringNumber zero{0};
    AtomicStringNumber number{zero};
    std::atomic_int counter{};
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < kNumExchangers; i++) {
        tasks.push_back(std::async(std::launch::async, exchanger, 
            std::ref(number), std::ref(counter)));
    }
    for(const auto& task : tasks) {
        task.wait();
    }
    pe::dbgprint(kNumExchangers, "threads adding 0 to", kMaxExchangeCountNumber,
        "takes a total of", counter.load(std::memory_order_relaxed), "Exchange operations.");
}

void adder(AtomicStringNumber& number, int delta, std::atomic_int& counter)
{
    for(int i = 0; i < kNumAddSteps; i++){
        auto value = number.Load();
        StringNumber newvalue{};
        pe::assert(value.CheckConsistent(), "Unexpected string value!");
        pe::assert(value.m_integer % delta == 0);
        do{
            counter.fetch_add(1, std::memory_order_relaxed);
            newvalue = StringNumber{value.m_integer + delta};
        }while(!number.CompareExchange(value, newvalue));
    }
}

void test_compare_exchange()
{
    StringNumber zero{0};
    AtomicStringNumber number{zero};
    std::atomic_int counter{};
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < kNumAdders; i++) {
        tasks.push_back(std::async(std::launch::async, adder, 
            std::ref(number), kAddStep, std::ref(counter)));
    }
    for(const auto& task : tasks) {
        task.wait();
    }

    int expected = kNumAdders * kAddStep * kNumAddSteps;
    int actual = number.Load().m_integer;
    pe::assert(expected == actual, "Unexpected final value!");

    pe::dbgprint(kNumExchangers, "threads adding 0 to", expected,
        "by increments of", kAddStep,
        "takes a total of", counter.load(std::memory_order_relaxed), "CompareExchange operations.");
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting Atomic Struct test.");

        test_load_store();
        test_exchange();
        test_compare_exchange();

        pe::ioprint(pe::TextColor::eGreen, "Finished Atomic Struct test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

