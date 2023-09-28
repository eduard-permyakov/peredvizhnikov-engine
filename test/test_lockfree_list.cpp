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

import lockfree_list;
import lockfree_iterable_list;
import logger;
import assert;
import platform;

import <cstdlib>;
import <exception>;
import <random>;
import <set>;
import <limits>;
import <future>;
import <list>;


constexpr int kWorkerCount = 16;
constexpr int kIteratorCount = 4;
constexpr int kNumValues = 5'000;

template <typename L, typename T>
concept List = requires(L list, T value)
{
    {list.Insert(value)} -> std::same_as<bool>;
    {list.Delete(value)} -> std::same_as<bool>;
    {list.Find(value)} -> std::same_as<bool>;
};

template <typename T>
class BlockingList
{
private:

    std::mutex   m_mutex{};
    std::list<T> m_list{};

public:

    template <typename U>
    bool Insert(U&& value)
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        auto at = std::lower_bound(m_list.begin(), m_list.end(), value);
        bool exists = (at != m_list.end()) && (*at == value);
        if(!exists) {
            m_list.insert(at, std::forward<U>(value));
        }
        return !exists;
    }

    bool Find(T value)
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        auto at = std::find(m_list.begin(), m_list.end(), value);
        return (at != m_list.end());
    }

    bool Delete(T value)
    {
        std::lock_guard<std::mutex> lock{m_mutex};
        auto at = std::find(m_list.begin(), m_list.end(), value);
        if(at == m_list.end())
            return false;
        m_list.erase(at);
        return true;
    }
};

template <List<int> ListType>
void inserter(ListType& list, std::vector<int>& input)
{
    using namespace std::string_literals;
    for(int i = 0; i < input.size(); i++) {
        bool result = list.Insert(input[i]);
        pe::assert(result, "insert "s + std::to_string(input[i]));
    }
}

template <List<int> ListType>
void deleter(ListType& list, std::vector<int>& input)
{
    for(auto it = input.rbegin(); it != input.rend(); it++) {
        bool result = list.Find(*it);
        pe::assert(result, "find");

        result = list.Delete(*it);
        pe::assert(result, "delete");
    }
}

template <List<int> ListType>
void test(ListType &list, std::vector<std::vector<int>>& work_items)
{
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < kWorkerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, inserter<decltype(list)>, 
            std::ref(list), std::ref(work_items[i])));
    }
    for(const auto& task : tasks) {
        task.wait();
    }
    tasks.clear();

    for(int i = 0; i < kWorkerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, deleter<decltype(list)>, 
            std::ref(list), std::ref(work_items[i])));
    }
    for(const auto& task : tasks) {
        task.wait();
    }
}

void validate_concurrent_snapshot(const std::vector<int>& snapshot,
    const std::vector<std::vector<int>>& work_items)
{
    /* We know the inserters are working in parallel to 
     * push slices of the set into the list in order. So 
     * we know that a valid snapshot is one where it holds
     * only items from the start of each worker's input
     * array. If we find any "skipped" entries, then the
     * linearizability property has been violated.
     *
     * The deleters are deleting in reverse order of their
     * work items array. So the same logic holds - valid
     * snapshots contain only the first entries in deleter's
     * work items.
     */
    int worker_idx = 0;
    int items_idx = 0;

    for(int i = 0; i < snapshot.size(); i++) {

        int curr = snapshot[i];
        pe::assert(worker_idx < work_items.size(), "invalid snapshot");
        pe::assert(items_idx < work_items[worker_idx].size(), "invalid snapshot");

        if(work_items[worker_idx][items_idx] == curr) {
            items_idx++;
        }else{
            /* Find and move on to the next worker's "slice" */
            do{
                worker_idx++;
                items_idx = 0;
            }while((worker_idx < work_items.size())
                && (work_items[worker_idx].size() == 0 
                        ? true
                        : work_items[worker_idx][items_idx] != curr));

            pe::assert(worker_idx < work_items.size(), "invalid snapshot");
            items_idx++;
        }
        if(items_idx == work_items[worker_idx].size()) {
            worker_idx++;
            items_idx = 0;
        }
    }
}

void iterator_insert(pe::LockfreeIterableList<int>& list,
    const std::vector<std::vector<int>>& work_items, std::atomic_int& snapshots_taken)
{
    std::vector<int> snapshot{};
    int prev_size = 0;
    do{
        snapshot = list.TakeSnapshot();
        snapshots_taken.fetch_add(1, std::memory_order_relaxed);
        validate_concurrent_snapshot(snapshot, work_items);

        pe::assert(snapshot.size() >= prev_size, "invalid snapshot");
        prev_size = snapshot.size();

    }while(snapshot.size() < kNumValues);
}

void iterator_delete(pe::LockfreeIterableList<int>& list,
    const std::vector<std::vector<int>>& work_items, std::atomic_int& snapshots_taken)
{
    std::vector<int> snapshot{};
    int prev_size = kNumValues;
    do{
        snapshot = list.TakeSnapshot();
        snapshots_taken.fetch_add(1, std::memory_order_relaxed);
        validate_concurrent_snapshot(snapshot, work_items);

        pe::assert(snapshot.size() <= prev_size, "invalid snapshot");
        prev_size = snapshot.size();

    }while(snapshot.size() > 0);
}

void test_iterator(pe::LockfreeIterableList<int>& list, std::vector<std::vector<int>>& work_items)
{
    std::atomic_int num_snapshots = 0;
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < kWorkerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, inserter<decltype(list)>, 
            std::ref(list), std::ref(work_items[i])));
    }
    for(int i = 0; i < kIteratorCount; i++) {
        tasks.push_back(std::async(std::launch::async, iterator_insert,
            std::ref(list), std::ref(work_items), std::ref(num_snapshots)));
    }

    for(const auto& task : tasks) {
        task.wait();
    }
    tasks.clear();
    pe::dbgprint(num_snapshots.load(std::memory_order_relaxed), 
        "snapshot(s) were successfully taken concurrently with list insersions.");

    num_snapshots = 0;
    for(int i = 0; i < kWorkerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, deleter<decltype(list)>, 
            std::ref(list), std::ref(work_items[i])));
    }
    for(int i = 0; i < kIteratorCount; i++) {
        tasks.push_back(std::async(std::launch::async, iterator_delete,
            std::ref(list), std::ref(work_items), std::ref(num_snapshots)));
    }

    for(const auto& task : tasks) {
        task.wait();
    }
    tasks.clear();
    pe::dbgprint(num_snapshots.load(std::memory_order_relaxed), 
        "snapshot(s) were successfully taken concurrently with list deletions.");
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Generating random data");
        std::random_device rd{};
        std::mt19937 mt{rd()};
        std::uniform_int_distribution<int> dist{
            std::numeric_limits<int>::min(),
            std::numeric_limits<int>::max()
        };

        std::set<int> set;
        while(set.size() < kNumValues) {
            set.insert(dist(mt));
        }

        int num_work_items = kNumValues / kWorkerCount;
        int last_extra = kNumValues - (num_work_items * kWorkerCount);

        std::vector<std::vector<int>> work_items(kWorkerCount);
        auto it = std::begin(set);

        for(int i = 0; i < kWorkerCount; i++) {

            int inserter_items = (i == kWorkerCount - 1)
                               ? (num_work_items + last_extra)
                               : num_work_items;
            for(int j = 0; j < inserter_items; j++) {
                pe::assert(it != std::end(set));
                work_items[i].push_back(*it++);
            }
        }

        pe::assert(it == std::end(set));
        pe::ioprint(pe::TextColor::eGreen, "Starting insertion-deletion test.");

        auto lockfree_list = pe::LockfreeList<int>{};
        pe::dbgtime<true>([&](){
            test(lockfree_list, work_items);
        }, [&](uint64_t delta) {
            pe::dbgprint("Lockfree list test with", kWorkerCount, "worker(s) and",
                kNumValues, "value(s) took", pe::rdtsc_usec(delta), "microseconds.");
        });

        BlockingList<int> blocking_list{};
        pe::dbgtime<true>([&](){
            test(blocking_list, work_items);
        }, [&](uint64_t delta) {
            pe::dbgprint("Blocking list test with", kWorkerCount, "worker(s) and",
                kNumValues, "value(s) took", pe::rdtsc_usec(delta), "microseconds.");
        });

        pe::ioprint(pe::TextColor::eGreen, "Finished insertion-deletion test.");

        pe::ioprint(pe::TextColor::eGreen, "Starting concurrent iteration test.");
        auto iter_list = pe::LockfreeIterableList<int>{};
        test_iterator(iter_list, work_items);
        pe::ioprint(pe::TextColor::eGreen, "Finished concurrent iteration test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

