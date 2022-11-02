import lockfree_list;
import logger;
import assert;
import platform;

import <cstdlib>;
import <exception>;
import <random>;
import <unordered_set>;
import <limits>;
import <future>;
import <list>;


constexpr int kWorkerCount = 16;
constexpr int kNumValues = 10000;

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
    for(int i = 0; i < input.size(); i++) {
        bool result = list.Insert(input[i]);
        pe::assert(result, "insert");
    }
}

template <List<int> ListType>
void deleter(ListType& list, std::vector<int>& input)
{
    for(int i = 0; i < input.size(); i++) {
        bool result = list.Find(input[i]);
        pe::assert(result, "find");

        result = list.Delete(input[i]);
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

        std::unordered_set<int> set;
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

        auto& lockfree_list = pe::LockfreeList<int, 0>::Instance();
        pe::dbgtime([&](){
            test(lockfree_list, work_items);
        }, [&](uint64_t delta) {
            pe::dbgprint("Lockfree list test with", kWorkerCount, "worker(s) and",
                kNumValues, "value(s) took", pe::rdtsc_usec(delta), "microseconds.");
        });

        BlockingList<int> blocking_list{};
        pe::dbgtime([&](){
            test(blocking_list, work_items);
        }, [&](uint64_t delta) {
            pe::dbgprint("Blocking list test with", kWorkerCount, "worker(s) and",
                kNumValues, "value(s) took", pe::rdtsc_usec(delta), "microseconds.");
        });

        pe::ioprint(pe::TextColor::eGreen, "Finished insertion-deletion test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }

    return ret;
}

