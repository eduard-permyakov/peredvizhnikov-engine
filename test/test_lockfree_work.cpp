import lockfree_work;
import iterable_lockfree_list;
import logger;
import assert;

import <cstdlib>;
import <array>;
import <future>;
import <optional>;


constexpr int kNumSteps = 100;
constexpr int kNumParallelWorkItems = 100;
constexpr int kNumParallelWorkers = 10;

struct Object
{
    int a;
    int b;
    int c;
    int d;
    int e;
    int f;

    void step(int i)
    {
        a += i;
        b = a + 1;
        c = b + 2;
        d = c + 3;
        e = d + 4;
        f = e + 5;
    }

    bool operator==(const Object& rhs) const
    {
        return (a == rhs.a)
            && (b == rhs.b)
            && (c == rhs.c)
            && (d == rhs.d)
            && (e == rhs.e)
            && (f == rhs.f);
    }

    std::strong_ordering operator<=>(const Object& rhs) const
    {
        return (a <=> rhs.a);
    }
};

void lfsw_worker(int i, pe::LockfreeFunctionalSerialWork<Object>& work)
{
    work.PerformSerially(+[](Object& obj, int i) {
        obj.step(i);
    }, i);
}

void test_lfsw()
{
    Object test{0, 1, 2, 3, 4, 5};
    pe::LockfreeFunctionalSerialWork work{test};
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
    pe::assert(result == expected, "", __FILE__, __LINE__);
}

template <typename Work>
void lfpw_worker(Work& work)
{
    work.Complete();
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
    pe::LockfreeParallelWork work{
        input, retry_count,
        +[](const ObjectStepWorkItem& work, std::atomic_int& retries) {
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
    auto results = work.GetResults();
    pe::dbgprint("Completed", results.size(), "work items with",
        retry_count.load(std::memory_order_relaxed), "retries.");

    for(int i = 0; i < std::size(objects); i++) {
        objects[i].step(input[i].m_i);
    }

    pe::assert(results.size() == objects.size(), "", __FILE__, __LINE__);

    for(int i = 0; i < kNumParallelWorkItems; i++) {
        pe::assert(objects[i] == results[i], "", __FILE__, __LINE__);
    }

    for(const auto& task : tasks) {
        task.wait();
    }
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

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }

    return ret;
}

