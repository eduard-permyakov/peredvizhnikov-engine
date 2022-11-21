import lockfree_work;
import logger;
import assert;

import <cstdlib>;
import <array>;
import <future>;


constexpr int kNumSteps = 100;

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
};

void lfsw_worker(int i, pe::LockfreeSerialWork<Object>& work)
{
    work.PerformSerially(+[](Object& obj, int i) {
        obj.step(i);
    }, i);
}

void test_lfsw()
{
    Object test{0, 1, 2, 3, 4, 5};
    pe::LockfreeSerialWork work{test};
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

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::ioprint(pe::TextColor::eGreen, "Testing Lock-Free Serial Work.");
        test_lfsw();
        pe::ioprint(pe::TextColor::eGreen, "Finished Lock-Free Serial Work test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }

    return ret;
}

