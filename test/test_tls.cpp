import tls;
import logger;
import assert;

import <future>;
import <cstdlib>;
import <iostream>;
import <string>;
import <chrono>;


constexpr int kWorkerCount = 16;

struct Object
{
    std::string m_name;
    int         m_count;

    ~Object()
    {
        using namespace std::string_literals;
        pe::dbgprint("Object", m_name, "("s + std::to_string(m_count) + ")"s,
            "is destroyed!");
    }
};

void worker(pe::TLSAllocation<Object>& tls, std::string name)
{
    tls.SetThreadSpecific(name, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    {
        auto obj = tls.GetThreadSpecific();
        pe::assert(obj->m_name == name, "unexpected name");
        pe::assert(obj->m_count == 0, "unexpected count");
        obj->m_count++;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    {
        auto obj = tls.GetThreadSpecific();
        pe::assert(obj->m_name == name, "unexpected name");
        pe::assert(obj->m_count == 1, "unexpected count");
    }
}

void test_tls()
{
    std::string names[kWorkerCount] = {
        "Jim",
        "Tim",
        "Mike",
        "Jake",
        "Alfonso",
        "Geronimo",
        "Kyle",
        "Bryan",
        "Bryce",
        "Luke",
        "Hans",
        "Timmy",
        "Ivan",
        "Scott",
        "Siddhartha",
        "Walter"
    };

    auto tls = pe::AllocTLS<Object>();
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < kWorkerCount; i++) {
        tasks.push_back(
            std::async(std::launch::async, worker, 
            std::ref(tls), names[i]));
    }
    for(const auto& task : tasks) {
        task.wait();
    }
}

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::ioprint(pe::TextColor::eGreen, "Testing Thread-Local Storage.");
        test_tls();
        pe::ioprint(pe::TextColor::eGreen, "Finished Thread-Local Storage test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }

    return ret;
}

