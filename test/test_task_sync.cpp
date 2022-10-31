import sync;
import logger;

import <cstdlib>;
import <string>;

class LatchWorker : public pe::Task<
    void, LatchWorker, std::string&, pe::Latch&, pe::Latch&>
{
    using base = Task<void, LatchWorker, std::string&, pe::Latch&, pe::Latch&>;

    /* We can choose to use class members or arguments passed to Run */
    std::string m_name;

    virtual LatchWorker::handle_type Run(std::string& product,
        pe::Latch& work_done, pe::Latch& start_clean_up)
    {
        product = m_name + " worked";
        work_done.CountDown();
        co_await start_clean_up;
        product = m_name + " cleaned";
        co_return;
    }

public:

    LatchWorker(base::TaskCreateToken token, pe::Scheduler& scheduler, uint32_t priority, 
        bool initially_suspended, pe::Affinity affinity, std::string name)
        : base{token, scheduler, priority, initially_suspended, affinity}
        , m_name{name}
    {}
};

class LatchTester : public pe::Task<void, LatchTester>
{
    using Task<void, LatchTester>::Task;

    virtual LatchTester::handle_type Run()
    {
        struct Job{
            const std::string           m_name;
            std::string                 m_product{"Not worked"};
            pe::shared_ptr<LatchWorker> m_worker{};
        }jobs[] = {
            {"Annika"},
            {"Buru"},
            {"Chuck"},
        };

        pe::Latch work_done{Scheduler(), std::size(jobs)};
        pe::Latch start_clean_up{Scheduler(), 1};

        pe::dbgprint("Work starting...");
        for(auto& job : jobs) {
            job.m_worker = LatchWorker::Create(Scheduler(), 0, false, pe::Affinity::eAny,
                job.m_name, job.m_product, work_done, start_clean_up);
        }
        co_await work_done;
        pe::dbgprint("Done!");
        for(const auto& job : jobs) {
            pe::dbgprint("  " + job.m_product);
        }

        pe::dbgprint("Workers cleaning up...");
        start_clean_up.CountDown();
        for(const auto& job : jobs) {
            co_await job.m_worker->Join();
        }
        pe::dbgprint("Done!");
        for(const auto& job : jobs) {
            pe::dbgprint("  " + job.m_product);
        }
    }
};

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::Scheduler scheduler{};
        auto latch_test = LatchTester::Create(scheduler, 0);
        scheduler.Run();

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }

    return ret;
}

