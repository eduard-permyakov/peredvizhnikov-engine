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

import sync;
import logger;
import meta;
import event;

import <cstdlib>;
import <string>;
import <atomic>;
import <vector>;

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

    LatchWorker(base::TaskCreateToken token, pe::Scheduler& scheduler, pe::Priority priority, 
        pe::CreateMode mode, pe::Affinity affinity, std::string name)
        : base{token, scheduler, priority, mode, affinity}
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
            job.m_worker = LatchWorker::Create(Scheduler(), pe::Priority::eNormal,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny,
                job.m_name, job.m_product, work_done, start_clean_up);
        }
        co_await work_done;
        pe::dbgprint("Done!");
        for(const auto& job : jobs) {
            pe::dbgprint("  " + job.m_product);
        }

        pe::dbgprint("Workers cleaning up...");
        start_clean_up.CountDown();
        for(auto& job : jobs) {
            co_await job.m_worker;
        }
        pe::dbgprint("Done!");
        for(const auto& job : jobs) {
            pe::dbgprint("  " + job.m_product);
        }
    }
};

class BarrierWorker : public pe::Task<void, BarrierWorker, std::string_view, int, pe::Barrier&>
{
    using Task<void, BarrierWorker, std::string_view, int, pe::Barrier&>::Task;

    virtual BarrierWorker::handle_type Run(std::string_view name, 
        int num_shifts, pe::Barrier& sync_point)
    {
        for(int i = 0; i < num_shifts; i++) {
            pe::dbgprint(std::string{"  "} + name.data() + " worked shift", i + 1);
            co_await sync_point.ArriveAndWait();

            pe::dbgprint(std::string{"  "} + name.data() + " cleaned after shift", i + 1);
            if(i == num_shifts-1) {
                pe::dbgprint(std::string{"  "} + name.data() + " has gone home");
                sync_point.ArriveAndDrop();
            }else {
                co_await sync_point.ArriveAndWait();
            }
        }
    }
};

class BarrierTester : public pe::Task<void, BarrierTester>
{
    using Task<void, BarrierTester>::Task;

    virtual BarrierTester::handle_type Run()
    {
        struct Job{
            const std::string m_name;
            int m_num_shifts;
        }jobs[] = {
            {"Anil", 6},
            {"Busara", 4},
            {"Carl", 2}
        };
        int max_shifts = std::max_element(std::begin(jobs), std::end(jobs),
            [](const Job& a, const Job& b){
                return a.m_num_shifts < b.m_num_shifts;
            }
        )->m_num_shifts;
        auto on_completion = [max_shifts]() noexcept {
            pe::dbgprint("Done!");
            static std::atomic_int phase_count{0};
            if(phase_count++ % 2 == 0)
                pe::dbgprint("Cleaning up...");
            else if(phase_count < max_shifts)
                pe::dbgprint("Starting...");
        };

        pe::Barrier sync_point{Scheduler(), static_cast<uint16_t>(std::size(jobs)),
            on_completion};
		pe::dbgprint("Starting...");

		std::vector<pe::shared_ptr<BarrierWorker>> tasks;
		for(const auto& job : jobs) {
            tasks.push_back(BarrierWorker::Create(Scheduler(), pe::Priority::eNormal, 
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, job.m_name, job.m_num_shifts, sync_point));
		}
        for(auto& task : tasks) {
            co_await task;
        }
    }
};

class Tester : public pe::Task<void, Tester>
{
    using Task<void, Tester>::Task;

    virtual Tester::handle_type Run()
    {
        pe::ioprint(pe::TextColor::eGreen, "Testing Latch");
        auto latch_test = LatchTester::Create(Scheduler());
        co_await latch_test;

        pe::ioprint(pe::TextColor::eGreen, "Testing Barrier");
        auto barrier_test = BarrierTester::Create(Scheduler());
        co_await barrier_test;

        pe::ioprint(pe::TextColor::eGreen, "Testing Finished");
        Broadcast<pe::EventType::eQuit>();
    }
};

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::Scheduler scheduler{};
        auto tester = Tester::Create(scheduler);
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

