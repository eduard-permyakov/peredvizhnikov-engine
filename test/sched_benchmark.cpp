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
import event;
import assert;
import nmatrix;
import alloc;
import unistd;

import <new>;
import <cstdlib>;
import <chrono>;
import <memory>;
import <atomic>;
import <vector>;
import <thread>;
import <any>;
import <limits>;


constexpr std::chrono::microseconds kCPUBenchDuration{5'000'000};
constexpr std::chrono::microseconds kMessageBenchDuration{5'000'000};
constexpr std::chrono::microseconds kNotifyBenchDuration{5'000'000};

using BenchResult = std::tuple<std::chrono::microseconds, std::size_t>;

/*****************************************************************************/
/* CPU Scaling Benchmark                                                     */
/*****************************************************************************/

class CPUWorker : public pe::Task<void, CPUWorker, std::atomic_uint64_t&, std::atomic_flag&>
{
    using Task<void, CPUWorker, std::atomic_uint64_t&, std::atomic_flag&>::Task;

    static inline constexpr std::size_t kWorkBatchSize = 1000;

    __attribute__((optnone))
    void work()
    {
        auto a = std::make_unique_for_overwrite<pe::Mat4f[]>(kWorkBatchSize);
        auto b = std::make_unique_for_overwrite<pe::Mat4f[]>(kWorkBatchSize);
        auto c = std::make_unique_for_overwrite<pe::Mat4f[]>(kWorkBatchSize);

        for(int i = 0; i < kWorkBatchSize; i++) {
            c[i] = a[i] * b[i];
        }
    }

    virtual CPUWorker::handle_type Run(std::atomic_uint64_t& ncompleted, std::atomic_flag& done)
    {
        while(!done.test(std::memory_order_relaxed)) {
            work();
            ncompleted.fetch_add(kWorkBatchSize, std::memory_order_relaxed);
            co_await Yield(Affinity());
        }
    }
};

class CPUWorkMaster : public pe::Task<BenchResult, CPUWorkMaster, 
    std::size_t, std::chrono::microseconds>
{
    using Task<BenchResult, CPUWorkMaster, std::size_t, std::chrono::microseconds>::Task;

    virtual CPUWorkMaster::handle_type Run(std::size_t ntasks, std::chrono::microseconds duration)
    {
        std::atomic_uint64_t nmults{0};
        std::atomic_flag done{};
        std::vector<pe::shared_ptr<CPUWorker>> workers;

        auto before = std::chrono::steady_clock::now();
        for(int i = 0; i < ntasks; i++) {
            auto worker = CPUWorker::Create(Scheduler(), pe::Priority::eNormal,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, nmults, done);
            workers.push_back(worker);
        }
        co_await IO([duration]{ std::this_thread::sleep_for(duration); });
        done.test_and_set(std::memory_order_relaxed);
        for(int i = 0; i < ntasks; i++) {
            co_await workers[i];
        }
        auto after = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(after - before);
        co_return std::make_tuple(delta, nmults.load(std::memory_order_relaxed));
    }
};

/*****************************************************************************/
/* Messaging benchmark                                                       */
/*****************************************************************************/

class Receiver : public pe::Task<void, Receiver>
{
    using Task<void, Receiver>::Task;

    virtual Receiver::handle_type Run()
    {
        while(true) {
            auto msg = co_await Receive();
            Reply(msg.m_sender.lock(), pe::Message{this->shared_from_this(), 0, 0});
            if(msg.m_header == 0x1)
                co_return;
        }
    }
};

class Sender : public pe::Task<void, Sender, std::atomic_uint64_t&, std::atomic_flag&>
{
    using base = Task<void, Sender, std::atomic_uint64_t&, std::atomic_flag&>;
    using base::base;

    pe::weak_ptr<Receiver> m_receiver;

    virtual Sender::handle_type Run(std::atomic_uint64_t& nsends, std::atomic_flag& done)
    {
        while(!done.test(std::memory_order_relaxed)) {
            co_await Send(m_receiver.lock(), pe::Message{this->shared_from_this(), 0, 0});
            nsends.fetch_add(1, std::memory_order_relaxed);
        }
        co_await Send(m_receiver.lock(), pe::Message{this->shared_from_this(), 0x1, 0});
        nsends.fetch_add(1, std::memory_order_relaxed);
    }

public:

    Sender(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity, 
        pe::shared_ptr<Receiver> receiver)
        : base{token, scheduler, priority, mode, affinity}
        , m_receiver{receiver}
    {}
};

class SenderReceiverMaster : public pe::Task<BenchResult, SenderReceiverMaster, 
    std::size_t, std::chrono::microseconds>
{
    using Task<BenchResult, SenderReceiverMaster, std::size_t, std::chrono::microseconds>::Task;

    virtual SenderReceiverMaster::handle_type Run(std::size_t ntasks, 
        std::chrono::microseconds duration)
    {
        std::atomic_uint64_t nsends{0};
        std::atomic_flag done{};
        std::vector<pe::shared_ptr<Sender>> senders;
        std::vector<pe::shared_ptr<Receiver>> receivers;

        auto before = std::chrono::steady_clock::now();
        for(int i = 0; i < ntasks; i++) {
            
            auto receiver = Receiver::Create(Scheduler(), pe::Priority::eNormal,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny);
            auto sender = Sender::Create(Scheduler(), pe::Priority::eNormal,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, receiver, nsends, done);

            receivers.push_back(receiver);
            senders.push_back(sender);
        }
        co_await IO([duration]{ std::this_thread::sleep_for(duration); });
        done.test_and_set(std::memory_order_relaxed);
        for(int i = 0; i < ntasks; i++) {
            co_await senders[i];
            co_await receivers[i];
        }
        auto after = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(after - before);
        co_return std::make_tuple(delta, nsends.load(std::memory_order_relaxed));
    }
};

/*****************************************************************************/
/* Notification Benchmark                                                    */
/*****************************************************************************/

class EventProducer : public pe::Task<void, EventProducer, std::atomic_uint64_t&, std::atomic_flag&>
{
private:

    using base = Task<void, EventProducer, std::atomic_uint64_t&, std::atomic_flag&>;
    using base::base;

    uint32_t m_id;

    virtual EventProducer::handle_type Run(std::atomic_uint64_t& nnotifies, std::atomic_flag& done)
    {
        uint32_t curr = 1;
        while(!done.test(std::memory_order_relaxed)) {
            uint64_t qword = (static_cast<uint64_t>(m_id) << 32) | curr;
            Broadcast<pe::EventType::eNewFrame>(qword);
            nnotifies.fetch_add(1, std::memory_order_relaxed);
            curr++;
            co_await Yield(Affinity());
        }
        Broadcast<pe::EventType::eNewFrame>(std::numeric_limits<uint64_t>::max());
        nnotifies.fetch_add(1, std::memory_order_relaxed);
        co_return;
    }

public:

    EventProducer(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity, 
        uint32_t id)
        : base{token, scheduler, priority, mode, affinity}
        , m_id{id}
    {}
};

class EventConsumer : public pe::Task<void, EventConsumer, std::size_t>
{
    using Task<void, EventConsumer, std::size_t>::Task;

    virtual EventConsumer::handle_type Run(std::size_t nproducers)
    {
        Subscribe<pe::EventType::eNewFrame>();

        std::vector<uint64_t> counters(nproducers);
        std::fill(std::begin(counters), std::end(counters), 1);

        while(true) {

            uint64_t event = co_await Event<pe::EventType::eNewFrame>();
            uint32_t id = static_cast<uint32_t>(event >> 32);
            uint32_t seq = static_cast<uint32_t>(event);

            if(event == std::numeric_limits<uint64_t>::max())
                break;

            pe::assert(counters[id] == seq, "Unexpected event sequence number!");
            counters[id]++;
        }
        Unsubscribe<pe::EventType::eNewFrame>();
    }
};

class EventProducerConsumerMaster : public pe::Task<BenchResult, EventProducerConsumerMaster, 
    std::size_t, std::chrono::microseconds>
{
    using Task<BenchResult, EventProducerConsumerMaster, 
        std::size_t, std::chrono::microseconds>::Task;

    virtual EventProducerConsumerMaster::handle_type Run(std::size_t ntasks, 
        std::chrono::microseconds duration)
    {
        std::atomic_uint64_t nnotifies{0};
        std::atomic_flag done{};
        std::vector<pe::shared_ptr<EventProducer>> producers;
        std::vector<pe::shared_ptr<EventConsumer>> consumers;

        auto before = std::chrono::steady_clock::now();
        for(int i = 0; i < ntasks; i++) {
            auto consumer = EventConsumer::Create(Scheduler(), pe::Priority::eNormal,
                pe::CreateMode::eLaunchSync, pe::Affinity::eAny, ntasks);
            consumers.push_back(consumer);
        }
        for(int i = 0; i < ntasks; i++) {
            auto producer = EventProducer::Create(Scheduler(), pe::Priority::eNormal,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, static_cast<uint32_t>(i), 
                    nnotifies, done);
            producers.push_back(producer);
        }
        co_await IO([duration]{ std::this_thread::sleep_for(duration); });
        done.test_and_set(std::memory_order_relaxed);
        for(int i = 0; i < ntasks; i++) {
            co_await producers[i];
            co_await consumers[i];
        }
        auto after = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(after - before);
        co_return std::make_tuple(delta, nnotifies.load(std::memory_order_relaxed));
    }
};

/*****************************************************************************/
/* Task Creation Benchmark                                                   */
/*****************************************************************************/

class SimpleTask : public pe::Task<void, SimpleTask>
{
    using Task<void, SimpleTask>::Task;

    virtual SimpleTask::handle_type Run()
    {
        co_return;
    }
};

class TaskCreationMaster : public pe::Task<BenchResult, TaskCreationMaster, std::size_t>
{
    using Task<BenchResult, TaskCreationMaster, std::size_t>::Task;

    virtual TaskCreationMaster::handle_type Run(std::size_t ntasks)
    {
        std::vector<pe::shared_ptr<SimpleTask>> tasks;
        auto before = std::chrono::steady_clock::now();

        for(int i = 0; i < ntasks; i++) {
            tasks.push_back(SimpleTask::Create(Scheduler()));
        }
        auto after = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(after - before);

        Broadcast<pe::EventType::eNewFrame>(0);
        for(int i = 0; i < ntasks; i++) {
            co_await tasks[i];
        }
        co_return std::make_tuple(delta, 0);
    }
};

/*****************************************************************************/
/* Top-level benchmarking logic                                              */
/*****************************************************************************/

class Benchmarker : public pe::Task<void, Benchmarker>
{
    using Task<void, Benchmarker>::Task;

    virtual Benchmarker::handle_type Run()
    {
        pe::ioprint(pe::TextColor::eGreen, "Benchmarking scheduler...");

        pe::ioprint(pe::TextColor::eYellow, "Starting CPU benchmark...");
        std::size_t ntasks[] = {2, 4, 6, 8, 10, 12, 16, 24, 32};
        for(int i = 0; i < std::size(ntasks); i++) {
            const std::size_t n = ntasks[i];
            auto master = CPUWorkMaster::Create(Scheduler(), pe::Priority::eHigh,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, n, kCPUBenchDuration);
            auto result = co_await master;
            auto seconds = std::get<0>(result).count() / 1'000'000.0f;
            auto nmults = std::get<1>(result);
            pe::dbgprint(nmults, "matrix multiplications by", n, "tasks in", 
                seconds, "secs (", pe::fmt::cat{}, nmults / seconds,
                "multiplications per second)");
        }

        pe::ioprint(pe::TextColor::eYellow, "Starting message sending benchmark...");
        std::size_t nmsgpairs[] = {1, 2, 3, 4, 5, 6, 8, 12, 16, 18};
        for(int i = 0; i < std::size(nmsgpairs); i++) {
            const std::size_t n = nmsgpairs[i];
            auto master = SenderReceiverMaster::Create(Scheduler(), pe::Priority::eHigh,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, n, kMessageBenchDuration);
            auto result = co_await master;
            auto seconds = std::get<0>(result).count() / 1'000'000.0f;
            auto nsends = std::get<1>(result);
            pe::dbgprint(nsends, "messages sent by", n, "task(s) in", 
                seconds, "secs (", pe::fmt::cat{}, nsends / seconds,
                "messages per second)");
        }

        pe::ioprint(pe::TextColor::eYellow, "Starting notification benchmark...");
        std::size_t nnotifypairs[] = {1, 2, 3, 4, 5, 6, 8};
        for(int i = 0; i < std::size(nnotifypairs); i++) {
            const std::size_t n = nnotifypairs[i];
            auto master = EventProducerConsumerMaster::Create(Scheduler(), pe::Priority::eHigh,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, n, kNotifyBenchDuration);
            auto result = co_await master;
            auto seconds = std::get<0>(result).count() / 1'000'000.0f;
            auto nnotifies = std::get<1>(result);
            pe::dbgprint(nnotifies, "notifications sent by", n, "task(s) in", 
                seconds, "secs (", pe::fmt::cat{}, nnotifies / seconds,
                "notifications per second)");
        }

        pe::ioprint(pe::TextColor::eYellow, "Starting task creation benchmark...");
        std::size_t ncreated[] = {1'000, 10'000, 50'000, 100'000};
        for(int i = 0; i < std::size(ncreated); i++) {
            const std::size_t n = ncreated[i];
            auto master = TaskCreationMaster::Create(Scheduler(), pe::Priority::eHigh,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, n);
            auto result = co_await master;
            auto seconds = std::get<0>(result).count() / 1'000'000.0f;
            pe::dbgprint(n, "tasks created in",
                seconds, "secs (", pe::fmt::cat{}, n / seconds,
                "tasks per second)");
        }

        pe::ioprint(pe::TextColor::eGreen, "Benchmarking finished");
        Broadcast<pe::EventType::eQuit>();
        co_return;
    }
};

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::Scheduler scheduler{};
        auto tester = Benchmarker::Create(scheduler);
        scheduler.Run();

    }catch(pe::TaskException &e) {

        e.Print();
        ret = EXIT_FAILURE;

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

