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

#include <sys/syscall.h>

import sync;
import event;
import logger;
import alloc;
import unistd;
import futex;

import <any>;
import <new>;
import <cstdlib>;
import <algorithm>;
import <vector>;
import <thread>;
import <chrono>;
import <filesystem>;
import <fstream>;
import <iterator>;
import <csignal>;
import <unordered_map>;
import <unordered_set>;

/*****************************************************************************/
/* Test parameters                                                           */
/*****************************************************************************/

constexpr std::size_t kNumNotifiers = 32;
constexpr std::size_t kNumMessengers = 32;
constexpr std::size_t kKillIntervalMsec = 5000;
constexpr std::size_t kMonitorReportIntervalMsec = 1000;
constexpr std::size_t kNotifyIntervalUsec = 500'000;
constexpr std::size_t kMessageIntervalUsec = 500'000;

/*****************************************************************************/
/* Allocation overrides - to force global usage of the lockfree allocator.   */
/*****************************************************************************/

void *operator new(std::size_t size) noexcept(false)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    return alloc.Allocate(size);
}

void *operator new[](std::size_t size) noexcept(false)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    return alloc.Allocate(size);
}

void *operator new(std::size_t size, std::align_val_t align) noexcept(false)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    return alloc.AllocateAligned(size, align);
}

void *operator new[](std::size_t size, std::align_val_t align) noexcept(false)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    return alloc.AllocateAligned(size, align);
}

void operator delete(void *ptr) noexcept
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.Free(ptr);
}

void operator delete[](void *ptr) noexcept
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.Free(ptr);
}

void operator delete(void* ptr, std::align_val_t align) noexcept
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.FreeAligned(ptr, align);
}

void operator delete[](void* ptr, std::align_val_t align) noexcept
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.FreeAligned(ptr, align);
}

void *malloc(size_t size)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    return alloc.Allocate(size);
}

void *calloc(size_t num, size_t size)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(num * size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    auto ret = alloc.Allocate(size);
    std::memset(ret, 0, size);
    return ret;
}

void *realloc(void *ptr, size_t size)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, __STDCPP_DEFAULT_NEW_ALIGNMENT__);
    auto ret = alloc.Allocate(size);
    if(ptr) {
        std::size_t copy_size = std::min(size, alloc.AllocationSize(ptr));
        std::memcpy(ret, ptr, copy_size);
        alloc.Free(ptr);
    }
    return ret;
}

void *memalign(size_t alignment, size_t size)
{
    if(alignment > pe::kMaxBlockSize)
        return nullptr;
    pe::Allocator& alloc = pe::Allocator::Instance();
    size = alloc.NextAlignedBlockSize(size, alignment);
    return alloc.Allocate(size);
}

int posix_memalign(void **memptr, size_t alignment, size_t size)
{
    if(size == 0) {
        *memptr = nullptr;
        return 0;
    }

    void *ret = memalign(alignment, size);
    if(!ret)
        return -ENOMEM;

    *memptr = ret;
    return 0;
}

void *aligned_alloc(size_t alignment, size_t size)
{
    return memalign(alignment, size);
}

void *valloc(size_t size)
{
    return memalign(getpagesize(), size);
}

void *pvalloc(size_t size)
{
    size_t page_size = getpagesize();
    size = std::ceil(size / ((float)page_size)) * page_size;
    return memalign(page_size, size);
}

size_t malloc_usable_size(void *ptr)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    return alloc.AllocationSize(ptr);
}

void free(void *ptr)
{
    pe::Allocator& alloc = pe::Allocator::Instance();
    alloc.Free(ptr);
}

/*****************************************************************************/
/* Monitor                                                                    */
/*****************************************************************************/

using TimestampType = std::chrono::time_point<std::chrono::steady_clock> ;

struct TaskAliveNotification
{
    pe::tid_t       m_tid;
    std::thread::id m_thread_id;
    TimestampType   m_timestamp;
};

struct RemoteTaskState
{
    enum class Type
    {
        eNotifier,
        eMessenger
    };

    TimestampType                       m_last_timestamp;
    std::unordered_set<std::thread::id> m_threads;
    Type                                m_type;
};

class Monitor : public pe::Task<void, Monitor>
{
    using Task<void, Monitor>::Task;
    using RemoteTaskStateMap = std::unordered_map<pe::tid_t, RemoteTaskState>;

    static constexpr std::size_t kMaxNotificationProcessingBatchSize = 100;
    static constexpr std::size_t kMaxMessageProcessingBatchSize = 100;

    void process_notifications(RemoteTaskStateMap& state, std::size_t& num_notifications)
    {
        int i = 0;
        while(auto event = PollEvent<pe::EventType::eUser>()) {

            auto notification = any_cast<TaskAliveNotification>(event->m_payload);
            num_notifications++;

            state[notification.m_tid].m_last_timestamp = notification.m_timestamp;
            state[notification.m_tid].m_threads.insert(notification.m_thread_id);
            state[notification.m_tid].m_type = RemoteTaskState::Type::eNotifier;

            if(i++ == kMaxNotificationProcessingBatchSize)
                break;
        }
    }

    void process_messages(RemoteTaskStateMap& state, std::size_t& num_messages)
    {
        int i = 0;
        while(auto message = PollMessage()) {

            Reply(message->m_sender.lock(), pe::Message{this->shared_from_this()});
            auto notification = any_cast<TaskAliveNotification>(message->m_payload);
            num_messages++;

            state[notification.m_tid].m_last_timestamp = notification.m_timestamp;
            state[notification.m_tid].m_threads.insert(notification.m_thread_id);
            state[notification.m_tid].m_type = RemoteTaskState::Type::eMessenger;

            if(i++ == kMaxMessageProcessingBatchSize)
                break;
        }
    }

    void report(RemoteTaskStateMap& state, std::size_t num_notifications, std::size_t num_messages)
    {
        std::unordered_set<std::thread::id> threads{};
        std::size_t num_notifiers{};
        std::size_t num_messengers{};

        for(const auto& [key, value]: state) {
            for(const auto& thread_id : value.m_threads) {
                threads.insert(thread_id);
            }
            switch(value.m_type) {
            case RemoteTaskState::Type::eNotifier:
                num_notifiers++;
                break;
            case RemoteTaskState::Type::eMessenger:
                num_messengers++;
                break;
            }
        }
        pe::ioprint(pe::TextColor::eBrightGreen, "Monitor detected",
            num_notifiers, "notifiers (", pe::fmt::cat{}, num_notifications, "notifications)",
            "and", 
            num_messengers, "messengers (", pe::fmt::cat{}, num_messages, "messages)",
            "running on", threads.size(), "threads.");
    }

    virtual Monitor::handle_type Run()
    {
        Subscribe<pe::EventType::eUser>();

        RemoteTaskStateMap state{};
        std::size_t num_notifications{};
        std::size_t num_messages{};
        TimestampType last_report_time = std::chrono::steady_clock::now();

        while(true) {

            process_notifications(state, num_notifications);
            process_messages(state, num_messages);

            auto now = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration_cast<std::chrono::microseconds>(now - last_report_time);

            if(delta >= std::chrono::milliseconds{kMonitorReportIntervalMsec}) {
                report(state, num_notifications, num_messages);
                state.clear();
                num_notifications = 0;
                num_messages = 0;
                last_report_time = now;
            }
            co_await Yield(Affinity());
        }
        co_return;
    }
};

/*****************************************************************************/
/* Messenger                                                                  */
/*****************************************************************************/

class Messenger : public pe::Task<void, Messenger>
{
private:

    using base = Task<void, Messenger>;
    using base::base;

    pe::weak_ptr<Monitor> m_monitor;

    virtual Messenger::handle_type Run()
    {
        while(true) {

            auto before = std::chrono::steady_clock::now();
            TaskAliveNotification notification{TID(), std::this_thread::get_id(), before};
            pe::Message msg{this->shared_from_this(), 0, notification};

            co_await Send(m_monitor.lock(), msg);

            auto after = std::chrono::steady_clock::now();
            auto delta = std::chrono::duration_cast<std::chrono::microseconds>(after - before);

            if(delta < std::chrono::microseconds{kMessageIntervalUsec}) {
                auto left = std::chrono::microseconds{kMessageIntervalUsec} - delta;
                co_await IO([left](){ std::this_thread::sleep_for(left); });
            }
        }
        co_return;
    }

public:

    Messenger(base::TaskCreateToken token, pe::Scheduler& scheduler, 
        pe::Priority priority, pe::CreateMode mode, pe::Affinity affinity, 
        pe::shared_ptr<Monitor> monitor)
        : base{token, scheduler, priority, mode, affinity}
        , m_monitor{monitor}
    {}
};

/*****************************************************************************/
/* Notifier                                                                   */
/*****************************************************************************/

class Notifier : public pe::Task<void, Notifier>
{
    using Task<void, Notifier>::Task;

    virtual Notifier::handle_type Run()
    {
        while(true) {

            auto now = std::chrono::steady_clock::now();
            TaskAliveNotification notification{TID(), std::this_thread::get_id(), now};

            pe::UserEvent event{0, notification};
            Broadcast<pe::EventType::eUser>(event);

            auto duration = std::chrono::microseconds{kNotifyIntervalUsec};
            co_await IO([duration](){ std::this_thread::sleep_for(duration); });
        }
    }
};

/*****************************************************************************/
/* Thread Killer                                                              */
/*****************************************************************************/

std::vector<int> get_worker_tids()
{
    pid_t pid = getpid();
    std::string path{"/proc/" + std::to_string(pid) + "/task"};
    std::filesystem::path procdir{path};
    std::vector<int> ret{};

    for(const auto& dir_entry : std::filesystem::directory_iterator{procdir}) {

        auto tid = dir_entry.path().filename().string();
        std::filesystem::path namepath{dir_entry.path() / "comm"};

        std::ifstream ifs{namepath, std::ios::in | std::ios::binary};
        std::string name{std::istreambuf_iterator<char>{ifs}, {}};
        name.pop_back(); /* trim newline */

        if(name.rfind("worker-", 0) == 0) {
            int tid = std::stoi(dir_entry.path().filename().string());
            ret.push_back(tid);
        }
    }
    return ret;
}

void sighandler(int)
{
    /* We know the signaled thread won't be
     * calling this, so it's safe to call,
     * even though it's not re-entrant.
     */
    pe::dbgprint("Ouch! Go on without me...");
    syscall(SYS_exit, 0);
}

void thread_killer()
{
    int tgid = getpid();
    signal(SIGSEGV, sighandler);
    pthread_setname_np(pthread_self(), "KILLER");

    for(auto tid : get_worker_tids()) {

        std::this_thread::sleep_for(std::chrono::milliseconds{kKillIntervalMsec});
        pe::dbgprint("Killing thread", tid, pe::fmt::cat{}, "...");
        tgkill(tgid, tid, SIGSEGV);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{kKillIntervalMsec});
    pe::ioprint(pe::TextColor::eGreen, "Kudos if the engine is still running... Done...");
    _exit(0);
}

/*****************************************************************************/
/* Top-level test logic.                                                     */
/*****************************************************************************/

void create_tasks(pe::Scheduler& scheduler)
{
    auto monitor = Monitor::Create(scheduler, pe::Priority::eCritical,
        pe::CreateMode::eLaunchAsync, pe::Affinity::eMainThread);

    for(int i = 0; i < kNumNotifiers; i++) {
        std::ignore = Notifier::Create(scheduler, pe::Priority::eNormal,
            pe::CreateMode::eLaunchAsync, pe::Affinity::eAny);
    }
    for(int i = 0; i < kNumMessengers; i++) {
        std::ignore = Messenger::Create(scheduler, pe::Priority::eNormal,
            pe::CreateMode::eLaunchAsync, pe::Affinity::eAny, monitor);
    }
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting Fault Tolerance testing...");

        pe::Scheduler scheduler{};
        create_tasks(scheduler);
        auto killer = std::thread{thread_killer};
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

