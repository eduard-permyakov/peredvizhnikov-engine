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

export module sync:worker_pool;

import tls;
import lockfree_deque;
import lockfree_queue;
import shared_ptr;
import assert;
import atomic_bitset;
import platform;
import logger;
import concurrency;

import <coroutine>;
import <optional>;
import <array>;
import <string>;
import <thread>;
import <vector>;

namespace pe{

export 
inline const std::thread::id g_main_thread_id = std::this_thread::get_id();

/* Forward declarations
 */
export class TaskBase;
export class Scheduler;
export void PushCurrThreadTask(Scheduler *sched, pe::shared_ptr<TaskBase> task);
export void PopCurrThreadTask(Scheduler *sched);

/*****************************************************************************/
/* PRIORITY                                                                  */
/*****************************************************************************/

export
enum class Priority
{
    eBackground,
    eLow,
    eNormal,
    eHigh,
    eCritical,
    eNumPriorities
};

constexpr std::size_t kNumPriorities = static_cast<std::size_t>(Priority::eNumPriorities);

/*****************************************************************************/
/* AFFINITY                                                                  */
/*****************************************************************************/

export
enum class Affinity
{
    eAny,
    eMainThread,
};

/*****************************************************************************/
/* SCHEDULABLE                                                               */
/*****************************************************************************/

struct Schedulable
{
    Priority           m_priority;
    pe::weak_ptr<void> m_handle;
    Affinity           m_affinity;
};

/*****************************************************************************/
/* COROUTINE                                                                 */
/*****************************************************************************/
/*
 * RAII wrapper for the native coroutine_handle type
 */
template <typename PromiseType>
class Coroutine
{
private:

    std::coroutine_handle<PromiseType> m_handle;
    std::string                        m_name;
    Scheduler                         *m_scheduler;
    pe::shared_ptr<TaskBase>         (*m_get_task)(std::coroutine_handle<void>);

    friend class Scheduler;
    friend class Worker;

    void Resume()
    {
        m_handle.resume();
    }

public:

    Coroutine(std::coroutine_handle<PromiseType> handle, std::string name)
        : m_handle{handle}
        , m_name{name}
        , m_scheduler{&handle.promise().Scheduler()}
        , m_get_task{+[](std::coroutine_handle<> handle){
            auto coro = std::coroutine_handle<PromiseType>::from_address(handle.address());
            return pe::static_pointer_cast<TaskBase>(coro.promise().Task());
        }}
    {}

    Coroutine(const Coroutine&) = delete;
    Coroutine& operator=(const Coroutine&) = delete;

    Coroutine(Coroutine&& other) noexcept
    {
        if(this == &other)
            return;
        if(m_handle)
            m_handle.destroy();
        std::swap(m_handle, other.m_handle);
        std::swap(m_name, other.m_name);
    }

    Coroutine& operator=(Coroutine&& other) noexcept
    {
        if(this == &other)
            return *this;
        if(m_handle)
            m_handle.destroy();
        std::swap(m_handle, other.m_handle);
        std::swap(m_name, other.m_name);
        return *this;
    }

    ~Coroutine()
    {
        if(m_handle) {
            m_handle.destroy();
        }
    }

    template <typename T = PromiseType>
    requires (!std::is_void_v<T>)
    T& Promise() const
    {
        return m_handle.promise();
    }

    const std::string Name() const
    {
        return m_name;
    }

    void PushCurrThreadTask()
    {
        pe::PushCurrThreadTask(m_scheduler, m_get_task(m_handle));
    }

    void PopCurrThreadTask()
    {
        pe::PopCurrThreadTask(m_scheduler);
    }
};

using UntypedCoroutine = Coroutine<void>;

template <typename PromiseType>
using SharedCoroutinePtr = pe::shared_ptr<Coroutine<PromiseType>>;


/*****************************************************************************/
/* WORKER POOL                                                               */
/*****************************************************************************/

export
class WorkerPool;

class Worker
{
private:

    /* 'stealable' is a subset of 'available', which excludes
     * those priorities for which we only have tasks that have
     * an affinity for the current worker's thread. 
     */
    std::array<LockfreeDeque<Schedulable>, kNumPriorities> m_tasks;
    AtomicBitset                                           m_available;
    AtomicBitset                                           m_stealable;
    WorkerPool&                                            m_pool;

    void quit();

public:

    Worker(WorkerPool& pool)
        : m_tasks{}
        , m_available{kNumPriorities}
        , m_stealable{kNumPriorities}
        , m_pool{pool}
    {}

    bool ClaimsHasStealableTaskWithPriority(Priority prio)
    {
        std::size_t bit = kNumPriorities - 1 - static_cast<std::size_t>(prio);
        return m_stealable.Test(bit, std::memory_order_acquire);
    }

    void SetHasStealableTaskWithPriority(Priority prio)
    {
        std::size_t bit = kNumPriorities - 1 - static_cast<std::size_t>(prio);
        m_stealable.Set(bit, std::memory_order_release);
    }

    void ClearHasStealableTaskWithPriority(Priority prio)
    {
        std::size_t bit = kNumPriorities - 1 - static_cast<std::size_t>(prio);
        m_stealable.Clear(bit, std::memory_order_release);
    }

    void SetHasTaskWithPriority(Priority prio)
    {
        std::size_t bit = kNumPriorities - 1 - static_cast<std::size_t>(prio);
        m_available.Set(bit, std::memory_order_release);
    }

    void ClearHasTaskWithPriority(Priority prio)
    {
        std::size_t bit = kNumPriorities - 1 - static_cast<std::size_t>(prio);
        m_available.Clear(bit, std::memory_order_release);
    }

    std::optional<std::size_t> FindHighestAvailablePriority()
    {
        return m_available.FindFirstSet();
    }

    std::optional<Schedulable> TryPop(Priority priority)
    {
        std::size_t prio = static_cast<std::size_t>(priority);
        auto ret = m_tasks[prio].PopLeft();
        if(!ret.has_value()) {
            ClearHasStealableTaskWithPriority(priority);
            ClearHasTaskWithPriority(priority);
        }
        return ret;
    }

    std::optional<Schedulable> TrySteal(Priority priority)
    {
        /* Don't clear the stealable/avaialble bits here,
         * as that should only be done by the owning worker
         * thread.
         */
        std::size_t prio = static_cast<std::size_t>(priority);
        return m_tasks[prio].PopRight();
    }

    void PushTask(Schedulable task);
    void Work();
};

export
class WorkerPool
{
private:

    TLSAllocation<Worker>      m_workers;
    pe::shared_ptr<Worker>     m_main_worker;
    LockfreeQueue<Schedulable> m_main_tasks;
    std::vector<std::thread>   m_worker_threads;
    AtomicBitset               m_priorities;
    std::atomic_flag           m_quit;
    std::atomic_uint           m_num_exited;

    using WorkerSet = std::vector<std::reference_wrapper<Worker>>;

    WorkerSet workers_claiming_task_with_prio(Priority prio)
    {
        WorkerSet ret;
        auto workers = m_workers.GetThreadPtrsSnapshot();
        for(const auto& worker : workers) {
            if(worker->ClaimsHasStealableTaskWithPriority(prio))
                ret.emplace_back(std::ref(*worker));
        }
        return ret;
    }

    std::optional<std::size_t> next_available_prio()
    {
        auto local_highest = m_workers.GetThreadSpecific(*this)->FindHighestAvailablePriority();
        auto global_highest  = m_priorities.FindFirstSet();

        if(local_highest && global_highest) {
            return {std::min(local_highest.value(), global_highest.value())};
        }else if(global_highest) {
            return global_highest;
        }else if(local_highest) {
            return local_highest;
        }
        return std::nullopt;
    }

public:

    WorkerPool(std::size_t num_workers)
        : m_workers{AllocTLS<Worker>()}
        , m_main_worker{m_workers.GetThreadSpecific(*this)}
        , m_main_tasks{}
        , m_worker_threads{}
        , m_priorities{kNumPriorities}
        , m_quit{}
    {
        for(int i = 0; i < num_workers; i++){
            auto workfn = [this]() {
                m_workers.GetThreadSpecific(*this)->Work();
            };
            m_worker_threads.emplace_back(workfn);
            SetThreadName(m_worker_threads[i], "worker-" + std::to_string(i));
        }
    }

    bool IsMainWorker(const Worker *worker) const
    {
        return (m_main_worker.get() == worker);
    }

    void PushMainTask(Schedulable sched)
    {
        m_main_tasks.Enqueue(sched);
    }

    void DrainMainTasksQueue()
    {
        pe::assert(m_workers.GetThreadSpecific(*this) == m_main_worker);
        while(auto task = m_main_tasks.Dequeue()) {
            m_main_worker->PushTask(task.value());
        }
    }

    std::optional<Schedulable> FindTask()
    {
        /* Exhaustively search local and global pools, attempting steals */
        auto first_set = next_available_prio();
        if(!first_set.has_value())
            return std::nullopt;

        std::size_t prio_bit = first_set.value();
        while(prio_bit < kNumPriorities) {

            /* First search for a local task to execute */
            Priority priority = static_cast<Priority>(kNumPriorities - 1 - prio_bit);
            auto task = m_workers.GetThreadSpecific(*this)->TryPop(priority);
            if(task.has_value())
                return task;

            /* Choose victim carefully and try to steal from there */
            for(const auto& victim_worker : workers_claiming_task_with_prio(priority)) {

                auto stolen_task = victim_worker.get().TrySteal(priority);
                if(!stolen_task.has_value())
                    continue;

                /* Try to find the first available task that doesn't have
                 * an affinity for the specific thread. 
                 */
                if(stolen_task.value().m_affinity == Affinity::eMainThread) {

                    std::vector<Schedulable> affine_tasks;
                    affine_tasks.push_back(stolen_task.value());

                    while(true) {
                        stolen_task = victim_worker.get().TrySteal(priority);
                        if(!stolen_task.has_value())
                            break;
                        if(stolen_task.value().m_affinity == Affinity::eMainThread) {
                            affine_tasks.push_back(stolen_task.value());
                            continue;
                        }
                        break;
                    }
                    for(const auto& task : affine_tasks) {
                        PushMainTask(task);
                    }
                    if(stolen_task.has_value()) {
                        return stolen_task;
                    }
                    continue;
                }
                return stolen_task;
            }

            /* Update global state, then try to search again */
            m_priorities.Clear(prio_bit);
            auto next = next_available_prio();
            if(!next.has_value())
                break;
            prio_bit = next.value();
        }
        return std::nullopt;
    }

    void PushTask(Schedulable task)
    {
        Priority priority = task.m_priority;
        std::size_t priority_bit = kNumPriorities - 1 - static_cast<std::size_t>(priority);

        if(task.m_affinity == Affinity::eMainThread) {
            PushMainTask(task);
        }else{
            m_workers.GetThreadSpecific(*this)->PushTask(task);
            m_priorities.Set(priority_bit);
        }
    }

    void PerformMainThreadWork()
    {
        pe::assert(m_workers.GetThreadSpecific(*this) == m_main_worker);
        m_main_worker->Work();
    }

    void Quiesce()
    {
        pe::assert(m_workers.GetThreadSpecific(*this) == m_main_worker);
        m_quit.test_and_set(std::memory_order_relaxed);

        for(auto& thread : m_worker_threads) {
            thread.join();
        }
    }

    bool ShouldQuit() const
    {
        return m_quit.test(std::memory_order_relaxed);
    }

    void ReportQuit()
    {
        m_num_exited.fetch_add(1, std::memory_order_release);
    }

    bool AllQuit()
    {
        const std::size_t nworkers = std::size(m_worker_threads);
        return (m_num_exited.load(std::memory_order_acquire) == nworkers);
    }
};

void Worker::quit()
{
    if(m_pool.IsMainWorker(this))
        return;

    m_pool.ReportQuit();
    /* Since workers are able to steal tasks from other threads,
     * we must be sure that none of the worker threads will be 
     * touching the queues before we destroy the threads.
     */
    Backoff backoff{10, 1'000, 5'000'000};
    while(!m_pool.AllQuit() && !backoff.TimedOut()) {
        backoff.BackoffMaybe();
    }
}

void Worker::PushTask(Schedulable task)
{
    std::size_t prio = static_cast<std::size_t>(task.m_priority);
    m_tasks[prio].PushLeft(task);

    SetHasTaskWithPriority(task.m_priority);
    if(!(m_pool.IsMainWorker(this) && task.m_affinity == Affinity::eMainThread)) {
        SetHasStealableTaskWithPriority(task.m_priority);
    }
}

void Worker::Work()
{
    Backoff backoff{10, 1'000, 0};
    while(true) {
        if(m_pool.ShouldQuit()) {
            quit();
            break;
        }
        if(m_pool.IsMainWorker(this)) {
            m_pool.DrainMainTasksQueue();
        }
        auto task = m_pool.FindTask();
        if(task.has_value()) {
            auto coro = pe::static_pointer_cast<UntypedCoroutine>(task.value().m_handle.lock());
            coro->PushCurrThreadTask();
            coro->Resume();
            coro->PopCurrThreadTask();
            backoff.Reset();
        }else{
            backoff.BackoffMaybe();
        }
    }
}

} //namespace pe

