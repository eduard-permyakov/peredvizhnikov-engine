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
import taskgraph;
import logger;
import event;
import assert;

import <cstdlib>;
import <exception>;


constexpr std::size_t kNumPhases = 10'000;

/* Implicitly create a DAG of tasks:
 * 
 *     +--> B +--+
 *    /           \
 * A +             +-> E +--> F +->
 *    \           /           ^
 *     +--> C +--+           /
 *          +               /
 *           \             /
 *            +---+ D +---+
 *
 * G +--------> H +--------------->
 *
 */

struct AOutput{};
struct BOutput{};
struct COutput{};
struct DOutput{};
struct EOutput{};
struct FOutput{};
struct GOutput{};
struct HOutput{};

struct PhaseCompletionCounts
{
    size_t m_a_completed;
    size_t m_b_completed;
    size_t m_c_completed;
    size_t m_d_completed;
    size_t m_e_completed;
    size_t m_f_completed;
    size_t m_g_completed;
    size_t m_h_completed;
};

class A : public pe::Task<pe::PhaseCompletion, A, PhaseCompletionCounts&>
        , pe::Writes<AOutput, A>
{
    using Task<pe::PhaseCompletion, A, PhaseCompletionCounts&>::Task;

    virtual A::handle_type Run(PhaseCompletionCounts& counts)
    {
        int counter = 0;
        while(true) {
            pe::assert(counts.m_b_completed == counter);
            pe::assert(counts.m_c_completed == counter);
            pe::assert(counts.m_d_completed == counter);
            pe::assert(counts.m_e_completed == counter);
            pe::assert(counts.m_f_completed == counter);
            counts.m_a_completed = ++counter;

            co_yield pe::PhaseCompleted;
        }
    }
};

class B : public pe::Task<pe::PhaseCompletion, B, PhaseCompletionCounts&>
        , pe::Reads<AOutput, B>
        , pe::Writes<BOutput, B>
{
    using Task<pe::PhaseCompletion, B, PhaseCompletionCounts&>::Task;

    virtual B::handle_type Run(PhaseCompletionCounts& counts)
    {
        int counter = 0;
        while(true) {
            pe::assert(counts.m_a_completed == counter + 1);
            pe::assert(counts.m_e_completed == counter);
            pe::assert(counts.m_f_completed == counter);
            counts.m_b_completed = ++counter;

            co_yield pe::PhaseCompleted;
        }
    }
};

class C : public pe::Task<pe::PhaseCompletion, C, PhaseCompletionCounts&>
        , pe::Reads<AOutput, C>
        , pe::Writes<COutput, C>
{
    using Task<pe::PhaseCompletion, C, PhaseCompletionCounts&>::Task;

    virtual C::handle_type Run(PhaseCompletionCounts& counts)
    {
        int counter = 0;
        while(true) {
            pe::assert(counts.m_a_completed == counter + 1);
            pe::assert(counts.m_d_completed == counter);
            pe::assert(counts.m_e_completed == counter);
            pe::assert(counts.m_f_completed == counter);
            counts.m_c_completed = ++counter;

            co_yield pe::PhaseCompleted;
        }
    }
};

class D : public pe::Task<pe::PhaseCompletion, D, PhaseCompletionCounts&>
        , pe::Reads<COutput, D>
        , pe::Writes<DOutput, D>
{
    using Task<pe::PhaseCompletion, D, PhaseCompletionCounts&>::Task;

    virtual D::handle_type Run(PhaseCompletionCounts& counts)
    {
        int counter = 0;
        while(true) {
            pe::assert(counts.m_a_completed == counter + 1);
            pe::assert(counts.m_c_completed == counter + 1);
            pe::assert(counts.m_f_completed == counter);
            counts.m_d_completed = ++counter;

            co_yield pe::PhaseCompleted;
        }
    }
};

class E : public pe::Task<pe::PhaseCompletion, E, PhaseCompletionCounts&>
        , pe::Reads<BOutput, E>
        , pe::Reads<COutput, E>
        , pe::Writes<EOutput, E>
{
    using Task<pe::PhaseCompletion, E, PhaseCompletionCounts&>::Task;

    virtual E::handle_type Run(PhaseCompletionCounts& counts)
    {
        int counter = 0;
        while(true) {
            pe::assert(counts.m_a_completed == counter + 1);
            pe::assert(counts.m_b_completed == counter + 1);
            pe::assert(counts.m_c_completed == counter + 1);
            pe::assert(counts.m_f_completed == counter);
            counts.m_e_completed = ++counter;

            co_yield pe::PhaseCompleted;
        }
    }
};

class F : public pe::Task<pe::PhaseCompletion, F, PhaseCompletionCounts&>
        , pe::Reads<DOutput, F>
        , pe::Reads<EOutput, F>
        , pe::Writes<FOutput, F>
{
    using Task<pe::PhaseCompletion, F, PhaseCompletionCounts&>::Task;

    virtual F::handle_type Run(PhaseCompletionCounts& counts)
    {
        int counter = 0;
        while(true) {
            pe::assert(counts.m_a_completed == counter + 1);
            pe::assert(counts.m_b_completed == counter + 1);
            pe::assert(counts.m_c_completed == counter + 1);
            pe::assert(counts.m_d_completed == counter + 1);
            pe::assert(counts.m_e_completed == counter + 1);
            counts.m_f_completed = ++counter;

            co_yield pe::PhaseCompleted;
        }
    }
};

class G : public pe::Task<pe::PhaseCompletion, G, PhaseCompletionCounts&>
        , pe::Writes<GOutput, G>
{
    using Task<pe::PhaseCompletion, G, PhaseCompletionCounts&>::Task;

    virtual G::handle_type Run(PhaseCompletionCounts& counts)
    {
        int counter = 0;
        while(true) {
            pe::assert(counts.m_h_completed == counter);
            counts.m_g_completed = ++counter;

            co_yield pe::PhaseCompleted;
        }
    }
};

class H : public pe::Task<pe::PhaseCompletion, H, PhaseCompletionCounts&>
        , pe::Reads<GOutput, H>
        , pe::Writes<HOutput, H>
{
    using Task<pe::PhaseCompletion, H, PhaseCompletionCounts&>::Task;

    virtual H::handle_type Run(PhaseCompletionCounts& counts)
    {
        int counter = 0;
        while(true) {
            pe::assert(counts.m_g_completed == counter + 1);
            counts.m_h_completed = ++counter;

            co_yield pe::PhaseCompleted;
        }
    }
};

class TaskGraphTester : public pe::Task<void, TaskGraphTester>
{
    using Task<void, TaskGraphTester>::Task;

    virtual TaskGraphTester::handle_type Run()
    {
        pe::ioprint(pe::TextColor::eGreen, "Testing TaskGraph...");

        PhaseCompletionCounts counts{};
        pe::TaskGraph graph{
            Scheduler(),
            pe::make_task_create_info<A>(pe::Priority::eNormal, pe::Affinity::eAny, counts),
            pe::make_task_create_info<B>(pe::Priority::eNormal, pe::Affinity::eAny, counts),
            pe::make_task_create_info<C>(pe::Priority::eNormal, pe::Affinity::eAny, counts),
            pe::make_task_create_info<D>(pe::Priority::eNormal, pe::Affinity::eAny, counts),
            pe::make_task_create_info<E>(pe::Priority::eNormal, pe::Affinity::eAny, counts),
            pe::make_task_create_info<F>(pe::Priority::eNormal, pe::Affinity::eAny, counts),
            pe::make_task_create_info<G>(pe::Priority::eNormal, pe::Affinity::eAny, counts),
            pe::make_task_create_info<H>(pe::Priority::eNormal, pe::Affinity::eAny, counts),
        };
        for(int i = 0; i < kNumPhases; i++) {
            co_await graph.RunPhase();
        }
        co_await graph.Exit();

        pe::assert(counts.m_a_completed == kNumPhases);
        pe::assert(counts.m_b_completed == kNumPhases);
        pe::assert(counts.m_c_completed == kNumPhases);
        pe::assert(counts.m_d_completed == kNumPhases);
        pe::assert(counts.m_e_completed == kNumPhases);
        pe::assert(counts.m_f_completed == kNumPhases);
        pe::assert(counts.m_g_completed == kNumPhases);
        pe::assert(counts.m_h_completed == kNumPhases);

        pe::ioprint(pe::TextColor::eGreen, "Testing TaskGraph finished");
        Broadcast<pe::EventType::eQuit>();
        co_return;
    }
};

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::Scheduler scheduler{};
        auto tester = TaskGraphTester::Create(scheduler);
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

