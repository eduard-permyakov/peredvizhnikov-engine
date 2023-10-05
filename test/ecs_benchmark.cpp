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

import ecs;
import assert;
import logger;
import sync;
import event;
import meta;

import <cstdlib>;
import <atomic>;
import <chrono>;
import <tuple>;
import <thread>;
import <array>;


constexpr std::chrono::microseconds kIterateBenchDuration{5'000'000};
constexpr std::chrono::microseconds kQueryBenchDuration{5'000'000};
constexpr std::size_t kEntitiesPerType = 10'000;

using BenchResult = std::tuple<std::chrono::microseconds, std::size_t>;

/*****************************************************************************/
/* Define Components                                                         */
/*****************************************************************************/

using ComponentA = pe::StrongTypedef<uint64_t>;
using ComponentB = pe::StrongTypedef<uint64_t>;
using ComponentC = pe::StrongTypedef<uint64_t>;
using ComponentD = pe::StrongTypedef<uint64_t>;
using ComponentE = pe::StrongTypedef<uint64_t>;
using ComponentF = pe::StrongTypedef<uint64_t>;
using ComponentG = pe::StrongTypedef<uint64_t>;
using ComponentH = pe::StrongTypedef<uint64_t>;
using ComponentI = pe::StrongTypedef<uint64_t>;
using ComponentJ = pe::StrongTypedef<uint64_t>;
using ComponentK = pe::StrongTypedef<uint64_t>;
using ComponentL = pe::StrongTypedef<uint64_t>;
using ComponentM = pe::StrongTypedef<uint64_t>;
using ComponentN = pe::StrongTypedef<uint64_t>;
using ComponentO = pe::StrongTypedef<uint64_t>;
using ComponentP = pe::StrongTypedef<uint64_t>;

/*****************************************************************************/
/* Define Entities                                                           */
/*****************************************************************************/

struct EntityA
    : public pe::Entity<EntityA, pe::World<>>
    , public pe::WithComponent<EntityA, ComponentA>
    , public pe::WithComponent<EntityA, ComponentB>
    , public pe::WithComponent<EntityA, ComponentC>
    , public pe::WithComponent<EntityA, ComponentD>
{};

struct EntityB
    : public pe::Entity<EntityB, pe::World<>>
    , public pe::WithComponent<EntityB, ComponentE>
    , public pe::WithComponent<EntityB, ComponentF>
    , public pe::WithComponent<EntityB, ComponentG>
    , public pe::WithComponent<EntityB, ComponentH>
{};

struct EntityC
    : public pe::Entity<EntityC, pe::World<>>
    , public pe::WithComponent<EntityC, ComponentI>
    , public pe::WithComponent<EntityC, ComponentJ>
    , public pe::WithComponent<EntityC, ComponentK>
    , public pe::WithComponent<EntityC, ComponentL>
{};

struct EntityD
    : public pe::Entity<EntityD, pe::World<>>
    , public pe::WithComponent<EntityD, ComponentM>
    , public pe::WithComponent<EntityD, ComponentN>
    , public pe::WithComponent<EntityD, ComponentO>
    , public pe::WithComponent<EntityD, ComponentP>
{};

struct EntityE
    : public pe::Entity<EntityE, pe::World<>>
    , public pe::WithComponent<EntityE, ComponentA>
    , public pe::WithComponent<EntityE, ComponentE>
    , public pe::WithComponent<EntityE, ComponentI>
    , public pe::WithComponent<EntityE, ComponentM>
{};

struct EntityF
    : public pe::Entity<EntityF, pe::World<>>
    , public pe::WithComponent<EntityF, ComponentB>
    , public pe::WithComponent<EntityF, ComponentF>
    , public pe::WithComponent<EntityF, ComponentJ>
    , public pe::WithComponent<EntityF, ComponentN>
{};

struct EntityG
    : public pe::Entity<EntityG, pe::World<>>
    , public pe::WithComponent<EntityG, ComponentC>
    , public pe::WithComponent<EntityG, ComponentG>
    , public pe::WithComponent<EntityG, ComponentK>
    , public pe::WithComponent<EntityG, ComponentO>
{};

struct EntityH
    : public pe::Entity<EntityG, pe::World<>>
    , public pe::WithComponent<EntityG, ComponentD>
    , public pe::WithComponent<EntityG, ComponentH>
    , public pe::WithComponent<EntityG, ComponentL>
    , public pe::WithComponent<EntityG, ComponentP>
{};

struct EntityI
    : public pe::Entity<EntityI, pe::World<>>
    , public pe::InheritComponents<EntityI, EntityA>
    , public pe::InheritComponents<EntityI, EntityB>
{};

struct EntityJ
    : public pe::Entity<EntityJ, pe::World<>>
    , public pe::InheritComponents<EntityJ, EntityC>
    , public pe::InheritComponents<EntityJ, EntityD>
{};

struct EntityK
    : public pe::Entity<EntityK, pe::World<>>
    , public pe::InheritComponents<EntityK, EntityE>
    , public pe::InheritComponents<EntityK, EntityF>
{};

struct EntityL
    : public pe::Entity<EntityL, pe::World<>>
    , public pe::InheritComponents<EntityL, EntityG>
    , public pe::InheritComponents<EntityL, EntityH>
{};

struct EntityM
    : public pe::Entity<EntityM, pe::World<>>
    , public pe::InheritComponents<EntityM, EntityI>
    , public pe::InheritComponents<EntityM, EntityJ>
{};

struct EntityN
    : public pe::Entity<EntityN, pe::World<>>
    , public pe::InheritComponents<EntityN, EntityK>
    , public pe::InheritComponents<EntityN, EntityL>
{};

struct EntityO
    : public pe::Entity<EntityO, pe::World<>>
    , public pe::InheritComponents<EntityO, EntityM>
    , public pe::InheritComponents<EntityO, EntityN>
{};

struct EntityP
    : public pe::Entity<EntityP, pe::World<>>
    , public pe::InheritComponents<EntityP, EntityI>
    , public pe::InheritComponents<EntityP, EntityJ>
    , public pe::InheritComponents<EntityP, EntityK>
    , public pe::InheritComponents<EntityP, EntityL>
    , public pe::InheritComponents<EntityP, EntityM>
    , public pe::InheritComponents<EntityP, EntityN>
    , public pe::InheritComponents<EntityP, EntityO>
{};

/*****************************************************************************/
/* Entity Set                                                                */
/*****************************************************************************/

template <typename... Components>
struct EntitySet
{
    std::tuple<std::array<Components, kEntitiesPerType>...> m_entities{};
};

using FullEntitySet = EntitySet<
    EntityA,
    EntityB,
    EntityC,
    EntityD,
    EntityE,
    EntityF,
    EntityG,
    EntityH,
    EntityI,
    EntityJ,
    EntityK,
    EntityL,
    EntityM,
    EntityN,
    EntityO
>;

/*****************************************************************************/
/* Component Iteration Benchmark                                             */
/*****************************************************************************/

template <typename Component>
class ComponentIterator : public pe::Task<void, 
                                          ComponentIterator<Component>, 
                                          std::atomic_flag&, 
                                          std::atomic_uint64_t&>
{
    using pe::Task<
        void, 
        ComponentIterator<Component>,
        std::atomic_flag&,
        std::atomic_uint64_t&
    >::Task;

    virtual typename ComponentIterator<Component>::handle_type 
    Run(std::atomic_flag& quit, std::atomic_uint64_t& count)
    {
        uint64_t curr = 0;
        while(!quit.test(std::memory_order_relaxed)) {

            uint64_t nitems = 0;
            for(auto&& [eid, component] : pe::components_view<pe::World<>, Component>()) {
                pe::assert<true>(uint64_t(component) == curr);
                component++;
                nitems++;
            }
            count.fetch_add(nitems, std::memory_order_relaxed);
            curr++;
            co_await this->Yield(this->Affinity());
        }
        co_return;
    }
};

template <typename... Components>
class ComponentIteratorMaster : public pe::Task<BenchResult, 
                                                ComponentIteratorMaster<Components...>, 
                                                std::chrono::microseconds>
{
    using pe::Task<BenchResult, 
                   ComponentIteratorMaster<Components...>, 
                   std::chrono::microseconds>::Task;

    virtual typename ComponentIteratorMaster<Components...>::handle_type 
    Run(std::chrono::microseconds duration)
    {
        std::atomic_uint64_t ncount{};
        std::atomic_flag done{};
        auto before = std::chrono::steady_clock::now();
        auto tasks = std::make_tuple(
            ComponentIterator<Components>::Create(
                this->Scheduler(), 
                pe::Priority::eNormal,
                pe::CreateMode::eLaunchAsync, 
                pe::Affinity::eAny, done, ncount)...
        );
        co_await this->IO([duration]{ std::this_thread::sleep_for(duration); });
        done.test_and_set(std::memory_order_relaxed);
        ((co_await std::get<pe::shared_ptr<ComponentIterator<Components>>>(tasks)), ...);

        auto after = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(after - before);
        co_return std::make_tuple(delta, ncount.load(std::memory_order_relaxed));
    }
};

/*****************************************************************************/
/* Component Query Benchmark                                                 */
/*****************************************************************************/

template <typename T>
void print_type()
{
    pe::dbgprint(__PRETTY_FUNCTION__);
}

template <typename Component>
class ComponentGetter : public pe::Task<void, 
                                        ComponentGetter<Component>, 
                                        FullEntitySet&,
                                        std::atomic_flag&, 
                                        std::atomic_uint64_t&>
{
    using pe::Task<
        void, 
        ComponentGetter<Component>,
        FullEntitySet&,
        std::atomic_flag&,
        std::atomic_uint64_t&
    >::Task;

    virtual typename ComponentGetter<Component>::handle_type 
    Run(FullEntitySet& set, std::atomic_flag& quit, std::atomic_uint64_t& count)
    {
        while(!quit.test(std::memory_order_relaxed)) {

            constexpr std::size_t ntypes = std::tuple_size_v<decltype(FullEntitySet{}.m_entities)>;
            pe::constexpr_for<0, ntypes, 1>([&]<std::size_t I>{

                if(quit.test(std::memory_order_relaxed))
                    return;

                const auto& subset = std::get<I>(set.m_entities);
                for(int j = 0; j < std::size(subset); j++) {

                    using entity_type = typename std::remove_cvref_t<decltype(subset)>::value_type;
                    constexpr bool has_component = requires(entity_type e){
                        {e.template Get<Component>()} -> std::same_as<Component>;
                    };
                    if constexpr(has_component) {
                        auto *e = const_cast<entity_type*>(&subset[j]);
                        [[maybe_unused]] auto c = e->template Get<Component>();
                        count.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
            co_await this->Yield(this->Affinity());
        }
        co_return;
    }
};

template <typename... Components>
class ComponentGetterMaster : public pe::Task<BenchResult, 
                                              ComponentGetterMaster<Components...>, 
                                              FullEntitySet&,
                                              std::chrono::microseconds>
{
    using pe::Task<BenchResult, 
                   ComponentGetterMaster<Components...>, 
                   FullEntitySet&,
                   std::chrono::microseconds>::Task;

    virtual typename ComponentGetterMaster<Components...>::handle_type 
    Run(FullEntitySet& set, std::chrono::microseconds duration)
    {
        std::atomic_uint64_t ncount{};
        std::atomic_flag done{};
        auto before = std::chrono::steady_clock::now();
        auto tasks = std::make_tuple(
            ComponentGetter<Components>::Create(
                this->Scheduler(), 
                pe::Priority::eNormal,
                pe::CreateMode::eLaunchAsync, 
                pe::Affinity::eAny, set, done, ncount)...
        );
        co_await this->IO([duration]{ std::this_thread::sleep_for(duration); });
        done.test_and_set(std::memory_order_relaxed);
        ((co_await std::get<pe::shared_ptr<ComponentGetter<Components>>>(tasks)), ...);

        auto after = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(after - before);
        co_return std::make_tuple(delta, ncount.load(std::memory_order_relaxed));
    }
};

/*****************************************************************************/
/* Top-level benchmarking logic                                              */
/*****************************************************************************/

class Tester : public pe::Task<void, Tester>
{
    using pe::Task<void, Tester>::Task;

    virtual Tester::handle_type Run()
    {
        pe::ioprint(pe::TextColor::eYellow, "Starting Creation benchmark...");
        auto before = std::chrono::steady_clock::now();

        [[maybe_unused]] FullEntitySet set{};

        auto after = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(after - before);
        auto seconds = delta.count() / 1'000'000.0f;
        auto nentities = 16 * kEntitiesPerType;

        pe::dbgprint("Created", nentities, "in",
            seconds, "secs (", pe::fmt::cat{}, nentities / seconds,
            "entities per second)");

        pe::ioprint(pe::TextColor::eYellow, "Starting Iteration benchmark...");
        {
            /* When there are read-write dependencies between different 
             * components, the taskgraph module can be used.
             */
            auto master = ComponentIteratorMaster<
                ComponentA,
                ComponentB,
                ComponentC,
                ComponentD,
                ComponentE,
                ComponentF,
                ComponentG,
                ComponentH,
                ComponentI,
                ComponentJ,
                ComponentK,
                ComponentL,
                ComponentM,
                ComponentN,
                ComponentO
            >::Create(
                Scheduler(), 
                pe::Priority::eHigh,
                pe::CreateMode::eLaunchAsync, 
                pe::Affinity::eAny, 
                kIterateBenchDuration);

            auto result = co_await master;
            auto seconds = std::get<0>(result).count() / 1'000'000.0f;
            auto naccesses = std::get<1>(result);

            pe::dbgprint(naccesses, "component accesses by", 16, "tasks in", 
                seconds, "secs (", pe::fmt::cat{}, naccesses / seconds,
                "accesses per second)");
        }

        pe::ioprint(pe::TextColor::eYellow, "Starting Query benchmark...");
        {
            /* Again, the taskgraph module can be used to 
             * specify read-write dependencies
             */
            auto master = ComponentGetterMaster<
                ComponentA,
                ComponentB,
                ComponentC,
                ComponentD,
                ComponentE,
                ComponentF,
                ComponentG,
                ComponentH,
                ComponentI,
                ComponentJ,
                ComponentK,
                ComponentL,
                ComponentM,
                ComponentN,
                ComponentO
            >::Create(
                Scheduler(), 
                pe::Priority::eHigh,
                pe::CreateMode::eLaunchAsync, 
                pe::Affinity::eAny, 
                set,
                kQueryBenchDuration);

            auto result = co_await master;
            auto seconds = std::get<0>(result).count() / 1'000'000.0f;
            auto naccesses = std::get<1>(result);

            pe::dbgprint(naccesses, "component accesses by", 16, "tasks in", 
                seconds, "secs (", pe::fmt::cat{}, naccesses / seconds,
                "accesses per second)");
        }

        Broadcast<pe::EventType::eQuit>();
        co_return;
    }
};

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting ECS benchmark.");

        pe::Scheduler scheduler{};
        auto tester = Tester::Create(scheduler);
        scheduler.Run();

        pe::ioprint(pe::TextColor::eGreen, "Finished ECS benchmark.");

    }catch(pe::TaskException &e){

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

