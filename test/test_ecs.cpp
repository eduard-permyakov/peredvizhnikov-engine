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
import nvector;
import nmatrix;
import assert;
import logger;

import <cstdlib>;
import <array>;
import <iostream>;


using Position  = pe::StrongTypedef<pe::Vec3f>;
using Velocity  = pe::StrongTypedef<pe::Vec3f>;
using Transform = pe::StrongTypedef<pe::Mat4f>;
using Health    = pe::StrongTypedef<float>;

struct PlayerTag {};

enum class TreeType
{
    eBirch,
    eOak,
    eWillow
};

std::ostream& operator<<(std::ostream& stream, const TreeType& tree_type)
{
    switch(tree_type) {
    case TreeType::eBirch:
        stream << "Birch";
        break;
    case TreeType::eOak:
        stream << "Oak";
        break;
    case TreeType::eWillow:
        stream << "Willow";
        break;
    }
    return stream;
}

struct Player 
    : public pe::Entity<Player, pe::World<>>
    , public pe::WithComponent<Player, Position>
    , public pe::WithComponent<Player, Velocity>
    , public pe::WithComponent<Player, Transform>
    , public pe::WithComponent<Player, Health>
    , public pe::WithComponent<Player, PlayerTag>
{};

struct Tree
    : public pe::Entity<Tree, pe::World<>>
    , public pe::WithComponent<Tree, Position>
    , public pe::WithComponent<Tree, Transform>
    , public pe::WithComponent<Tree, TreeType>
{};

struct Ent
    : public pe::Entity<Ent, pe::World<>>
    , public pe::InheritComponents<Ent, Tree>
    , public pe::WithComponent<Ent, Velocity>
    , public pe::WithComponent<Ent, Health>
{};

template <>
struct pe::Default<Player, Health>
{
    static constexpr Health value = 250.0f;
};

template <>
struct pe::Default<Ent, Health>
{
    static constexpr Health value = 100.0f;
};

void test_ecs()
{
    Player player{};
    std::array<Tree, 4> trees{};
    std::array<Ent, 2> ents{};

    pe::assert<true>(player.HasComponent<Position>());
    pe::assert<true>(player.HasComponent<Velocity>());
    pe::assert<true>(player.HasComponent<Transform>());
    pe::assert<true>(player.HasComponent<Health>());
    pe::assert<true>(player.HasComponent<PlayerTag>());
    pe::assert<true>(!player.HasComponent<TreeType>());

    pe::assert<true>(player.Get<Health>() == pe::Default<Player, Health>::value);

    pe::Vec3f position{12.0, 12.0, 44.0};
    player.Set<Position>(position);
    auto read = player.Get<Position>();
    pe::assert<true>(read == position);

    for(int i = 0; i < std::size(trees); i++) {
        trees[i].Set<TreeType>(TreeType::eBirch);
        pe::assert<true>(trees[i].Get<TreeType>() == TreeType::eBirch);
    }
    for(int i = 0; i < std::size(ents); i++) {
        ents[i].Set<TreeType>(TreeType::eBirch);
        pe::assert<true>(ents[i].Get<TreeType>() == TreeType::eBirch);
    }

    pe::ioprint(pe::TextColor::eYellow, "Iterating positions:");
    for(auto [eid, position] : pe::components_view<pe::World<>, Position>()) {
        pe::dbgprint("  id:", eid, "position:", position);
    }

    pe::ioprint(pe::TextColor::eYellow, "Iterating positions and velocities:");
    for(auto [eid, position, velocity] : pe::components_view<pe::World<>, Position, Velocity>()) {
        pe::dbgprint("  id:", eid, "position:", position, "velocity:", velocity);
    }

    pe::ioprint(pe::TextColor::eYellow, "Iterating tree types:");
    for(auto [eid, tree_type] : pe::components_view<pe::World<>, TreeType>()) {
        pe::dbgprint("  id:", eid, "tree type:", tree_type);
    }
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting ECS test.");
        test_ecs();
        pe::ioprint(pe::TextColor::eGreen, "Finished ECS test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

