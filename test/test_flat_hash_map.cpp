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

import flat_hash_map;
import logger;
import assert;
import meta;
import platform;

import <cstdlib>;
import <string>;
import <vector>;
import <utility>;
import <ranges>;
import <initializer_list>;
import <type_traits>;
import <unordered_map>;


constexpr std::size_t kNumElements = 1'000'000;

template <typename M, typename K, typename T>
concept HashMap = requires(M map, K key, T value)
{
    {map.insert(std::pair<K, T>{})} -> std::same_as<std::pair<typename M::iterator, bool>>;
    {map.erase(key)} -> std::same_as<typename M::size_type>;
    {map.size()} -> std::same_as<typename M::size_type>;
    {map.find(key)} -> std::same_as<typename M::iterator>;
    {map.end()} -> std::same_as<typename M::iterator>;
};

struct TrackedString
{
    static inline std::size_t s_nmoves{};
    static inline std::size_t s_ncopies{};
    static inline std::size_t s_string_construct{};
    static inline std::size_t s_char_construct{};

    std::string m_string{};

    TrackedString() = default;

    template <typename String>
    requires std::is_same_v<String, std::string>
    TrackedString(String&& s)
        : m_string{std::forward<String>(s)}
    {
        s_string_construct++;
    }

    TrackedString(const char *s)
        : m_string{s}
    {
        s_char_construct++;
    }

    TrackedString(TrackedString&& other)
    {
        m_string = std::move(other.m_string);
        s_nmoves++;
    }

    TrackedString& operator=(TrackedString&& other)
    {
        m_string = std::move(other.m_string);
        s_nmoves++;
        return *this;
    }

    TrackedString(TrackedString const& other)
    {
        m_string = other.m_string;
        s_ncopies++;
    }

    TrackedString& operator=(TrackedString const& other)
    {
        m_string = other.m_string;
        s_ncopies++;
        return *this;
    }
};

void test_api()
{
    /* Different flavors of constructors */
    pe::FlatHashMap<int, std::string> a{};
    pe::assert<true>(a.empty());

    std::vector<std::pair<std::string, double>> vec_input{
        {"abc",  12.0},
        {"def", -99.0},
        {"geh",  69.0}
    };
    pe::FlatHashMap b{vec_input.begin(), vec_input.end()};

    static_assert(std::is_same_v<decltype(b)::key_type, std::string>);
    static_assert(std::is_same_v<decltype(b)::mapped_type, double>);

    pe::assert<true>(b.size() == 3);
    pe::assert<true>(b["abc"] == 12.0);
    pe::assert<true>(b["def"] == -99.0);
    pe::assert<true>(b["geh"] == 69.0);

    pe::FlatHashMap c{std::ranges::views::all(vec_input) | std::ranges::views::take(2)};

    static_assert(std::is_same_v<decltype(c)::key_type, std::string>);
    static_assert(std::is_same_v<decltype(c)::mapped_type, double>);

    pe::assert<true>(c.size() == 2);
    pe::assert<true>(c["abc"] == 12);
    pe::assert<true>(c["def"] == -99.0);

    std::vector<std::pair<std::string, double>> lvalue_range{
        {"def", 69.0},
        {"geh", 99.0}
    };
    pe::FlatHashMap d{lvalue_range};

    pe::assert<true>(d.size() == 2);
    pe::assert<true>(d["def"] == 69.0);
    pe::assert<true>(d["geh"] == 99.0);

    pe::FlatHashMap e{{
        std::pair{std::string{"abc"},  12.0},
        std::pair{std::string{"def"}, -99.0},
        std::pair{std::string{"geh"},  69.0},
        std::pair{std::string{"ijk"}, 420.0}
    }};

    pe::assert<true>(e.size() == 4);
    pe::assert<true>(e["abc"] == 12.0);
    pe::assert<true>(e["def"] == -99.0);
    pe::assert<true>(e["geh"] == 69.0);
    pe::assert<true>(e["ijk"] == 420.0);

    /* different flavors of emplace */
    pe::FlatHashMap<std::string, std::string> f{};
    f.emplace(std::make_pair(std::string{"a"}, std::string{"000"}));
    f.emplace(std::make_pair("b", "111"));
    f.emplace("c", "222");
    f.emplace("d", 3, '3');

    pe::assert<true>(f.size() == 4);
    pe::assert<true>(f["a"] == "000");
    pe::assert<true>(f["b"] == "111");
    pe::assert<true>(f["c"] == "222");
    pe::assert<true>(f["d"] == "333");

    /* Ensure only the move constructor gets called when emplacing */
    std::vector<std::pair<int, TrackedString>> input;
    input.reserve(3);
    input.emplace_back(std::piecewise_construct,
                       std::forward_as_tuple(0), 
                       std::forward_as_tuple("A"));
    input.emplace_back(std::piecewise_construct,
                       std::forward_as_tuple(1), 
                       std::forward_as_tuple("B"));
    input.emplace_back(std::piecewise_construct, 
                       std::forward_as_tuple(2),
                       std::forward_as_tuple(std::string{"C"}));

    pe::FlatHashMap g{std::move(input)};

    pe::assert<true>(TrackedString::s_string_construct == 1);
    pe::assert<true>(TrackedString::s_char_construct == 2);
    pe::assert<true>(TrackedString::s_ncopies == 0);
    pe::assert<true>(TrackedString::s_nmoves == 3);

    g.emplace(3, "D");
    pe::assert<true>(TrackedString::s_char_construct == 3);
    pe::assert<true>(TrackedString::s_ncopies == 0);
    pe::assert<true>(TrackedString::s_nmoves == 3);

    g.emplace(std::make_pair(4, std::string{"E"}));
    pe::assert<true>(TrackedString::s_string_construct == 2);
    pe::assert<true>(TrackedString::s_ncopies == 0);
    pe::assert<true>(TrackedString::s_nmoves == 3);
    pe::assert<true>(g[4].m_string == "E");
    pe::assert<true>(g.size() == 5);

    /* Test iteration */
    pe::FlatHashMap<int, std::string> h{{
        std::pair{0, "ABC"},
        std::pair{1, "DEF"},
        std::pair{2, "GHI"}
    }};
    static_assert(std::ranges::range<decltype(h)>);
    static_assert(std::ranges::bidirectional_range<decltype(h)>);
    std::unordered_map<int, std::string> expected{{
        std::pair{0, "ABC"},
        std::pair{1, "DEF"},
        std::pair{2, "GHI"}
    }};
    std::unordered_map<int, std::string> results{};
    for(const auto& pair : h) {
        results.insert(pair);
    }
    pe::assert<true>(results == expected);
    results.clear();

    for(auto it = h.cbegin(); it != h.cend(); ++it) {
        results.insert(*it);
    }
    pe::assert<true>(results == expected);
    results.clear();

    for(const auto& pair : std::ranges::views::all(h) | std::ranges::views::reverse) {
        results.insert(pair);
    }
    pe::assert<true>(results == expected);
    results.clear();

    for(auto it = h.crbegin(); it != h.crend(); ++it) {
        results.insert(*it);
    }
    pe::assert<true>(results == expected);
    results.clear();

    pe::FlatHashMap<int, int> i{{
        std::pair{0, 0},
        std::pair{1, 1},
        std::pair{2, 2},
        std::pair{3, 3}
    }};
    auto double_up = [](const auto& pair) {
        return std::pair<int, int>(pair.first, pair.second * 2);
    };
    pe::FlatHashMap<int, int> copy{
        std::ranges::views::all(i) | std::ranges::views::transform(double_up)
    };
    pe::assert<true>(copy.size() == 4);
    pe::assert<true>(copy[0] == 0);
    pe::assert<true>(copy[1] == 2);
    pe::assert<true>(copy[2] == 4);
    pe::assert<true>(copy[3] == 6);

    /* Verify insert and subscipting behavior */
    pe::assert<true>(copy[4] == 0);
    pe::assert<true>(copy[5] == 0);
    auto result = copy.insert(std::make_pair(1, 69));
    pe::assert<true>(result.first != copy.end());
    pe::assert<true>(result.second == false);
    pe::assert<true>((*result.first).first == 1);
    pe::assert<true>((*result.first).second == 2);

    result = copy.emplace(2, 69);
    pe::assert<true>(result.first != copy.end());
    pe::assert<true>(result.second == false);
    pe::assert<true>((*result.first).first == 2);
    pe::assert<true>((*result.first).second == 4);

    auto a1 = copy.insert_or_assign(99, 99);
    pe::assert<true>(a1.second == true);
    pe::assert<true>((*a1.first).first == 99);
    pe::assert<true>((*a1.first).second == 99);

    auto a2 = copy.insert_or_assign(99, 66);
    pe::assert<true>(a2.second == false);
    pe::assert<true>((*a2.first).first == 99);
    pe::assert<true>((*a2.first).second == 66);

    bool caught = false;
    try{
        copy.at(128);
    }catch(std::out_of_range& e) {
        caught = true;
    }
    pe::assert<true>(caught);
    pe::assert<true>(copy.at(3) == 6);

    /* Test emplace_hint */
    pe::FlatHashMap<int, int> j{};
    auto inserted = j.emplace_hint(j.begin(), 5, 5);
    pe::assert<true>((*inserted).first == 5);
    pe::assert<true>((*inserted).second == 5);

    /* Test find, contains, erase */
    pe::FlatHashMap<int, std::string> k{{
        std::pair{0, "Value 001"},
        std::pair{1, "Value 002"},
        std::pair{2, "Value 003"},
        std::pair{3, "Value 004"},
        std::pair{4, "Value 005"}
    }};
    auto found = k.find(2);
    pe::assert<true>(found != k.end());
    pe::assert<true>((*found).first == 2);
    pe::assert<true>((*found).second == "Value 003");
    pe::assert<true>(k.contains(2));

    auto next = k.erase(found);
    pe::assert<true>(next != k.end());
    pe::assert<true>(k.find(2) == k.end());
    pe::assert<true>(!k.contains(2));

    k.erase(3);
    pe::assert<true>(k.find(3) == k.end());
    pe::assert<true>(k.size() == 3);
    k.erase(k.cbegin(), k.cend());
    pe::assert<true>(k.size() == 0);

    /* Test swap/rehash/clear */
    k.insert(k.begin(), std::make_pair(1, std::string{"One"}));
    k.insert(k.begin(), std::make_pair(2, std::string{"Two"}));
    k.rehash(256);
    pe::assert<true>(k.size() == 2);
    found = k.find(2);
    pe::assert<true>(found != k.end());
    pe::assert<true>((*found).second == "Two");

    pe::FlatHashMap<int, std::string> l{};
    l.swap(k);
    pe::assert<true>(k.size() == 0);
    auto found2 = l.find(2);
    pe::assert<true>(found2 != l.end());
    pe::assert<true>((*found2).second == "Two");

    l.clear();
    pe::assert<true>(l.size() == 0);

    /* Test copy/move/swap/equals */
    pe::FlatHashMap<float, std::string> m{{
        std::pair{1.0f, "1.0f"},
        std::pair{2.0f, "2.0f"},
        std::pair{3.0f, "3.0f"},
    }};
    pe::FlatHashMap<float, std::string> n{{
        std::pair{4.0f, "4.0f"},
        std::pair{5.0f, "5.0f"},
        std::pair{6.0f, "6.0f"},
    }};
    auto m_copy = m, n_copy = n;
    pe::assert<true>(m_copy == m);
    pe::assert<true>(n_copy == n);

    std::swap(m, n);
    pe::assert<true>(m.size() == 3 && m.size() == n.size());
    pe::assert<true>(n[1.0f] == "1.0f" && n[2.0f] == "2.0f" && n[3.0f] == "3.0f");
    pe::assert<true>(m[4.0f] == "4.0f" && m[5.0f] == "5.0f" && m[6.0f] == "6.0f");

    auto m_moved = std::move(m_copy);
    pe::assert<true>(m_copy.size() == 0, "");
    pe::assert<true>(m_moved.size() == 3, "");
    pe::assert<true>(m_moved[1.0f] == "1.0f" 
            && m_moved[2.0f] == "2.0f" 
            && m_moved[3.0f] == "3.0f");
}

template <HashMap<std::string, int> Map>
void test_insert(Map& map, std::vector<std::pair<std::string, int>> elements)
{
    for(const auto& pair : elements) {
        map.insert(pair);
    }
}

template <HashMap<std::string, int> Map>
void verify_insert(const Map& map, std::vector<std::pair<std::string, int>> elements)
{
    pe::assert<true>(map.size() == elements.size());
    for(const auto& pair : elements) {
        auto it = map.find(pair.first);
        pe::assert<true>(it != map.end());
        pe::assert<true>((*it).second == pair.second);
    }
}

template <HashMap<std::string, int> Map>
void test_erase(Map& map, std::vector<std::pair<std::string, int>> elements)
{
    for(const auto& pair : elements) {
        map.erase(pair.first);
    }
}

template <HashMap<std::string, int> Map>
void verify_erase(const Map& map)
{
    pe::assert<true>(map.size() == 0);
    std::size_t count = 0;
    for(const auto& kv : map) {
        (void)kv;
        count++;
    }
    pe::assert<true>(count == 0);
}

template <HashMap<std::string, int> Map>
void test_find(Map& map, std::vector<std::pair<std::string, int>> elements)
{
    for(const auto& pair : elements) {
        auto it = map.find(pair.first);
        pe::assert<true>(it != map.end());
    }
}

template <HashMap<std::string, int> Map>
void test_iterate(Map& map)
{
    for(auto& pair : map) {
        pair.second *= 2;
    }
}

template <HashMap<std::string, int> Map>
void verify_iterate(const Map& map, std::vector<std::pair<std::string, int>> elements)
{
    for(const auto& pair : elements) {
        auto it = map.find(pair.first);
        pe::assert<true>(it != map.end());
        pe::assert<true>((*it).second == pair.second * 2);
    }
}

std::string random_string(size_t length)
{
    auto randchar = []() -> char {
        const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        const std::size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

auto elements()
{
    std::unordered_map<std::string, int> set{};
    while(set.size() < kNumElements) {
        std::pair<std::string, int> next_elem{
            random_string(24),
            rand()
        };
        set.insert(next_elem);
    }
    std::vector<std::pair<std::string, int>> ret{
        std::begin(set),
        std::end(set)
    };
    pe::assert<true>(ret.size() == kNumElements);
    return ret;
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting flat hash map test.");
        test_api();

        auto inputs = elements();
        std::unordered_map<std::string, int> node_map{};
        pe::FlatHashMap<std::string, int> flat_map{};

        /* Benchmark insert */
        pe::dbgtime<true>([&](){
            test_insert(node_map, inputs);
        }, [&](uint64_t delta) {
            verify_insert(node_map, inputs);
            pe::dbgprint("std::unordered_map insertion test with", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        pe::dbgtime<true>([&](){
            test_insert(flat_map, inputs);
        }, [&](uint64_t delta) {
            verify_insert(flat_map, inputs);
            pe::dbgprint("FlatHashMap insertion test with", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        /* Benchmark find */
        pe::dbgtime<true>([&](){
            test_find(node_map, inputs);
        }, [&](uint64_t delta) {
            pe::dbgprint("std::unordered_map find test with", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        pe::dbgtime<true>([&](){
            test_find(node_map, inputs);
        }, [&](uint64_t delta) {
            pe::dbgprint("FlatHashMap find test with", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        /* Benchmark iterate */
        pe::dbgtime<true>([&](){
            test_iterate(node_map);
        }, [&](uint64_t delta) {
            verify_iterate(node_map, inputs);
            pe::dbgprint("std::unordered_map iteration test with", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        pe::dbgtime<true>([&](){
            test_iterate(flat_map);
        }, [&](uint64_t delta) {
            verify_iterate(flat_map, inputs);
            pe::dbgprint("FlatHashMap iteration test with", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        /* Benchmark erase */
        pe::dbgtime<true>([&](){
            test_erase(node_map, inputs);
        }, [&](uint64_t delta) {
            verify_erase(node_map);
            pe::dbgprint("std::unordered_map deletion test with", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        pe::dbgtime<true>([&](){
            test_erase(node_map, inputs);
        }, [&](uint64_t delta) {
            verify_erase(node_map);
            pe::dbgprint("FlatHashMap deletion test with", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.");
        });

        pe::ioprint(pe::TextColor::eGreen, "Finished flat hash map test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

