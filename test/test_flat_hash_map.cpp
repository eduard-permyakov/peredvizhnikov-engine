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

import <cstdlib>;
import <string>;
import <vector>;
import <utility>;
import <ranges>;
import <initializer_list>;
import <type_traits>;
import <unordered_map>;


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
    pe::assert(a.empty());

    std::vector<std::pair<std::string, double>> vec_input{
        {"abc",  12.0},
        {"def", -99.0},
        {"geh",  69.0}
    };
    pe::FlatHashMap b{vec_input.begin(), vec_input.end()};

    static_assert(std::is_same_v<decltype(b)::key_type, std::string>);
    static_assert(std::is_same_v<decltype(b)::mapped_type, double>);

    pe::assert(b.size() == 3);
    pe::assert(b["abc"] == 12.0);
    pe::assert(b["def"] == -99.0);
    pe::assert(b["geh"] == 69.0);

    pe::FlatHashMap c{std::ranges::views::all(vec_input) | std::ranges::views::take(2)};

    static_assert(std::is_same_v<decltype(c)::key_type, std::string>);
    static_assert(std::is_same_v<decltype(c)::mapped_type, double>);

    pe::assert(c.size() == 2);
    pe::assert(c["abc"] == 12);
    pe::assert(c["def"] == -99.0);

    std::vector<std::pair<std::string, double>> lvalue_range{
        {"def", 69.0},
        {"geh", 99.0}
    };
    pe::FlatHashMap d{lvalue_range};

    pe::assert(d.size() == 2);
    pe::assert(d["def"] == 69.0);
    pe::assert(d["geh"] == 99.0);

    pe::FlatHashMap e{{
        std::pair{std::string{"abc"},  12.0},
        std::pair{std::string{"def"}, -99.0},
        std::pair{std::string{"geh"},  69.0},
        std::pair{std::string{"ijk"}, 420.0}
    }};

    pe::assert(e.size() == 4);
    pe::assert(e["abc"] == 12.0);
    pe::assert(e["def"] == -99.0);
    pe::assert(e["geh"] == 69.0);
    pe::assert(e["ijk"] == 420.0);

    /* different flavors of emplace */
    pe::FlatHashMap<std::string, std::string> f{};
    f.emplace(std::make_pair(std::string{"a"}, std::string{"000"}));
    f.emplace(std::make_pair("b", "111"));
    f.emplace("c", "222");
    f.emplace("d", 3, '3');

    pe::assert(f.size() == 4);
    pe::assert(f["a"] == "000");
    pe::assert(f["b"] == "111");
    pe::assert(f["c"] == "222");
    pe::assert(f["d"] == "333");

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

    pe::assert(TrackedString::s_string_construct == 1);
    pe::assert(TrackedString::s_char_construct == 2);
    pe::assert(TrackedString::s_ncopies == 0);
    pe::assert(TrackedString::s_nmoves == 3);

    g.emplace(3, "D");
    pe::assert(TrackedString::s_char_construct == 3);
    pe::assert(TrackedString::s_ncopies == 0);
    pe::assert(TrackedString::s_nmoves == 3);

    g.emplace(std::make_pair(4, std::string{"E"}));
    pe::assert(TrackedString::s_string_construct == 2);
    pe::assert(TrackedString::s_ncopies == 0);
    pe::assert(TrackedString::s_nmoves == 3);
    pe::assert(g[4].m_string == "E");
    pe::assert(g.size() == 5);

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
    pe::assert(results == expected);
    results.clear();

    for(auto it = h.cbegin(); it != h.cend(); ++it) {
        results.insert(*it);
    }
    pe::assert(results == expected);
    results.clear();

    for(const auto& pair : std::ranges::views::all(h) | std::ranges::views::reverse) {
        results.insert(pair);
    }
    pe::assert(results == expected);
    results.clear();

    for(auto it = h.crbegin(); it != h.crend(); ++it) {
        results.insert(*it);
    }
    pe::assert(results == expected);
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
    pe::assert(copy.size() == 4);
    pe::assert(copy[0] == 0);
    pe::assert(copy[1] == 2);
    pe::assert(copy[2] == 4);
    pe::assert(copy[3] == 6);

    /* Verify insert and subscipting behavior */
    pe::assert(copy[4] == 0);
    pe::assert(copy[5] == 0);
    auto result = copy.insert(std::make_pair(1, 69));
    pe::assert(result.first != copy.end());
    pe::assert(result.second == false);
    pe::assert((*result.first).first == 1);
    pe::assert((*result.first).second == 2);

    result = copy.emplace(2, 69);
    pe::assert(result.first != copy.end());
    pe::assert(result.second == false);
    pe::assert((*result.first).first == 2);
    pe::assert((*result.first).second == 4);

    auto a1 = copy.insert_or_assign(99, 99);
    pe::assert(a1.second == true);
    pe::assert((*a1.first).first == 99);
    pe::assert((*a1.first).second == 99);

    auto a2 = copy.insert_or_assign(99, 66);
    pe::assert(a2.second == false);
    pe::assert((*a2.first).first == 99);
    pe::assert((*a2.first).second == 66);

    bool caught = false;
    try{
        copy.at(128);
    }catch(std::out_of_range& e) {
        caught = true;
    }
    pe::assert(caught);
    pe::assert(copy.at(3) == 6);

    /* Test emplace_hint */
    pe::FlatHashMap<int, int> j{};
    auto inserted = j.emplace_hint(j.begin(), 5, 5);
    pe::assert((*inserted).first == 5);
    pe::assert((*inserted).second == 5);

    /* Test find, contains, erase */
    pe::FlatHashMap<int, std::string> k{{
        std::pair{0, "Value 001"},
        std::pair{1, "Value 002"},
        std::pair{2, "Value 003"},
        std::pair{3, "Value 004"},
        std::pair{4, "Value 005"}
    }};
    auto found = k.find(2);
    pe::assert(found != k.end());
    pe::assert((*found).first == 2);
    pe::assert((*found).second == "Value 003");
    pe::assert(k.contains(2));

    auto next = k.erase(found);
    pe::assert(next != k.end());
    pe::assert(k.find(2) == k.end());
    pe::assert(!k.contains(2));

    k.erase(3);
    pe::assert(k.find(3) == k.end());
    pe::assert(k.size() == 3);
    k.erase(k.cbegin(), k.cend());
    pe::assert(k.size() == 0);

    /* Test swap/rehash/clear */
    k.insert(k.begin(), std::make_pair(1, std::string{"One"}));
    k.insert(k.begin(), std::make_pair(2, std::string{"Two"}));
    k.rehash(256);
    pe::assert(k.size() == 2);
    found = k.find(2);
    pe::assert(found != k.end());
    pe::assert((*found).second == "Two");

    pe::FlatHashMap<int, std::string> l{};
    l.swap(k);
    pe::assert(k.size() == 0);
    auto found2 = l.find(2);
    pe::assert(found2 != l.end());
    pe::assert((*found2).second == "Two");

    l.clear();
    pe::assert(l.size() == 0);

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
    pe::assert(m_copy == m);
    pe::assert(n_copy == n);

    std::swap(m, n);
    pe::assert(m.size() == 3 && m.size() == n.size());
    pe::assert(n[1.0f] == "1.0f" && n[2.0f] == "2.0f" && n[3.0f] == "3.0f");
    pe::assert(m[4.0f] == "4.0f" && m[5.0f] == "5.0f" && m[6.0f] == "6.0f");

    auto m_moved = std::move(m_copy);
    pe::assert(m_copy.size() == 0, "", __FILE__, __LINE__);
    pe::assert(m_moved.size() == 3, "", __FILE__, __LINE__);
    pe::assert(m_moved[1.0f] == "1.0f" 
            && m_moved[2.0f] == "2.0f" 
            && m_moved[3.0f] == "3.0f");
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting flat hash map test.");
        test_api();
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

