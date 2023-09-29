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

import bitwise_trie;
import logger;
import assert;
import platform;

import <cstdlib>;
import <string>;
import <optional>;
import <stack>;
import <vector>;
import <algorithm>;
import <bitset>;
import <random>;
import <unordered_set>;


constexpr std::size_t kNumElements = 10'000;

template <std::size_t N>
struct BitsetLess
{
    bool operator()(const std::bitset<N>& a, const std::bitset<N>& b)
    {
        for(int i = N-1; i >= 0; i--) {
            if (a[i] ^ b[i]) return b[i];
        }
        return false;
    }
};

template <typename KeyType, typename Compare = std::less<KeyType>>
void test_api()
{
    pe::BitwiseTrie<KeyType> trie{};
    KeyType keys[] = {
        KeyType{0b001},
        KeyType{0b010},
        KeyType{0b100},
        KeyType{0b111},
        KeyType{0b101},
        KeyType{uint32_t(0b1) << 16},
        KeyType{uint32_t(0b1) << 31},
        KeyType{0xffffffff}
    };

    for(int i = 0; i < std::size(keys); i++) {
        bool success = trie.Insert(keys[i]);
        pe::assert<true>(success);
    }
    pe::assert<true>(trie.Size() == std::size(keys));

    std::vector<KeyType> read{};
    for(KeyType key : trie) {
        read.push_back(key);
    }
    pe::assert<true>(read.size() == std::size(keys));
    std::sort(std::begin(keys), std::end(keys), Compare{});
    std::sort(std::begin(read), std::end(read), Compare{});
    for(int i = 0; i < std::size(keys); i++) {
        pe::assert<true>(read[i] == keys[i]);
    }

    for(int i = 0; i < std::size(keys); i++) {
        bool contains = trie.Get(keys[i]);
        pe::assert<true>(contains);
    }

    for(int i = 0; i < std::size(keys); i++) {
        bool removed = trie.Remove(keys[i]);
        pe::assert<true>(removed);
    }
    pe::assert<true>(trie.Size() == 0);
}

template <std::integral KeyType>
auto integral_elements(std::size_t n)
{
    std::unordered_set<KeyType> ret{};
    std::uniform_int_distribution<KeyType> dist{
        std::numeric_limits<KeyType>::min(), 
        std::numeric_limits<KeyType>::max()
    };
    std::default_random_engine re{};

    while(ret.size() < n) {
        ret.insert(dist(re));
    }
    return std::vector<KeyType>{std::begin(ret), std::end(ret)};
}

template <std::size_t N>
auto bitset_elements(std::size_t n)
{
    std::unordered_set<std::bitset<N>> ret{};
    std::uniform_int_distribution<int> dist{0, 1};
    std::default_random_engine re{};

    auto random_bitset = [&](){
        std::bitset<N> ret{};
        for(int i = 0; i < N; i++) {
            ret[i] = dist(re);
        }
        return ret;
    };

    while(ret.size() < n) {
        ret.insert(random_bitset());
    }
    return std::vector<std::bitset<N>>{std::begin(ret), std::end(ret)};
}

template <typename KeyType>
void test_insert(pe::BitwiseTrie<KeyType>& trie, std::vector<KeyType>& elements)
{
    for(const auto& key : elements) {
        trie.Insert(key);
    }
}

template <typename KeyType, typename Compare = std::less<KeyType>>
void verify_insert(pe::BitwiseTrie<KeyType>& trie, std::vector<KeyType>& elements)
{
    for(const auto& key : elements) {
        bool contains = trie.Get(key);
        pe::assert<true>(contains);
    }

    std::vector<KeyType> read{};
    for(KeyType key : trie) {
        read.push_back(key);
    }
    std::sort(std::begin(elements), std::end(elements), Compare{});
    std::sort(std::begin(read), std::end(read), Compare{});
    pe::assert<true>(read.size() == elements.size());
    pe::assert<true>(read == elements);
}

template <typename KeyType>
void test_remove(pe::BitwiseTrie<KeyType>& trie, std::vector<KeyType>& elements)
{
    for(const auto& key : elements) {
        trie.Remove(key);
    }
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting Bitwise Trie test.");

        test_api<uint32_t>();
        test_api<uint32_t>();
        test_api<uint64_t>();
        test_api<int64_t>();
        test_api<__int128>();
        test_api<std::bitset<256>, BitsetLess<256>>();
        test_api<std::bitset<1024>, BitsetLess<1024>>();

        auto u64_elements = integral_elements<uint64_t>(kNumElements);
        auto u128_elements = integral_elements<__int128>(kNumElements);
        auto b256_elements = bitset_elements<256>(kNumElements);

        pe::BitwiseTrie<uint64_t> trie64{};
        pe::BitwiseTrie<__int128> trie128{};
        pe::BitwiseTrie<std::bitset<256>> trie256{};

        /* Benchmark insert */
        pe::dbgtime<true>([&](){
            test_insert(trie64, u64_elements);
        }, [&](uint64_t delta) {
            verify_insert(trie64, u64_elements);
            pe::dbgprint("Insertion test with 64-bit keys and", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.",
                "(", pe::fmt::cat{}, pe::rdtsc_usec(delta) / (float)kNumElements,
                "us per element)");
        });

        pe::dbgtime<true>([&](){
            test_insert(trie128, u128_elements);
        }, [&](uint64_t delta) {
            verify_insert(trie128, u128_elements);
            pe::dbgprint("Insertion test with 128-bit keys and", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.",
                "(", pe::fmt::cat{}, pe::rdtsc_usec(delta) / (float)kNumElements,
                "us per element)");
        });

        pe::dbgtime<true>([&](){
            test_insert(trie256, b256_elements);
        }, [&](uint64_t delta) {
            verify_insert<std::bitset<256>, BitsetLess<256>>(trie256, b256_elements);
            pe::dbgprint("Insertion test with 256-bit keys and", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.",
                "(", pe::fmt::cat{}, pe::rdtsc_usec(delta) / (float)kNumElements,
                "us per element)");
        });

        /* Benchmark remove */
        pe::dbgtime<true>([&](){
            test_remove(trie64, u64_elements);
        }, [&](uint64_t delta) {
            pe::assert<true>(trie64.Size() == 0);
            pe::dbgprint("Deletion test with 64-bit keys and", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.",
                "(", pe::fmt::cat{}, pe::rdtsc_usec(delta) / (float)kNumElements,
                "us per element)");
        });

        pe::dbgtime<true>([&](){
            test_remove(trie128, u128_elements);
        }, [&](uint64_t delta) {
            pe::assert<true>(trie128.Size() == 0);
            pe::dbgprint("Deletion test with 128-bit keys and", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.",
                "(", pe::fmt::cat{}, pe::rdtsc_usec(delta) / (float)kNumElements,
                "us per element)");
        });

        pe::dbgtime<true>([&](){
            test_remove(trie256, b256_elements);
        }, [&](uint64_t delta) {
            pe::assert<true>(trie256.Size() == 0);
            pe::dbgprint("Deletion test with 256-bit keys and", 
                kNumElements, "value(s) took",
                pe::rdtsc_usec(delta), "microseconds.",
                "(", pe::fmt::cat{}, pe::rdtsc_usec(delta) / (float)kNumElements,
                "us per element)");
        });

        pe::ioprint(pe::TextColor::eGreen, "Finished Bitwise Trie test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

