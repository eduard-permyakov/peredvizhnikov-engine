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

import atomic_bitset;
import assert;
import logger;

import <cstdlib>;
import <limits>;
import <future>;
import <optional>;
import <vector>;


constexpr int kBitsetSize = 16384;

void setter(pe::AtomicBitset& bitset)
{
    std::size_t size = bitset.Size();
    std::size_t num_set = 0, left_set = 0, right_set = 0;

    std::size_t mid = size / 2;
    if((mid > 0) && (size % 2 == 0))
        mid = mid - 1;

    while(num_set < size) {
        if(num_set % 2 == 0) {
            bitset.Set(mid - left_set);
            left_set++;
        }else{
            bitset.Set(size - right_set - 1);
            right_set++;
        }
        num_set++;
    }
}

void checker(const pe::AtomicBitset& bitset)
{
    std::size_t size = bitset.Size();
    std::size_t mid = size / 2;
    if((mid > 0) && (size % 2 == 0))
        mid = mid - 1;

    std::size_t last_first = std::numeric_limits<std::size_t>::max();
    while(true) {

        auto set = bitset.FindFirstSet();
        if(!set.has_value())
            continue;

        std::size_t first = set.value();
        std::size_t before_mid = mid - first;

        /* first has to be strictly decreasing */
        pe::assert(first <= last_first);
        last_first = first;

        /* before_mid bits at the end of the bitset must be set */
        for(int i = 0; i < before_mid; i++) {
            pe::assert(bitset.Test(size - 1 - i));
        }

        if(bitset.CountSetBits() == bitset.Size())
            break;
    }
}

void test(pe::AtomicBitset& bitset)
{
    std::vector<std::future<void>> tasks{};
    tasks.push_back(std::async(std::launch::async, checker, std::ref(bitset)));
    tasks.push_back(std::async(std::launch::async, setter, std::ref(bitset)));

    for(const auto& task : tasks) {
        task.wait();
    }
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::AtomicBitset bitset{kBitsetSize};

        pe::ioprint(pe::TextColor::eGreen, "Starting Atomic Bitset test.");
        test(bitset);
        pe::ioprint(pe::TextColor::eGreen, "Finished Atomic Bitset test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

