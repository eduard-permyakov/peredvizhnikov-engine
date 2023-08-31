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
import event;
import logger;

import <cstdlib>;
import <exception>;
import <chrono>;
import <thread>;
import <filesystem>;
import <iostream>;
import <fstream>;
import <memory>;
import <exception>;
import <vector>;


constexpr int kNumReaders = 16;
constexpr int kNumReadBytes = 16;

class Reader : public pe::Task<std::unique_ptr<char[]>, Reader>
{
    using Task<std::unique_ptr<char[]>, Reader>::Task;

    virtual Reader::handle_type Run()
    {
        std::filesystem::path urandom_path{"/dev/urandom"};
        auto bytes = co_await IO([&urandom_path](){
            std::ifstream ifs{urandom_path.string(), std::ios::in | std::ios::binary};
            if(!ifs.is_open())
                throw std::runtime_error{"Unable to open file:" + urandom_path.string()};
            std::unique_ptr<char[]> ret{new char[kNumReadBytes]};
            ifs.read(ret.get(), kNumReadBytes);
            ifs.close();
            return ret;
        });
        co_return bytes;
    }
};

class Tester : public pe::Task<void, Tester>
{
    using Task<void, Tester>::Task;

    virtual Tester::handle_type Run()
    {
        constexpr std::chrono::milliseconds sleep_duration{3000};
        co_await IO([sleep_duration](){
            pe::dbgprint("Starting sleeping...");
            std::this_thread::sleep_for(sleep_duration);
            pe::dbgprint("Finished sleeping...");
        });

        std::vector<pe::shared_ptr<Reader>> readers;
        std::vector<std::unique_ptr<char[]>> results;
        for(int i = 0; i < kNumReaders; i++) {
            auto reader = Reader::Create(Scheduler(), pe::Priority::eBackground,
                pe::CreateMode::eLaunchAsync, pe::Affinity::eAny);
            readers.push_back(reader);
        }
        for(int i = 0; i < kNumReaders; i++) {
            auto result = co_await readers[i];
            results.push_back(std::move(result));
        }
        for(int i = 0; i < kNumReaders; i++) {
            pe::ioprint(pe::TextColor::eGreen, "Reader", i, "read", kNumReadBytes, "bytes:");
            for(int j = 0; j < kNumReadBytes; j++) {
                uint16_t byte = static_cast<uint8_t>(results[i][j]);
                pe::dbgprint("    ", pe::fmt::justified{j, 2, pe::fmt::Justify::eRight, ' '},
                    pe::fmt::cat{}, ":", pe::fmt::hex{byte});
            }
        }

        Broadcast<pe::EventType::eQuit>();
        co_return;
    }
};

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::Scheduler scheduler{};
        auto tester = Tester::Create(scheduler);
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

