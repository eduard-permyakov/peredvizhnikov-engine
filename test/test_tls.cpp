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

import tls;
import logger;
import assert;

import <future>;
import <cstdlib>;
import <iostream>;
import <string>;
import <chrono>;
import <vector>;


struct Object
{
    std::string m_name;
    int         m_count;

    Object()
        : m_name{}
        , m_count{}
    {}

    Object(const std::string& name, int count)
        : m_name{name}
        , m_count{count}
    {}

    ~Object()
    {
        using namespace std::string_literals;
        pe::dbgprint("Object", m_name, "("s + std::to_string(m_count) + ")"s,
            "is destroyed!");
    }
};

void worker(pe::TLSAllocation<Object>& tls, std::string name)
{
    tls.EmplaceThreadSpecific(name, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    {
        auto obj = tls.GetThreadSpecific();
        pe::assert(obj->m_name == name, "unexpected name");
        pe::assert(obj->m_count == 0, "unexpected count");
        obj->m_count++;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    {
        auto obj = tls.GetThreadSpecific();
        pe::assert(obj->m_name == name, "unexpected name");
        pe::assert(obj->m_count == 1, "unexpected count");
    }
}

void test_tls()
{
    std::string names[] = {
        "Jim",
        "Tim",
        "Mike",
        "Jake",
        "Alfonso",
        "Geronimo",
        "Kyle",
        "Bryan",
        "Bryce",
        "Luke",
        "Hans",
        "Timmy",
        "Ivan",
        "Scott",
        "Siddhartha",
        "Walter"
    };

    auto tls = pe::AllocTLS<Object>();
    std::vector<std::future<void>> tasks{};

    for(int i = 0; i < std::size(names); i++) {
        tasks.push_back(
            std::async(std::launch::async, worker, 
            std::ref(tls), names[i]));
    }
    for(const auto& task : tasks) {
        task.wait();
    }
}

int main()
{
    int ret = EXIT_SUCCESS;

    try{

        pe::ioprint(pe::TextColor::eGreen, "Testing Thread-Local Storage.");
        test_tls();
        pe::ioprint(pe::TextColor::eGreen, "Finished Thread-Local Storage test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }

    return ret;
}

