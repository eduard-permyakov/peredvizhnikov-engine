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

export module assert;

import platform;
import logger;

export import <source_location>;
export import <string>;
import <mutex>;
import <iostream>;
import <vector>;

namespace pe{

export
template <bool Debug = kDebug>
requires (Debug == true)
void assert(bool predicate, std::string_view message = {},
    std::source_location location = std::source_location::current())
{
    if(predicate) [[likely]]
        return;

    std::lock_guard<std::mutex> lock{iolock};
    pe::ioprint_unlocked(TextColor::eBrightRed, "", true, false,
        "Failed Assert!");

    if(!message.empty()) {
        pe::ioprint_unlocked(TextColor::eWhite, "", false, false,
            " [", message, "]");
    }

	pe::ioprint_unlocked(TextColor::eWhite, "", false, true,
		" [", location.file_name(), ":", location.line(), "]");

    auto backtrace = Backtrace();
    for(auto& string : backtrace) {
        pe::ioprint_unlocked(TextColor::eWhite, "", true, true,
            "    ", string);
    }
    std::cout << std::flush;
    std::terminate();
}

export
template <bool Debug = kDebug>
requires (Debug == false)
void assert(bool predicate, std::string_view message = {},
    std::string_view file = {}, int line = {})
{}

}; //namespace pe

