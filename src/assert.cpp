export module assert;

import platform;
import logger;

export import <string>;
import <mutex>;
import <iostream>;

namespace pe{

export
template <bool Debug = kDebug>
requires (Debug == true)
void assert(bool predicate, std::string_view message = {},
    std::string_view file = {}, int line = {})
{
    if(predicate) [[likely]]
        return;

    std::lock_guard<std::mutex> lock{iolock};
    pe::log_ex(std::cout, nullptr, TextColor::eBrightRed, "", true, false,
        "Failed Assert!");

    if(!message.empty()) {
        pe::log_ex(std::cout, nullptr, TextColor::eWhite, "", false, false,
            " [", message, "]");
    }

    if(!file.empty()) {
        pe::log_ex(std::cout, nullptr, TextColor::eWhite, "", false, true,
            " [", file, ":", line, "]");
    }else{
        pe::log_ex(std::cout, nullptr, TextColor::eWhite, "", false, true);
    }

    auto backtrace = Backtrace();
    for(auto& string : backtrace) {
        pe::log_ex(std::cout, nullptr, TextColor::eWhite, "", true, true,
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

