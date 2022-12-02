import atomic_bitset;
import assert;
import logger;

import <cstdlib>;
import <limits>;
import <future>;
import <optional>;


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
        pe::assert(first <= last_first, "", __FILE__, __LINE__);
        last_first = first;

        /* before_mid bits at the end of the bitset must be set */
        for(int i = 0; i < before_mid; i++) {
            pe::assert(bitset.Test(size - 1 - i), "", __FILE__, __LINE__);
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

