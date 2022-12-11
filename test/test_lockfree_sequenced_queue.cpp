import lockfree_sequenced_queue;
import assert;
import logger;
import concurrency;

import <cstdlib>;
import <atomic>;
import <ranges>;
import <optional>;
import <future>;
import <vector>;

constexpr int kNumValues = 500;
constexpr int kNumEnqueuers = 4;
constexpr int kNumDequeuers = 4;

struct alignas(16) QueueSize
{
    uint64_t m_seqnum;
    uint64_t m_size;
};

using AtomicQueueSize = pe::DoubleQuadWordAtomic<QueueSize>;

void enqueuer(pe::LockfreeSequencedQueue<int>& queue, AtomicQueueSize& size,
    const std::ranges::input_range auto&& input)
{
    for(const auto& value : input) {
        queue.ConditionallyEnqueue(+[](AtomicQueueSize& size, uint64_t seqnum, int value){
            auto expected = size.Load(std::memory_order_relaxed);
            if(expected.m_seqnum >= seqnum)
                return true;
            size.CompareExchange(expected, {seqnum, expected.m_size + 1},
                std::memory_order_relaxed, std::memory_order_relaxed);
            return true;
        }, size, value);
    }
}

void dequeuer(pe::LockfreeSequencedQueue<int>& queue, AtomicQueueSize& size,
    std::atomic_uint& num_dequeued, std::atomic_uint(&result)[kNumValues])
{
    while(num_dequeued.load(std::memory_order_relaxed) < kNumValues * kNumEnqueuers) {
        auto ret = queue.ConditionallyDequeue(+[](AtomicQueueSize& size, uint64_t seqnum, int value){
            auto expected = size.Load(std::memory_order_relaxed);
            if(expected.m_size == 0)
                return false;
            if(expected.m_seqnum >= seqnum)
                return true;
            size.CompareExchange(expected, {seqnum, expected.m_size - 1},
                std::memory_order_relaxed, std::memory_order_relaxed);
            return true;
        }, size);
        if(ret.has_value()) {
            num_dequeued.fetch_add(1, std::memory_order_relaxed);
            result[ret.value()].fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void test(pe::LockfreeSequencedQueue<int>& queue, AtomicQueueSize& size,
    std::atomic_uint& num_dequeued, const std::ranges::input_range auto&& input)
{
    std::vector<std::future<void>> tasks{};
    std::atomic_uint result[kNumValues];

    for(int i = 0; i < kNumEnqueuers; i++) {
        tasks.push_back(std::async(std::launch::async, enqueuer<decltype(input)>,
            std::ref(queue), std::ref(size), input));
    }
    for(int i = 0; i < kNumDequeuers; i++) {
        tasks.push_back(std::async(std::launch::async, dequeuer,
            std::ref(queue), std::ref(size), std::ref(num_dequeued), std::ref(result)));
    }
    for(const auto& task : tasks) {
        task.wait();
    }

    auto final_size = size.Load(std::memory_order_relaxed);
    pe::assert(final_size.m_size == 0, "Unexpected queue size", __FILE__, __LINE__);

    for(int i = 0; i < std::size(result); i++) {
        auto dequeued = result[i].load(std::memory_order_relaxed);
        pe::assert(dequeued == kNumEnqueuers, "Unexpected number of dequeued values.",
            __FILE__, __LINE__);
    }
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting Lockfree Sequenced Queue test.");

        AtomicQueueSize size{};
        std::atomic_uint num_dequeued{};
        pe::LockfreeSequencedQueue<int> sequenced_queue{};
        test(sequenced_queue, size, num_dequeued, std::views::iota(0, kNumValues));

        pe::ioprint(pe::TextColor::eGreen, "Finished Lockfree Sequenced Queue test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

