export module engine;

import sync;
import event;
import event_pumper;
import logger;
import SDL2;
import window;

import <string>;
import <chrono>;
import <array>;
import <any>;

namespace pe{

constexpr auto kWindowTitle = "Peredvizhnikov Engine";

using namespace std::string_literals;

export 
class Engine : public Task<void, Engine>
{
    using Task<void, Engine>::Task;
    using TimestampType = std::chrono::time_point<std::chrono::steady_clock>;
    using Microseconds = std::chrono::microseconds;

    static inline constexpr std::size_t kFrameHistoryLength = 100;

    uint64_t m_frame_idx{0};
    std::array<Microseconds, kFrameHistoryLength> m_frame_times{};
    Microseconds                                  m_frame_times_total{};
    Microseconds                                  m_sma_latency{};

    /* Calculate a Simple Moving Average
     */
    void update_latency(TimestampType& prev)
    {
        auto now = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<Microseconds>(now - prev);
        std::size_t idx = m_frame_idx % kFrameHistoryLength;

        prev = now;
        m_frame_times_total -= m_frame_times[idx];
        m_frame_times_total += delta;
        m_frame_times[idx] = delta;
        m_sma_latency = m_frame_times_total / kFrameHistoryLength;
    }

    uint64_t fps()
    {
        return (1'000'000.0 / m_sma_latency.count());
    }

    virtual Engine::handle_type Run()
    {
        Barrier barrier{Scheduler(), 2};
        auto event_pumper = EventPumper::Create(Scheduler(), Priority::eCritical, 
            CreateMode::eLaunchSync, Affinity::eMainThread, barrier);
        auto prev = std::chrono::steady_clock::now();
        auto window = Window::Create(Scheduler(), Priority::eCritical,
            CreateMode::eLaunchSync, Affinity::eMainThread, kWindowTitle, 640, 480);

        while(true) {
            Broadcast<EventType::eNewFrame>(m_frame_idx++);
            update_latency(prev);
            co_await window->SetTitle(CallToken<Window>(), 
                kWindowTitle + " (fps: "s + std::to_string(fps()) + ")"s);
            co_await barrier.ArriveAndWait();
        }
    }
};

} // namespace pe

