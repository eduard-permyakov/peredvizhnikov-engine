export module shared_ptr;
export import :base;

import platform;
import logger;
import concurrency;

import <cstdlib>;
import <atomic>;

namespace pe{

export
template <typename T>
class alignas(16) atomic_shared_ptr
{
private:

public:

    constexpr atomic_shared_ptr() noexcept;
    constexpr atomic_shared_ptr(shared_ptr<T> desired) noexcept;
    atomic_shared_ptr(const atomic_shared_ptr&) = delete;

    void operator=(shared_ptr<T> desired) noexcept;
    void operator=(const atomic_shared_ptr&) = delete;

    static constexpr bool is_always_lock_free = true;

    bool is_lock_free() const noexcept
    {
        return true;
    }

    void store(shared_ptr<T> desired, std::memory_order order = std::memory_order_seq_cst) noexcept;
    shared_ptr<T> load(std::memory_order order = std::memory_order_seq_cst) const noexcept;
    operator shared_ptr<T>() const noexcept;
    shared_ptr<T> exchange(shared_ptr<T> desired, std::memory_order order = std::memory_order_seq_cst) noexcept;

    bool compare_exchange_weak(shared_ptr<T>& expected,   const shared_ptr<T>& desired,
                               std::memory_order success, std::memory_order failure) noexcept;
    bool compare_exchange_weak(shared_ptr<T>& expected,   shared_ptr<T>&& desired,
                               std::memory_order success, std::memory_order failure) noexcept;
    bool compare_exchange_weak(shared_ptr<T>& expected, const shared_ptr<T>& desired,
                               std::memory_order order = std::memory_order_seq_cst) noexcept;
    bool compare_exchange_weak(shared_ptr<T>& expected, shared_ptr<T>&& desired,
                               std::memory_order order = std::memory_order_seq_cst) noexcept;

    bool compare_exchange_strong(shared_ptr<T>& expected,   const shared_ptr<T>& desired,
                                 std::memory_order success, std::memory_order failure) noexcept;
    bool compare_exchange_strong(shared_ptr<T>& expected,   shared_ptr<T>&& desired,
                                 std::memory_order success, std::memory_order failure) noexcept;
    bool compare_exchange_strong(shared_ptr<T>& expected, const shared_ptr<T>& desired,
                                 std::memory_order order = std::memory_order_seq_cst) noexcept;
    bool compare_exchange_strong(shared_ptr<T>& expected, shared_ptr<T>&& desired,
                                 std::memory_order order = std::memory_order_seq_cst) noexcept;

};

} // namespace pe

