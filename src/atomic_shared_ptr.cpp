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

export module shared_ptr;
export import :base;

import platform;
import logger;
import concurrency;

import <cstdlib>;
import <atomic>;
import <string>;
import <mutex>;
import <variant>;
import <vector>;

namespace pe{

/* Lock-free atomic_shared_ptr implementation based on split
 * reference counting (a.k.a. differential reference counting).
 */
export
template <typename T>
class atomic_shared_ptr
{
private:

    template <typename Y>
    friend class atomic_shared_ptr;

    template <typename Y, bool Debug>
    friend struct OwnershipLogger;

    struct alignas(16) State
    {
        ControlBlock *m_control_block;
        uint32_t      m_local_refcount;
        uint32_t      m_offset;
    };

    using AtomicState = DoubleQuadWordAtomic<State>;

    mutable AtomicState m_state;

    /* Debug state that isn't compiled in for release builds */
    [[no_unique_address]] flag_type  m_tracing;
    [[no_unique_address]] flag_type  m_logging;
    [[no_unique_address]] owner_type m_owner;

    void clear()
    {
        m_state.Store({nullptr, 0, 0}, std::memory_order_relaxed);
    }

    void inc_strong_refcount()
    {
        auto *cb = m_state.Load(std::memory_order_relaxed).m_control_block;
        if(cb == nullptr)
            return;
        cb->inc_strong_refcount();
    }

    inline void dec_weak_refcount()
    {
        auto *cb = m_state.Load(std::memory_order_relaxed).m_control_block;
        if(cb == nullptr)
            return;

        cb->dec_weak_refcount();
        clear();
    }

    template <bool Debug = kDebug>
    requires (Debug == true)
    owner_type create_owner(flag_type tracing) const
    {
        if(tracing == flag_type{}) [[likely]]
            return {};

        std::string name = typeid(std::remove_extent_t<T>).name();
        auto demangled = Demangle(name);
        if(demangled) {
            name = std::string{demangled.get()};
        }

        return {
            .m_id = s_next_instance_id.fetch_add(1, std::memory_order_relaxed),
            .m_instance = this,
            .m_typename = name,
            .m_thread = std::this_thread::get_id(),
            .m_thread_name = GetThreadName(),
            .m_backtrace = Backtrace()
        };
    }

    template <bool Debug = kDebug>
    requires (Debug == false)
    owner_type create_owner(flag_type tracing) const
    {
        return {};
    }

    uint32_t ptrdiff(void *obj, void *subobject) const
    {
        uintptr_t objval = reinterpret_cast<uintptr_t>(obj);
        uintptr_t subobjval = reinterpret_cast<uintptr_t>(subobject);
        return static_cast<uint32_t>(subobjval - objval);
    }

    T *ptr(void *base, uint32_t offset) const
    {
        uintptr_t baseval = reinterpret_cast<uintptr_t>(base);
        baseval += static_cast<ptrdiff_t>(offset);
        return reinterpret_cast<T*>(baseval);
    }

    inline void trace_create()
    {
        if constexpr (!kDebug)
            return;

        auto state = m_state.Load(std::memory_order_relaxed);
        auto *cb = state.m_control_block;
        if(!cb || (m_tracing == flag_type{})) [[likely]]
            return;

        [[maybe_unused]] auto owners_lock = 
            OwnershipTracer<T, kDebug>::lock_owners(cb->m_owners_lock);
        OwnershipTracer<T, kDebug>::trace_add_owner(cb->m_owners, m_owner);

        if(!cb || (m_logging == flag_type{})) [[likely]]
            return;

        std::size_t nowners = OwnershipTracer<T, kDebug>::nowners(cb->m_owners);
        std::lock_guard<std::mutex> iolock{pe::iolock};
        OwnershipLogger<T, kDebug>::log_newline();
        OwnershipLogger<T, kDebug>::log_atomic_pointer(*this, state, nowners, "is created");
        OwnershipLogger<T, kDebug>::log_owner(m_owner, "new", true);
        OwnershipLogger<T, kDebug>::log_newline();
    }

    inline void trace_clear(State state)
    {
        if constexpr (!kDebug)
            return;

        ControlBlock *cb = state.m_control_block;
        if(!cb || (m_tracing == flag_type{})) [[likely]]
            return;

        [[maybe_unused]] auto owners_lock = 
            OwnershipTracer<T, kDebug>::lock_owners(cb->m_owners_lock);
        OwnershipTracer<T, kDebug>::trace_remove_owner(cb->m_owners, m_owner);

        if(m_logging == flag_type{}) [[likely]]
            return;

        std::size_t nowners = OwnershipTracer<T, kDebug>::nowners(cb->m_owners);
        std::lock_guard<std::mutex> iolock{pe::iolock};
        OwnershipLogger<T, kDebug>::log_newline();
        OwnershipLogger<T, kDebug>::log_atomic_pointer(*this, state, nowners, "is reset");
        OwnershipLogger<T, kDebug>::log_owner(m_owner, "del", true);
        OwnershipLogger<T, kDebug>::log_newline();

        if(nowners == 0) {
            OwnershipLogger<T, kDebug>::log_newline();
            OwnershipLogger<T, kDebug>::log_atomic_pointer(*this, state, nowners, "is deleted");
            OwnershipLogger<T, kDebug>::log_owner(m_owner, "last", true);
            OwnershipLogger<T, kDebug>::log_newline();
        }
    }

public:

    bool is_lock_free() const noexcept
    {
        return true;
    }

    atomic_shared_ptr(const atomic_shared_ptr&) = delete;
    void operator=(const atomic_shared_ptr&) = delete;

    constexpr atomic_shared_ptr(flag_type tracing = {}, flag_type logging  = {}) noexcept
        : m_state(State{nullptr, 0u, 0u})
        , m_tracing{tracing}
        , m_logging{logging}
        , m_owner{create_owner(tracing)}
    {}

    constexpr atomic_shared_ptr(shared_ptr<T> desired) noexcept
        : m_state(State{desired.m_control_block, 0u,
            ptrdiff(desired.m_control_block->m_obj, desired.get())})
        , m_tracing{desired.m_tracing}
        , m_logging{desired.m_logging}
        , m_owner{create_owner(m_tracing)}
    {
        inc_strong_refcount();
        trace_create();
    }

    void operator=(shared_ptr<T> desired) noexcept
    {
        return store(desired);
    }

    void store(shared_ptr<T> desired, std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t offset = 0;
        if(desired.m_control_block) {
            desired.m_control_block->inc_strong_refcount();
            offset = ptrdiff(desired.m_control_block->m_obj, desired.get());
        }

        AnnotateHappensBefore(__FILE__, __LINE__, &m_state);
        State newstate{desired.m_control_block, 0u, offset};
        State prev = m_state.Exchange(newstate, order);

        if(prev.m_control_block) {
            trace_clear(prev);
            prev.m_control_block->dec_strong_refcount(prev.m_local_refcount);
        }

        trace_create();
    }

    shared_ptr<T> load(std::memory_order order = std::memory_order_seq_cst) const noexcept
    {
        std::atomic_thread_fence(std::memory_order_acquire);
        State oldstate = m_state.FetchAdd(0, 0, 1, 0, order);
        if(!oldstate.m_control_block)
            return shared_ptr<T>{nullptr};

        AnnotateHappensAfter(__FILE__, __LINE__, &m_state);
        T *obj = ptr(oldstate.m_control_block->m_obj, oldstate.m_offset);
        return shared_ptr<T>{oldstate.m_control_block, obj, m_tracing, m_logging};
    }

    operator shared_ptr<T>() const noexcept
    {
        return load();
    }

    shared_ptr<T> exchange(shared_ptr<T> desired, 
                           std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t offset = 0;
        if(desired.m_control_block) {
            desired.m_control_block->inc_strong_refcount();
            offset = ptrdiff(desired.m_control_block->m_obj, desired.get());
        }

        State newstate{desired.m_control_block, 0u, offset};
        State prev = m_state.Exchange(newstate, order);

        T *obj = nullptr;
        if(prev.m_control_block) {
            trace_clear(prev);
            obj = ptr(prev.m_control_block->m_obj, prev.m_offset);
            prev.m_control_block->dec_strong_refcount(prev.m_local_refcount + 1);
        }

        trace_create();
        return shared_ptr<T>{prev.m_control_block, obj, m_tracing, m_logging};
    }

    bool compare_exchange_weak(shared_ptr<T>& expected,   const shared_ptr<T>& desired,
                               std::memory_order success, std::memory_order failure) noexcept
    {
        return compare_exchange_strong(expected, desired, success, failure);
    }

    bool compare_exchange_weak(shared_ptr<T>& expected,   shared_ptr<T>&& desired,
                               std::memory_order success, std::memory_order failure) noexcept
    {
        return compare_exchange_strong(expected, std::move(desired), success, failure);
    }

    bool compare_exchange_weak(shared_ptr<T>& expected, const shared_ptr<T>& desired,
                               std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        return compare_exchange_strong(expected, desired, order);
    }

    bool compare_exchange_weak(shared_ptr<T>& expected, shared_ptr<T>&& desired,
                               std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        return compare_exchange_strong(expected, std::move(desired), order);
    }

    bool compare_exchange_strong(shared_ptr<T>& expected,   const shared_ptr<T>& desired,
                                 std::memory_order success, std::memory_order failure) noexcept
    {
        if(desired.m_control_block) {
            desired.m_control_block->inc_strong_refcount();
        }

        State state = m_state.Load(std::memory_order_relaxed);
        while(true) {

            uint32_t expected_offset = 0;
            if(expected.m_control_block) {
                expected_offset = ptrdiff(expected.m_control_block->m_obj, expected.get());
            }

            if((expected.m_control_block != state.m_control_block)
            || (expected_offset != state.m_offset)) {

                expected = load(failure);

                if(desired.m_control_block) {
                    desired.m_control_block->dec_strong_refcount(0);
                }
                return false;
            }

            uint32_t desired_offset = 0;
            if(desired.m_control_block) {
                desired_offset = ptrdiff(desired.m_control_block->m_obj, desired.get());
            }

            State newstate{desired.m_control_block, 0u, desired_offset};
            AnnotateHappensBefore(__FILE__, __LINE__, &m_state);
            if(m_state.CompareExchange(state, newstate, success, std::memory_order_relaxed)) {

                if(state.m_control_block) {
                    trace_clear(state);
                    state.m_control_block->dec_strong_refcount(state.m_local_refcount);
                }
                trace_create();
                return true;
            }
        }
    }

    bool compare_exchange_strong(shared_ptr<T>& expected,   shared_ptr<T>&& desired,
                                 std::memory_order success, std::memory_order failure) noexcept
    {
        bool ret = compare_exchange_strong(expected, desired, success, failure);
        desired.reset();
        return ret;
    }

    bool compare_exchange_strong(shared_ptr<T>& expected, const shared_ptr<T>& desired,
                                 std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        return compare_exchange_strong(expected, desired, order, order);
    }

    bool compare_exchange_strong(shared_ptr<T>& expected, shared_ptr<T>&& desired,
                                 std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        return compare_exchange_strong(expected, std::move(desired), order, order);
    }
};

} // namespace pe

