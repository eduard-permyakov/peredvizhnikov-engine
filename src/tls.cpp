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

export module tls;

#ifdef __linux__
import pthread;
#endif

import platform;
import logger;
import assert;
import shared_ptr;

import <atomic>;
import <memory>;
import <unordered_map>;
import <exception>;
import <thread>;
import <array>;
import <stack>;
import <vector>;

namespace pe{

constexpr static int kMaxThreads = 256;

template <typename T>
class ThreadDestructors
{
private:

    using array_type = std::array<pe::atomic_shared_ptr<T>, kMaxThreads>;

    struct DeleteDescriptor
    {
        int                  m_index;
        weak_ptr<array_type> m_array;
    };

    std::stack<DeleteDescriptor> m_descs;

public:

    ThreadDestructors() = default;
    ThreadDestructors(ThreadDestructors const&) = delete;

    void operator=(ThreadDestructors const&) = delete;

    ~ThreadDestructors()
    {
        while(!m_descs.empty()){
            auto desc = m_descs.top();
            if(auto array = desc.m_array.lock()) {
                (*array)[desc.m_index].store(pe::shared_ptr<T>{nullptr}, std::memory_order_release);
            }
            m_descs.pop();
        }
    }

    void add(pe::shared_ptr<array_type>& ptr, int index)
    {
        m_descs.push({index, weak_ptr{ptr}});
    }
};

/* We allocate a single native TLS entry and use it to keep 
 * a table of application-specific TLS IDs. This way we don't
 * bump into any limits regarding the maximum number of unique
 * TLS allocations that we are able to have. Each application-
 * specific TLS allocation is responsible for deallocating any
 * per-thread data.
 */
template <OS Platform = kOS>
requires (Platform == OS::eLinux)
struct TLSNativeAllocation
{
private:

    using map_type = std::unordered_map<uint32_t, weak_ptr<void>>;
    using array_type = std::array<pe::atomic_shared_ptr<map_type>, kMaxThreads>;

    pthread_key_t              m_key;
    pe::shared_ptr<array_type> m_ptrs;

    void clear_on_thread_exit(int index)
    {
        static thread_local ThreadDestructors<map_type> t_destructors;
        t_destructors.add(m_ptrs, index);
    }

    int push_ptr(pe::shared_ptr<map_type> ptr)
    {
        /* Don't use the first bin. Index 0 is reserved as a 'null' value */
        int idx = 1;
        auto& ptrs = *m_ptrs.get();
        while(idx < std::size(ptrs)) {
            pe::shared_ptr<map_type> expected{nullptr};
            if(ptrs[idx].compare_exchange_strong(expected, ptr,
                std::memory_order_release, std::memory_order_relaxed)) {
                return idx;
            }
            idx++;
        }
        throw std::runtime_error{
            "Exceeded maximum thread count for Thread-Local Storage."
        };
    }

public:

    TLSNativeAllocation()
        : m_key{}
        , m_ptrs{pe::make_shared<array_type>()}
    {
        int result = pthread_key_create(&m_key, nullptr);
        if(result) [[unlikely]]
            throw std::runtime_error{"Failed to allocate Thread-Local Storage."};
    }

    ~TLSNativeAllocation()
    {
        auto& ptrs = *m_ptrs.get();
        for(int i = 1; i < std::size(ptrs); i++) {
            ptrs[i].store(pe::shared_ptr<map_type>{nullptr}, std::memory_order_relaxed);
        }
        pthread_key_delete(m_key);
    }

    pe::shared_ptr<map_type> GetTable()
    {
        uintptr_t idx = reinterpret_cast<uintptr_t>(pthread_getspecific(m_key));
        if(!idx) {
            auto ptr = pe::make_shared<map_type>();
            idx = push_ptr(ptr);
            if(pthread_setspecific(m_key, reinterpret_cast<void*>(idx))) [[unlikely]]
                throw std::runtime_error{"Failed to set Thread-Local Storage."};
        }
        auto& ptrs = *m_ptrs.get();
        return ptrs[idx].load(std::memory_order_acquire);
    }

    std::vector<pe::shared_ptr<map_type>> GetAllTablesSnapshot()
    {
        auto& ptrs = *m_ptrs.get();
        std::vector<pe::shared_ptr<map_type>> ret{};
        for(int i = 1; i < std::size(ptrs); i++) {
            if(auto ptr = ptrs[i].load(std::memory_order_acquire)) {
                ret.push_back(ptr);
            }
        }
        return ret;
    }
};

inline TLSNativeAllocation  s_native_tls{};
inline std::atomic_uint32_t s_next_tls_key{};

/* All thread-local data will be destroyed:
 *
 *     1. When the owning thread terminates
 *     2. When the TLSAllocation object is destroyed
 *
 * This makes it suitable for use cases where short-lived 
 * transient threads all make use of the same allocation and 
 * for use cases where multiple allocations are dynamically 
 * created and destroyed by long-lived threads. In both cases, 
 * the thread-local memory will be destroyed as soon as it is 
 * no longer needed.
 */
export
template <typename T>
struct TLSAllocation
{
private:

    /* Keep around a set of all lazily-created pointers. 
     * This way, we are able to return a (non-linearizable)
     * snapshot of all currently added thread-specific 
     * pointers, which allows a single thread to iterate 
     * over the private data of all threads.
     */
    using array_type = std::array<pe::atomic_shared_ptr<T>, kMaxThreads>;

    uint32_t                   m_key;
    bool                       m_delete_on_thread_exit;
    pe::shared_ptr<array_type> m_ptrs;

    void clear_on_thread_exit(int index)
    {
        if(!m_delete_on_thread_exit)
            return;

        static thread_local ThreadDestructors<T> t_destructors;
        t_destructors.add(m_ptrs, index);
    }
    
    int push_ptr(pe::shared_ptr<T> ptr)
    {
        int idx = 0;
        auto& ptrs = *m_ptrs.get();
        while(idx < std::size(ptrs)) {
            pe::shared_ptr<T> expected{nullptr};
            if(ptrs[idx].compare_exchange_strong(expected, ptr,
                std::memory_order_release, std::memory_order_relaxed)) {
                return idx;
            }
            idx++;
        }
        throw std::runtime_error{
            "Exceeded maximum thread count for Thread-Local Storage."
        };
    }

public:

    TLSAllocation(TLSAllocation const&) = delete;
    TLSAllocation& operator=(TLSAllocation const&) = delete;

    TLSAllocation(TLSAllocation&& other) = default;
    TLSAllocation& operator=(TLSAllocation&&) = default;

    TLSAllocation(uint32_t key, bool delete_on_thread_exit)
        : m_key{key}
        , m_delete_on_thread_exit{delete_on_thread_exit}
        , m_ptrs{pe::make_shared<array_type>()}
    {}

    ~TLSAllocation()
    {
        ClearAllThreadSpecific();
    }
    
    template <typename U = T>
    requires (std::is_default_constructible_v<U>)
    pe::shared_ptr<T> GetThreadSpecific()
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            auto ptr = pe::make_shared<T>();
            table[m_key] = weak_ptr<T>{ptr};

            int idx = push_ptr(ptr);
            clear_on_thread_exit(idx);
        }
        return pe::static_pointer_cast<T>(table.at(m_key).lock());
    }

    template <typename... Args>
    pe::shared_ptr<T> GetThreadSpecific(Args&&... args)
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            auto ptr = pe::make_shared<T>(std::forward<Args>(args)...);
            table[m_key] = weak_ptr<T>{ptr};

            int idx = push_ptr(ptr);
            clear_on_thread_exit(idx);
        }
        return pe::static_pointer_cast<T>(table.at(m_key).lock());
    }

    template <typename U = T>
    requires (std::is_copy_assignable_v<T>)
    void SetThreadSpecific(U&& value)
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            auto ptr = pe::make_shared<T>();
            table[m_key] = weak_ptr<T>{ptr};

            int idx = push_ptr(ptr);
            clear_on_thread_exit(idx);
        }
        auto ptr = pe::static_pointer_cast<T>(table.at(m_key).lock());
        *ptr = std::forward<U>(value);
    }

    template <typename... Args>
    void EmplaceThreadSpecific(Args&&... args)
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            auto ptr = pe::make_shared<T>(std::forward<Args>(args)...);
            table[m_key] = weak_ptr<T>{ptr};

            int idx = push_ptr(ptr);
            clear_on_thread_exit(idx);
            return;
        }
        auto ptr = pe::static_pointer_cast<T>(table.at(m_key).lock());
        new (ptr.get()) T{std::forward<Args>(args)...};
    }

    std::vector<pe::shared_ptr<T>> GetThreadPtrsSnapshot() const
    {
        auto& ptrs = *m_ptrs.get();
        std::vector<pe::shared_ptr<T>> ret{};
        for(int i = 0; i < std::size(ptrs); i++) {
            if(auto ptr = ptrs[i].load(std::memory_order_acquire)) {
                ret.push_back(ptr);
            }
        }
        return ret;
    }

    void ClearAllThreadSpecific()
    {
        auto& ptrs = *m_ptrs.get();
        for(int i = 0; i < std::size(ptrs); i++) {
            ptrs[i].store(pe::shared_ptr<T>{nullptr}, std::memory_order_relaxed);
        }
    }
};

export
template <typename T>
TLSAllocation<T> AllocTLS(bool delete_on_thread_exit = true)
{
    uint32_t key = s_next_tls_key.fetch_add(1, std::memory_order_relaxed);
    return TLSAllocation<T>{key, delete_on_thread_exit};
}

} //namespace pe

