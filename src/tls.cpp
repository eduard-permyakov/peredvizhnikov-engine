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

namespace pe{

constexpr static int kMaxThreads = 256;

template <OS Platform = kOS>
requires (Platform == OS::eLinux)
struct native_key_trait
{
    using type = pthread_key_t;
};

using native_tls_key_t = typename native_key_trait<kOS>::type;

/* We allocate a single native TLS entry and use it to keep 
 * a table of application-specifif TLS IDs. This way we don't
 * bump into any limits regarding the maximum number of unique
 * TLS applications that we are able to have. Each application-
 * specific TLS allocation is responsible for deallocating any
 * per-thread data.
 */
template <OS Platform = kOS>
requires (Platform == OS::eLinux)
struct TLSNativeAllocation
{
private:

    /* Map with certain operations protected by a spinlock. While 
     * not a good solution in the general case, it's fine here since
     * the access to the map will be overwhelmingly from a single
     * owning thread, so contention will be very low.
     */
    template <typename Key, typename Value>
    class ConcurrentMap
    {
    private:
        std::atomic_flag               m_spinlock{};
        std::unordered_map<Key, Value> m_map{};
    public:
        Value get(Key key)
        {
            while(m_spinlock.test_and_set(std::memory_order_acquire));
            auto&& ret = m_map[key];
            m_spinlock.clear(std::memory_order_release);
            return ret;
        }
        void set(Key key, Value value)
        {
            while(m_spinlock.test_and_set(std::memory_order_acquire));
            m_map[key] = value;
            m_spinlock.clear(std::memory_order_release);
        }
        void erase(Key key)
        {
            while(m_spinlock.test_and_set(std::memory_order_acquire));
            m_map.erase(key);
            m_spinlock.clear(std::memory_order_release);
        }
        bool contains(Key key)
        {
            while(m_spinlock.test_and_set(std::memory_order_acquire));
            auto&& ret = m_map.contains(key);
            m_spinlock.clear(std::memory_order_release);
            return ret;
        }
    };

    using map_type = ConcurrentMap<uint32_t, pe::shared_ptr<void>>;

    native_tls_key_t                   m_key;
    std::atomic_int                    m_ptr_count;
    std::atomic_int                    m_next_idx;
    std::array<map_type*, kMaxThreads> m_ptrs;

    void push_ptr(map_type *ptr)
    {
        int myidx = m_next_idx.fetch_add(1, std::memory_order_relaxed);
        if(myidx >= kMaxThreads) [[unlikely]]
            throw std::runtime_error{"Exceeded maximum thread count for Thread-Local Storage."};
        m_ptrs[myidx] = ptr;

        int expected;
        do{
            expected = myidx;
        }while(!m_ptr_count.compare_exchange_weak(expected, expected + 1,
            std::memory_order_release, std::memory_order_relaxed));
    }

public:

    TLSNativeAllocation()
        : m_key{}
        , m_ptr_count{}
        , m_ptrs{}
    {
        std::unique_ptr<map_type> local_keys{
            new map_type{}
        };
        int result = pthread_key_create(&m_key, nullptr);
        if(result) [[unlikely]]
            throw std::runtime_error{"Failed to allocate Thread-Local Storage."};
        result = pthread_setspecific(m_key, local_keys.release());
        if(result) [[unlikely]]
            throw std::runtime_error{"Failed to set Thread-Local Storage."};
    }

    ~TLSNativeAllocation()
    {
        int count = m_ptr_count.load(std::memory_order_relaxed);
        for(int i = 0; i < count; i++) {
            delete m_ptrs[i];
        }
        pthread_key_delete(m_key);
    }

    map_type *GetTable()
    {
        auto ptr = reinterpret_cast<map_type*>(pthread_getspecific(m_key));
        if(!ptr) {
            ptr = new map_type{};
            push_ptr(ptr);
            if(pthread_setspecific(m_key, ptr)) [[unlikely]]
                throw std::runtime_error{"Failed to set Thread-Local Storage."};
        }
        return ptr;
    }

    std::vector<map_type*> GetAllTablesSnapshot()
    {
        std::vector<map_type*> ret{};
        int count = m_ptr_count.load(std::memory_order_acquire);
        ret.reserve(count);
        ret.insert(std::end(ret), std::begin(m_ptrs), std::begin(m_ptrs) + count);
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
 * This makes it suitable for use cases where short-lived transient 
 * threads all make use of the same allocation and for use cases 
 * where multiple allocations are dynamically created and destroyed 
 * by long-lived threads. In both cases, the thread-local memory will 
 * be destroyed as soon as it is no longer needed.
 */
export
template <typename T>
struct TLSAllocation
{
private:

    /* Keep around a set of all lazily-created pointers. 
     * This way, we are able to return a linearizable 
     * snapshot of all currently added thread-specific 
     * pointers, which allows a single thread to iterate 
     * over the private data of all threads.
     *
     * To make this thread-safe and lock-free, a thread
     * will increment m_next_idx when it wishes to push
     * its' private pointer. The prior value of m_next_idx
     * is now "owned" by the thread and it's safe for it
     * to write its' pointer into the corresponding slot.
     * To "commit" this transaction, the thread will 
     * increment m_ptr_count and issue a release barrier,
     * but it must use CAS to poll until m_ptr_count is 
     * exactly equal to to the written-to index. After a 
     * successful increment the pointer array in the range 
     * of [0...m_ptr_count) is safe to read after issuing 
     * an acquire barrier.
     */

    uint32_t                                 m_key;
    bool                                     m_delete_on_thread_exit;
    std::atomic_int                          m_ptr_count;
    std::atomic_int                          m_next_idx;
    std::array<pe::weak_ptr<T>, kMaxThreads> m_ptrs;

    void clear_on_thread_exit(uint32_t key)
    {
        if(!m_delete_on_thread_exit)
            return;

        class ThreadDestructors
        {
        private:
            std::stack<uint32_t> m_keys;
        public:
            ThreadDestructors() = default;
            ThreadDestructors(ThreadDestructors const&) = delete;
            void operator=(ThreadDestructors const&) = delete;
            ~ThreadDestructors()
            {
                auto& table = *s_native_tls.GetTable();
                while(!m_keys.empty()){
                    uint32_t key = m_keys.top();
                    table.erase(key);
                    m_keys.pop();
                }
            }
            void add(uint32_t key)
            {
                m_keys.push(key);
            }
        };

        thread_local ThreadDestructors t_destructors;
        t_destructors.add(key);
    }
    
    void push_ptr(pe::shared_ptr<T> ptr)
    {
        int myidx = m_next_idx.fetch_add(1, std::memory_order_relaxed);
        if(myidx >= kMaxThreads) [[unlikely]]
            throw std::runtime_error{"Exceeded maximum thread count for Thread-Local Storage."};
        m_ptrs[myidx] = ptr;

        int expected;
        do{
            expected = myidx;
        }while(!m_ptr_count.compare_exchange_weak(expected, expected + 1,
            std::memory_order_release, std::memory_order_relaxed));
    }

public:

    TLSAllocation(TLSAllocation const&) = delete;
    TLSAllocation& operator=(TLSAllocation const&) = delete;

    TLSAllocation(TLSAllocation&&) = default;
    TLSAllocation& operator=(TLSAllocation&&) = default;

    TLSAllocation(uint32_t key, bool delete_on_thread_exit)
        : m_key{key}
        , m_delete_on_thread_exit{delete_on_thread_exit}
        , m_ptr_count{}
        , m_next_idx{}
        , m_ptrs{}
    {}

    ~TLSAllocation()
    {
        auto all_threads = s_native_tls.GetAllTablesSnapshot();
        for(auto table : all_threads) {
            table->erase(m_key);
        }
    }
    
    template <typename U = T>
    requires (std::is_default_constructible_v<U>)
    pe::shared_ptr<T> GetThreadSpecific()
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            auto ptr = pe::make_shared<T>();
            table.set(m_key, ptr);

            clear_on_thread_exit(m_key);
            push_ptr(ptr);
        }
        return static_pointer_cast<T>(table.get(m_key));
    }

    template <typename... Args>
    pe::shared_ptr<T> GetThreadSpecific(Args&&... args)
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            auto ptr = pe::make_shared<T>(std::forward<Args>(args)...);
            table.set(m_key, ptr);

            clear_on_thread_exit(m_key);
            push_ptr(ptr);
        }
        return static_pointer_cast<T>(table.get(m_key));
    }

    template <typename U = T>
    requires (std::is_copy_assignable_v<T>)
    void SetThreadSpecific(U&& value)
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            auto ptr = pe::make_shared<T>();
            table.set(m_key, ptr);

            clear_on_thread_exit(m_key);
            push_ptr(ptr);
        }
        auto ptr = pe::static_pointer_cast<T>(table.get(m_key));
        *ptr = std::forward<U>(value);
    }

    template <typename... Args>
    void EmplaceThreadSpecific(Args&&... args)
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            auto ptr = pe::make_shared<T>(std::forward<Args>(args)...);
            table.set(m_key, ptr);

            clear_on_thread_exit(m_key);
            push_ptr(ptr);
            return;
        }
        auto ptr = static_pointer_cast<T>(table.get(m_key));
        new (ptr.get()) T{std::forward<Args>(args)...};
    }

    std::vector<weak_ptr<T>> GetThreadPtrsSnapshot() const
    {
        std::vector<weak_ptr<T>> ret{};
        int count = m_ptr_count.load(std::memory_order_acquire);
        for(int i = 0; i < count; i++) {
            weak_ptr<T> curr = m_ptrs[i];
            ret.push_back(curr);
        }
        return ret;
    }

    void ClearAllThreadSpecific()
    {
        auto all_threads = s_native_tls.GetAllTablesSnapshot();
        for(auto table : all_threads) {
            table->erase(m_key);
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

}; //namespace pe

