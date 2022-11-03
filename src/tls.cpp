export module tls;

#ifdef __linux__
import pthread;
#endif

import platform;
import logger;
import assert;

import <atomic>;
import <memory>;
import <unordered_map>;
import <exception>;
import <thread>;
import <array>;

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

    using map_type = std::unordered_map<uint32_t, void*>;

    native_tls_key_t                   m_key;
    std::atomic_int                    m_ptr_count;
    std::array<map_type*, kMaxThreads> m_ptrs;

    void push_ptr(map_type *ptr)
    {
        int myidx = m_ptr_count.fetch_add(1, std::memory_order_relaxed);
        if(myidx >= kMaxThreads) [[unlikely]]
            throw std::runtime_error{"Exceeded maximum thread count for Thread-Local Storage."};
        m_ptrs[myidx] = ptr;
    }

public:

    TLSNativeAllocation()
    {
        std::unique_ptr<std::unordered_map<uint32_t, void*>> local_keys{
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

    std::unordered_map<uint32_t, void*> *GetTable()
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
};

inline TLSNativeAllocation  s_native_tls{};
inline std::atomic_uint32_t s_next_tls_key{};

export
template <typename T>
requires requires {
    requires (std::is_default_constructible_v<T>);
    requires (std::is_copy_assignable_v<T>);
}
struct TLSAllocation
{
private:

    uint32_t                       m_key;

    /* Keep around a set of all lazily-created pointers. 
     * This way we can delete all threads' pointers when
     * the Allocation object is destroyed.
     */
    std::atomic_int                m_ptr_count;
    std::array<void*, kMaxThreads> m_ptrs;
    
    void push_ptr(void *ptr)
    {
        int myidx = m_ptr_count.fetch_add(1, std::memory_order_relaxed);
        if(myidx >= kMaxThreads) [[unlikely]]
            throw std::runtime_error{"Exceeded maximum thread count for Thread-Local Storage."};
        m_ptrs[myidx] = ptr;
    }

public:

    TLSAllocation(uint32_t key)
        : m_key{key}
        , m_ptr_count{0}
        , m_ptrs{}
    {}
    
    ~TLSAllocation()
    {
        int count = m_ptr_count.load(std::memory_order_relaxed);
        for(int i = 0; i < count; i++) {
            delete reinterpret_cast<T*>(m_ptrs[i]);
        }
    }

    T *GetThreadSpecific()
    {
        auto table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            table[m_key] = reinterpret_cast<void*>(new T{});
            push_ptr(table[m_key]);
        }
        return reinterpret_cast<T*>(table[m_key]);
    }

    void SetThreadSpecific(T& value)
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            table[m_key] = reinterpret_cast<void*>(new T{});
            push_ptr(table[m_key]);
            return;
        }
        auto ptr = reinterpret_cast<T*>(table[m_key]);
        *ptr = value;
    }

    template <typename... Args>
    void SetThreadSpecific(Args... args)
    {
        auto& table = *s_native_tls.GetTable();
        if(!table.contains(m_key)) {
            table[m_key] = reinterpret_cast<void*>(new T{std::forward<Args>(args)...});
            push_ptr(table[m_key]);
            return;
        }
        auto ptr = reinterpret_cast<T*>(table[m_key]);
        *ptr = T{std::forward<Args>(args)...};
    }
};

export
template <typename T>
TLSAllocation<T> AllocTLS()
{
    uint32_t key = s_next_tls_key.fetch_add(1, std::memory_order_relaxed);
    return TLSAllocation<T>{key};
}

}; //namespace pe

