module;
// TODO: comment about this
// set scheduler-locking on
extern "C" [[maybe_unused]] void dump_atomic_trace(int n);
export module atomic_trace;

import tls;
import logger;
import platform;
import shared_ptr;

import <cstdlib>;
import <atomic>;
import <array>;
import <ranges>;
import <any>;
import <unordered_map>;
import <sstream>;
import <thread>;
import <vector>;

namespace pe{

constexpr int kTraceBufferSize = 4096;

enum class AtomicOp : uint32_t
{
    eLoad,
    eStore,
    eExchange,
    eCompareExchange,
    eFetchAdd,
    eFetchSub,
    eFetchAnd,
    eFetchOr,
    eFetchXor,
    eFunctionEnter,
    eFunctionReturn
};

template <typename T>
std::string typestring()
{
    std::string name = typeid(T).name();
    auto demangled = Demangle(name);
    if(demangled) {
        name = std::string{demangled.get()};
    }
    return name;
}

std::string orderstring(std::memory_order order)
{
    switch(order) {
    case std::memory_order_relaxed:
        return "relaxed";
    case std::memory_order_release:
        return "release";
    case std::memory_order_acquire:
        return "acquire";
    case std::memory_order_consume:
        return "consume";
    case std::memory_order_acq_rel:
        return "acq_rel";
    case std::memory_order_seq_cst:
        return "seq_cst";
    }
}

template <typename T>
struct LoadOpDesc
{
    std::memory_order m_order;
    T                 m_read;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "Load";
        ss << "<" << typestring<T>() << ">";
        ss << " (order: " << orderstring(m_order) << ")";
        ss << " -> " << m_read;
        return ss.str();
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<LoadOpDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<LoadOpDesc>(any);
        return std::string{desc};
    }
};

template <typename T>
struct StoreOpDesc
{
    std::memory_order m_order;
    T                 m_stored;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "Store";
        ss << "<" << typestring<T>() << ">";
        ss << " (order: " << orderstring(m_order) << ")";
        ss << " -> " << m_stored;
        return ss.str();
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<StoreOpDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<StoreOpDesc>(any);
        return std::string{desc};
    }
};

template <typename T>
struct ExchangeOpDesc
{
    std::memory_order m_order;
    T                 m_desired;
    T                 m_returned;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "Exchange";
        ss << "<" << typestring<T>() << ">";
        ss << " (order: " << orderstring(m_order) << ",";
        ss << " desired: " << m_desired << ")";
        ss << " -> " << m_returned;
        return ss.str();
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<ExchangeOpDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<ExchangeOpDesc>(any);
        return std::string{desc};
    }
};

template <typename T>
struct CompareExchangeOpDesc
{
    std::memory_order m_success;
    std::memory_order m_failure;
    T                 m_expected;
    T                 m_desired;
    T                 m_read;
    uint32_t          m_returned;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "CompareExchange";
        ss << "<" << typestring<T>() << ">";
        ss << " (success: " << orderstring(m_success) << ",";
        ss << " failure: " << orderstring(m_failure) << ",";
        ss << " expected: " << m_expected << " -> " << m_read << ")";
        ss << " -> " << m_returned;
        return ss.str();
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<CompareExchangeOpDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<CompareExchangeOpDesc>(any);
        return std::string{desc};
    }
};

template <typename T>
struct FetchAddOpDesc
{
    std::memory_order m_order;
    T                 m_delta;
    T                 m_returned;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "FetchAdd";
        ss << "<" << typestring<T>() << ">";
        ss << " (order: " << orderstring(m_order) << ",";
        ss << " delta: " << m_delta << ")";
        ss << " -> " << m_returned;
        return ss.str();
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<FetchAddOpDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<FetchAddOpDesc>(any);
        return std::string{desc};
    }
};

template <typename T>
struct FetchSubOpDesc
{
    std::memory_order m_order;
    T                 m_delta;
    T                 m_returned;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "FetchSub";
        ss << "<" << typestring<T>() << ">";
        ss << " (order: " << orderstring(m_order) << ",";
        ss << " delta: " << m_delta << ")";
        ss << " -> " << m_returned;
        return ss.str();
    }
};

template <typename T>
struct FetchAndOpDesc
{
    std::memory_order m_order;
    T                 m_arg;
    T                 m_returned;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "FetchAnd";
        ss << "<" << typestring<T>() << ">";
        ss << " (order: " << orderstring(m_order) << ",";
        ss << " arg: " << m_arg << ")";
        ss << " -> " << m_returned;
        return ss.str();
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<FetchAndOpDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<FetchAndOpDesc>(any);
        return std::string{desc};
    }
};

template <typename T>
struct FetchOrOpDesc
{
    std::memory_order m_order;
    T                 m_arg;
    T                 m_returned;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "FetchOr";
        ss << "<" << typestring<T>() << ">";
        ss << " (order: " << orderstring(m_order) << ",";
        ss << " arg: " << m_arg << ")";
        ss << " -> " << m_returned;
        return ss.str();
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<FetchOrOpDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<FetchOrOpDesc>(any);
        return std::string{desc};
    }
};

template <typename T>
struct FetchXorOpDesc
{
    std::memory_order m_order;
    T                 m_arg;
    T                 m_returned;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "FetchXor";
        ss << "<" << typestring<T>() << ">";
        ss << " (order: " << orderstring(m_order) << ",";
        ss << " arg: " << m_arg << ")";
        ss << " -> " << m_returned;
        return ss.str();
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<FetchXorOpDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<FetchXorOpDesc>(any);
        return std::string{desc};
    }
};

struct FunctionEnterDesc
{
    operator std::string() const
    {
        return "Function Entered";
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<FunctionEnterDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<FunctionEnterDesc>(any);
        return std::string{desc};
    }
};

template <typename T>
requires (std::is_trivially_copyable_v<T>)
struct FunctionRetDesc
{
    T m_returned;

    operator std::string() const
    {
        std::stringstream ss;
        ss << "Function Returned: ";
        ss << m_returned;
        return ss.str();
    }

    static std::any Decode(std::byte *bytes)
    {
        auto *desc = reinterpret_cast<FunctionRetDesc*>(bytes);
        return std::any{*desc};
    }

    static std::string ToString(std::any any)
    {
        auto desc = any_cast<FunctionRetDesc>(any);
        return std::string{desc};
    }
};

struct AtomicOpDescHeader
{
    std::size_t          m_type_hash;
    const char          *m_name;
    const volatile void *m_addr;
    AtomicOp             m_type;
    uint32_t             m_cpuid_start;
    uint32_t             m_cpuid_end;
    uint64_t             m_tsc_start;
    uint64_t             m_tsc_end;
};

struct AtomicOpDesc
{
    AtomicOpDescHeader m_header;
    std::any           m_desc;
    std::string      (*m_tostring)(std::any);
};

template <std::size_t Size>
class Ringbuffer
{
private:

    std::atomic<std::size_t>    m_tail;
    std::atomic<std::size_t>    m_size;
    std::array<std::byte, Size> m_buffer;

    struct Footer
    {
        std::size_t   m_payload_size;
        std::any    (*m_decoder)(std::byte*);
        std::string (*m_tostring)(std::any);
    };

    std::size_t advance(std::size_t pos, ssize_t delta) noexcept
    {
        std::size_t size = std::size(m_buffer);
        return (size + ((pos + delta) % size)) % size;
    }

    template <typename T>
    std::size_t write_wrapped(std::size_t pos, std::size_t size, T *data) noexcept
    {
        std::size_t left = std::size(m_buffer) - pos;
        std::size_t end = std::min(left, size);
        std::size_t overflow = size - end;
        std::memcpy(m_buffer.data() + pos, reinterpret_cast<std::byte*>(data), end);
        std::memcpy(m_buffer.data(), reinterpret_cast<std::byte*>(data) + end, overflow);
        return advance(pos, size);
    }

    std::size_t read_wrapped(std::size_t pos, std::size_t size, 
        std::size_t& bytes_read, std::byte *out) noexcept
    {
        std::size_t buffsize = m_size.load(std::memory_order_relaxed);
        if(bytes_read + size >= buffsize)
            return 0;

        std::size_t left = std::size(m_buffer) - pos;
        std::size_t end = std::min(left, size);
        std::size_t overflow = size - end;
        std::memcpy(out, m_buffer.data() + pos, end);
        std::memcpy(out + end, m_buffer.data(), overflow);

        bytes_read += size;
        return size;
    }

public:

    template <typename Desc>
    void Push(AtomicOpDescHeader header, Desc&& desc) noexcept
    {
        std::size_t cursor = m_tail;
        std::size_t payload_size = sizeof(Desc);
        std::size_t packet_size = payload_size + sizeof(Footer) + sizeof(AtomicOpDescHeader);
        bool overwrite = 
            (m_size.load(std::memory_order_relaxed) + packet_size) > std::size(m_buffer);

        /* The 'relaxed' semantics cut it for x86 architecture since 
         * stores are never re-ordered with other stores. The point
         * of decreasing the size before overwriting the buffer section
         * is to make sure that we never read from a section that may
         * have been partly updated.
         */
        if(overwrite) {
            m_size.store(std::max(
                static_cast<ssize_t>(m_size - packet_size),
                static_cast<ssize_t>(0)), 
                std::memory_order_relaxed);
        }

        cursor = write_wrapped(cursor, sizeof(header), &header);
        cursor = write_wrapped(cursor, sizeof(Desc), &desc);

        Footer footer{payload_size, &Desc::Decode, &Desc::ToString};
        cursor = write_wrapped(cursor, sizeof(Footer), &footer);

        m_tail.store(cursor, std::memory_order_relaxed);
        m_size.store(std::min(m_size + payload_size, std::size(m_buffer)), 
            std::memory_order_relaxed);
    }

    void ReadLast(std::size_t n, std::ranges::output_range<AtomicOpDesc> auto&& out) noexcept
    {
        std::byte tmp[1024];
        std::size_t bytes_read = 0;
        std::size_t logs_read = 0;
        std::size_t cursor = m_tail.load(std::memory_order_acquire);

        while(logs_read < n) {

            cursor = advance(cursor, -sizeof(Footer));
            std::size_t read = read_wrapped(cursor, sizeof(Footer), bytes_read, tmp);
            if(read == 0)
                break;

            Footer footer = *reinterpret_cast<Footer*>(tmp);
            std::size_t header_size = sizeof(AtomicOpDescHeader);

            cursor = advance(cursor, -(footer.m_payload_size + header_size));
            read = read_wrapped(cursor, sizeof(AtomicOpDescHeader), bytes_read, tmp);
            if(read == 0)
                break;

            AtomicOpDescHeader header = *reinterpret_cast<AtomicOpDescHeader*>(tmp);
            read = read_wrapped(advance(cursor, header_size), footer.m_payload_size,
                bytes_read, tmp);
            if(read == 0)
                break;

            AtomicOpDesc desc{header, footer.m_decoder(tmp), footer.m_tostring};
            std::ranges::copy(std::span{&desc, 1}, std::back_inserter(out));
            logs_read++;
        }
    }
};

class ThreadContext
{
private:

    std::string                  m_thread_name;
    std::thread::id              m_thread;
    Ringbuffer<kTraceBufferSize> m_ring;

public:

    ThreadContext(std::string name, std::thread::id id)
        : m_thread_name{name}
        , m_thread{id}
        , m_ring{}
    {}

    template <typename Desc>
    void Trace(AtomicOpDescHeader header, Desc&& desc) noexcept
    {
        m_ring.Push(header, std::forward<Desc>(desc));
    }

    void ReadLast(std::size_t n, std::ranges::output_range<AtomicOpDesc> auto&& out)
    {
        m_ring.ReadLast(n, out);
    }
};


[[maybe_unused]] inline TLSAllocation<ThreadContext>& GetTLS()
{
    static TLSAllocation s_tracing_tls = AllocTLS<ThreadContext>(false);
    return s_tracing_tls;
}

[[maybe_unused]] inline auto GetCtx()
{
    return GetTLS().GetThreadSpecific(
        GetThreadName(), std::this_thread::get_id());
}

template <bool Strict = false>
inline uint64_t rdtscp_before(uint32_t *out_cpuid)
{
    if constexpr (Strict) {
        asm volatile("lfence");
    }
    unsigned int lo, hi, cpuid;
    asm volatile(
        "rdtscp\n"
        : "=a" (lo), "=d" (hi), "=c" (cpuid)
    );
    *out_cpuid = cpuid;
    return ((uint64_t)hi << 32) | lo;
}

template <bool Strict = false>
inline uint64_t rdtscp_after(uint32_t *out_cpuid)
{
    unsigned int lo, hi, cpuid;
    asm volatile(
        "rdtscp\n"
        : "=a" (lo), "=d" (hi), "=c" (cpuid)
    );
    if constexpr (Strict) {
        asm volatile("lfence");
    }
    *out_cpuid = cpuid;
    return ((uint64_t)hi << 32) | lo;
}

/* When tracing is 'strict', the necessary memory barriers
 * will be placed to ensure that the RDTSCP instructions
 * are not executed out-of-order with the atomic operations
 * that they are timing. This will give more precise results,
 * but will introduce additional synchronization that may
 * impact the behavior of the traced algorithm, making this
 * option only suitable for sequentially consistent atomics.
 * Otherwise, we have to accept that there is some imprecision
 * in the timing data, which doesn't mean that it's not useful.
 */
export
template <typename T, bool Strict = false>
class TracedAtomic
{
private:

    std::atomic<T> m_atomic;
    const char    *m_name;

public:

    constexpr TracedAtomic(const char *name) noexcept(std::is_nothrow_default_constructible_v<T>)
        : m_atomic{}
        , m_name{name}
    {}

    constexpr TracedAtomic(T desired, const char *name) noexcept
        : m_atomic{desired}
        , m_name{name}
    {}

    TracedAtomic(const TracedAtomic&) = delete;

    T operator=(T desired) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        m_atomic.store(desired, std::memory_order_seq_cst);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eStore,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            StoreOpDesc<T>{std::memory_order_seq_cst, desired}
        );
    }

    T operator=(T desired) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        m_atomic.store(desired, std::memory_order_seq_cst);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eStore,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            StoreOpDesc<T>{std::memory_order_seq_cst, desired}
        );
    }

    TracedAtomic& operator=(const TracedAtomic&) = delete;
    TracedAtomic& operator=(const TracedAtomic&) volatile = delete;

    void store(T desired, std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        m_atomic.store(desired, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eStore,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            StoreOpDesc<T>{order, desired}
        );
    }

    void store(T desired, std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        m_atomic.store(desired, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eStore,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            StoreOpDesc<T>{order, desired}
        );
    }

    T load(std::memory_order order = std::memory_order_seq_cst) const noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T ret = m_atomic.load(order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eLoad,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            LoadOpDesc<T>{order, ret}
        );
        return ret;
    }

    T load(std::memory_order order = std::memory_order_seq_cst) const volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T ret = m_atomic.load(order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eLoad,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            LoadOpDesc<T>{order, ret}
        );
        return ret;
    }

    T exchange(T desired, std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T ret = m_atomic.exchange(desired, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            ExchangeOpDesc<T>{order, desired, ret}
        );
        return ret;
    }

    T exchange(T desired, std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T ret = m_atomic.exchange(desired, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            ExchangeOpDesc<T>{order, desired, ret}
        );
        return ret;
    }

    bool compare_exchange_weak(T& expected, T desired,
                               std::memory_order success,
                               std::memory_order failure) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T expected_before = expected;
        bool ret = m_atomic.compare_exchange_weak(expected, desired, success, failure);
        T expected_after = expected;
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eCompareExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            CompareExchangeOpDesc<T>{success, failure, expected_before, 
                desired, expected_after, ret}
        );
        return ret;
    }

    bool compare_exchange_weak(T& expected, T desired,
                               std::memory_order success,
                               std::memory_order failure) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T expected_before = expected;
        bool ret = m_atomic.compare_exchange_weak(expected, desired, success, failure);
        T expected_after = expected;
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eCompareExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            CompareExchangeOpDesc<T>{success, failure, expected_before, 
                desired, expected_after, ret}
        );
        return ret;
    }

    bool compare_exchange_weak(T& expected, T desired,
                               std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T expected_before = expected;
        bool ret = m_atomic.compare_exchange_weak(expected, desired, order);
        T expected_after = expected;
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eCompareExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            CompareExchangeOpDesc<T>{order, order, expected_before, 
                desired, expected_after, ret}
        );
        return ret;
    }

    bool compare_exchange_weak(T& expected, T desired,
                               std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T expected_before = expected;
        bool ret = m_atomic.compare_exchange_weak(expected, desired, order);
        T expected_after = expected;
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eCompareExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            CompareExchangeOpDesc<T>{order, order, expected_before, 
                desired, expected_after, ret}
        );
        return ret;
    }

    bool compare_exchange_strong(T& expected, T desired,
                                 std::memory_order success,
                                 std::memory_order failure) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T expected_before = expected;
        bool ret = m_atomic.compare_exchange_strong(expected, desired, success, failure);
        T expected_after = expected;
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eCompareExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            CompareExchangeOpDesc<T>{success, failure, expected_before, 
                desired, expected_after, ret}
        );
        return ret;
    }

    bool compare_exchange_strong(T& expected, T desired,
                                 std::memory_order success,
                                 std::memory_order failure) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T expected_before = expected;
        bool ret = m_atomic.compare_exchange_strong(expected, desired, success, failure);
        T expected_after = expected;
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eCompareExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            CompareExchangeOpDesc<T>{success, failure, expected_before, 
                desired, expected_after, ret}
        );
        return ret;
    }

    bool compare_exchange_strong(T& expected, T desired,
                                 std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T expected_before = expected;
        bool ret = m_atomic.compare_exchange_strong(expected, desired, order);
        T expected_after = expected;
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eCompareExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            CompareExchangeOpDesc<T>{order, order, expected_before, 
                desired, expected_after, ret}
        );
        return ret;
    }

    bool compare_exchange_strong(T& expected, T desired,
                                 std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        T expected_before = expected;
        bool ret = m_atomic.compare_exchange_strong(expected, desired, order);
        T expected_after = expected;
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(T).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eCompareExchange,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            CompareExchangeOpDesc<T>{order, order, expected_before, 
                desired, expected_after, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_floating_point_v<U>)
    U fetch_add(U arg,
                std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_add(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchAdd,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchAddOpDesc<U>{order, arg, ret}
        );
        return ret;
    }
        
    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_floating_point_v<U>)
    U fetch_add(U arg,
                std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_add(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchAdd,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchAddOpDesc<U>{order, arg, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_pointer_v<U>)
    U fetch_add(std::ptrdiff_t arg,
                std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_add(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchAdd,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchAddOpDesc<U>{order, arg, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_pointer_v<U>)
    U fetch_add(std::ptrdiff_t arg,
                std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_add(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchAdd,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchAddOpDesc<U>{order, arg, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_floating_point_v<U>)
    U fetch_sub(U arg,
                std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_sub(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchSub,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchSubOpDesc<U>{order, arg, ret}
        );
        return ret;
    }
        
    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_floating_point_v<U>)
    U fetch_sub(U arg,
                std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_sub(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchSub,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchSubOpDesc<U>{order, arg, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_pointer_v<U>)
    U fetch_sub(std::ptrdiff_t arg,
                std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_sub(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<void*>(&m_atomic),
                .m_type = AtomicOp::eFetchSub,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchSubOpDesc<U>{order, arg, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_pointer_v<U>)
    U fetch_sub(std::ptrdiff_t arg,
                std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_sub(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchSub,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchSubOpDesc<U>{order, arg, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_integral_v<U>)
    U fetch_and(U arg,
                std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_and(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchAnd,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchAndOpDesc<U>{order, arg, ret}
        );
        return ret;
    }
        
    template <typename U = T>
    requires (std::is_integral_v<U>)
    U fetch_and(U arg,
                std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_and(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchAnd,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchAndOpDesc<U>{order, arg, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_integral_v<U>)
    U fetch_or(U arg,
                std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_or(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchAnd,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchAndOpDesc<U>{order, arg, ret}
        );
        return ret;
    }
        
    template <typename U = T>
    requires (std::is_integral_v<U>)
    U fetch_or(U arg,
                std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_or(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchOr,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchOrOpDesc<U>{order, arg, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_integral_v<U>)
    U fetch_xor(U arg,
                std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_xor(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchXor,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchXorOpDesc<U>{order, arg, ret}
        );
        return ret;
    }
        
    template <typename U = T>
    requires (std::is_integral_v<U>)
    U fetch_xor(U arg,
                std::memory_order order = std::memory_order_seq_cst) volatile noexcept
    {
        uint32_t cpu_before, cpu_after;
        uint64_t before = rdtscp_before<Strict>(&cpu_before);
        U ret = m_atomic.fetch_xor(arg, order);
        uint64_t after = rdtscp_after<Strict>(&cpu_after);

        GetCtx()->Trace(AtomicOpDescHeader{
                .m_type_hash = typeid(U).hash_code(),
                .m_name = m_name,
                .m_addr = reinterpret_cast<const volatile void*>(&m_atomic),
                .m_type = AtomicOp::eFetchXor,
                .m_cpuid_start = cpu_before,
                .m_cpuid_end = cpu_after,
                .m_tsc_start = before,
                .m_tsc_end = after
            },
            FetchXorOpDesc<U>{order, arg, ret}
        );
        return ret;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_pointer_v<U>)
    U operator++() noexcept
    {
        return fetch_add(1) + 1;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_pointer_v<U>)
    U operator++() volatile noexcept
    {
        return fetch_add(1) + 1;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_pointer_v<U>)
    U operator++(int) noexcept
    {
        return fetch_add(1);
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_pointer_v<U>)
    U operator++(int) volatile noexcept
    {
        return fetch_add(1);
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_pointer_v<U>)
    U operator--() noexcept
    {
        return fetch_sub(1) - 1;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_integral_v<U>)
    U operator--() volatile noexcept
    {
        return fetch_sub(1) - 1;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_integral_v<U>)
    U operator--(int) noexcept
    {
        return fetch_sub(1);
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_floating_point_v<U>)
    U operator--(int) volatile noexcept
    {
        return fetch_sub(1);
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_floating_point_v<U>)
    U operator+=(U arg) noexcept
    {
        return fetch_add(arg) + arg;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_floating_point_v<U>)
    U operator+=(U arg) volatile noexcept
    {
        return fetch_add(arg) + arg;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_floating_point_v<U>)
    U operator-=(U arg) noexcept
    {
        return fetch_sub(arg) - arg;
    }

    template <typename U = T>
    requires (std::is_integral_v<U> or std::is_floating_point_v<U>)
    U operator-=(U arg) volatile noexcept
    {
        return fetch_sub(arg) - arg;
    }

    template <typename U = T>
    requires (std::is_pointer_v<U>)
    U operator+=(std::ptrdiff_t arg) noexcept
    {
        return fetch_add(arg) + arg;
    }

    template <typename U = T>
    requires (std::is_pointer_v<U>)
    U operator+=(std::ptrdiff_t arg) volatile noexcept
    {
        return fetch_add(arg) + arg;
    }

    template <typename U = T>
    requires (std::is_pointer_v<U>)
    U operator-=(std::ptrdiff_t arg) noexcept
    {
        return fetch_sub(arg) - arg;
    }

    template <typename U = T>
    requires (std::is_pointer_v<U>)
    U operator-=(std::ptrdiff_t arg) volatile noexcept
    {
        return fetch_sub(arg) - arg;
    }

    template <typename U = T>
    requires (std::is_integral_v<U>)
    U operator&=(U arg) noexcept
    {
        return fetch_and(arg) & arg;
    }

    template <typename U = T>
    requires (std::is_integral_v<U>)
    U operator&=(U arg) volatile noexcept
    {
        return fetch_and(arg) & arg;
    }

    template <typename U = T>
    requires (std::is_integral_v<U>)
    U operator|=(U arg) noexcept
    {
        return fetch_or(arg) | arg;
    }

    template <typename U = T>
    requires (std::is_integral_v<U>)
    U operator|=(U arg) volatile noexcept
    {
        return fetch_or(arg) | arg;
    }

    template <typename U = T>
    requires (std::is_integral_v<U>)
    U operator^=(U arg) noexcept
    {
        return fetch_xor(arg) ^ arg;
    }

    template <typename U = T>
    requires (std::is_integral_v<U>)
    U operator^=(U arg) volatile noexcept
    {
        return fetch_xor(arg) ^ arg;
    }
};

} //namespace pe

export using ::dump_atomic_trace;

extern "C" [[maybe_unused]] void dump_atomic_trace(int n)
{
    std::vector<pe::AtomicOpDesc> descs;
    auto ptrs = pe::GetTLS().GetThreadPtrsSnapshot();
    for(auto ptr : ptrs) {
        ptr->ReadLast(n, descs);
    }
    for(const auto& desc : descs) {
        pe::ioprint_unlocked(pe::TextColor::eWhite, " ", false, true, 
            " --> Atomic Operation:", 
            static_cast<uint32_t>(desc.m_header.m_type), 
            "at address", const_cast<void*>(desc.m_header.m_addr), pe::fmt::cat{}, ":",
            desc.m_tostring(desc.m_desc));
    }
}

