export module concurrency;

import platform;
import logger;
import tls;

import <atomic>;
import <string>;
import <vector>;
import <algorithm>;
import <variant>;
import <memory>;

namespace pe{

/*****************************************************************************/
/* ATOMIC SCOPED LOCK                                                        */
/*****************************************************************************/

export
class AtomicScopedLock
{
private:

    std::atomic_flag& m_flag;

public:

    AtomicScopedLock(std::atomic_flag& flag)
        : m_flag{flag}
    {
        pe::dbgtime([&](){
            while(m_flag.test_and_set(std::memory_order_acquire));
        }, [](uint64_t delta) {
            if (delta > 5000) [[unlikely]] {
                pe::ioprint(pe::LogLevel::eWarning, "Acquiring atomic lock took", delta, "cycles.",
                    "(" + std::to_string(pe::rdtsc_usec(delta)) + " usec)");
            }
        });
    }

    ~AtomicScopedLock()
    {
        m_flag.clear(std::memory_order_release);
    }
};

/*****************************************************************************/
/* SYNCHRONIZED YIELD VALUE                                                  */
/*****************************************************************************/
/*
 * Allows safe interleaved assignment and fetching from
 * different threads. Note that it is assumed that each
 * yielded value is always consumed exactly once.
 */
export
template <typename T>
class SynchronizedYieldValue
{
private:

    using value_type = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

    std::atomic_flag                 m_empty;
    [[no_unique_address]] value_type m_value;

public:

    SynchronizedYieldValue()
        : m_empty{true}
        , m_value{}
    {}

    template <typename U = T>
    void Yield(U&& value) requires (!std::is_void_v<U>)
    {
        /* Wait until the value is consumed */
        pe::dbgtime([&](){
            while(!m_empty.test(std::memory_order_acquire));
        }, [](uint64_t delta) {
            if (delta > 5000) [[unlikely]] {
                pe::ioprint(pe::LogLevel::eWarning, "Yielding took", delta, "cycles.",
                    "(" + std::to_string(pe::rdtsc_usec(delta)) + " usec)");
            }
        });
        m_value = std::forward<T>(value);
        m_empty.clear(std::memory_order_release);
    }

    template <typename U = T>
    T Consume() requires (!std::is_void_v<U>)
    {
        /* Wait until the value is yielded */
        pe::dbgtime([&](){
            while(m_empty.test(std::memory_order_acquire));
        }, [](uint64_t delta) {
            if (delta > 5000) [[unlikely]] {
                pe::ioprint(pe::LogLevel::eWarning, "Consuming took", delta, "cycles.",
                    "(" + std::to_string(pe::rdtsc_usec(delta)) + " usec)");
            }
        });
        T ret = m_value;
        m_empty.test_and_set(std::memory_order_release);
        return ret;
    }
};

/*****************************************************************************/
/* SYNCHRONIZED SINGLE YIELD VALUE                                           */
/*****************************************************************************/
/*
 * Same as SynchronizedYieldValue, but allowing the producer
 * to yield the value only a single time. All subsequent yields 
 * will be no-ops.
 */
export
template <typename T>
class SynchronizedSingleSetValue
{
private:

    T                m_value{};
    std::atomic_flag m_empty{true};

public:

    SynchronizedSingleSetValue(T&& initial)
        : m_value{std::forward<T>(initial)}
        , m_empty{false}
    {}

    bool Set(T&& value)
    {
        if(!m_empty.test(std::memory_order_acquire))
            return false;
        m_value = std::forward<T>(value);
        m_empty.clear(std::memory_order_release);
        return true;
    }

    T Ptr()
    {
        while(m_empty.test(std::memory_order_acquire));
        return m_value;
    }
};

/*****************************************************************************/
/* DOUBLE QUAD WORD ATOMIC                                                   */
/*****************************************************************************/

/* std::atomic holding a 128-bit 16-byte aligned value is
 * not guaranteed to be lockfree on all platforms that we
 * want to support. A lockfree implementation requires
 * the CMPXCH16B instruction to be available.
 */
export
template <typename T>
concept DoubleQuadWordAtomicCompatible = requires {

    requires (std::is_trivially_copyable_v<T>);
    requires (std::is_copy_constructible_v<T>);
    requires (std::is_move_constructible_v<T>);
    requires (std::is_copy_assignable_v<T>);
    requires (std::is_move_assignable_v<T>);

    requires (std::alignment_of_v<T> == 16);
    requires (sizeof(T) == 16);
};

static std::atomic_flag s_supported{std::invoke(
    [](){
        uint32_t eax, ebx, ecx, edx;
        asm volatile(
            "cpuid\n"
            : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
            : "a" (0x0), "c" (0)
        );
        /* Check the CMPXCH16B feature bit */
        return !!(ecx & (0b1 << 13));
    }
)};

export
template<DoubleQuadWordAtomicCompatible T>
class DoubleQuadWordAtomic
{
private:

    T m_value{};
    std::atomic<T> m_fallback;

    static inline constexpr uint64_t *low_qword(T& value)
    {
        auto ptr = reinterpret_cast<uint64_t*>(&value);
        return std::launder(ptr + 0);
    }

    static inline constexpr uint64_t *high_qword(T& value)
    {
        auto ptr = reinterpret_cast<uint64_t*>(&value);
        return std::launder(ptr + 1);
    }

public:

    template <typename... Args>
    DoubleQuadWordAtomic(Args... args)
        : m_value{T{args...}}
        , m_fallback{T{args...}}
    {}

    DoubleQuadWordAtomic() = default;
    DoubleQuadWordAtomic(T value)
        : m_value{value}
        , m_fallback{value}
    {}

    DoubleQuadWordAtomic(DoubleQuadWordAtomic const&) = delete;
    DoubleQuadWordAtomic& operator=(DoubleQuadWordAtomic const&) = delete;

    DoubleQuadWordAtomic(DoubleQuadWordAtomic&&) = delete;
    DoubleQuadWordAtomic& operator=(DoubleQuadWordAtomic&&) = delete;

    inline void Store(T desired, 
        std::memory_order order = std::memory_order_seq_cst) noexcept
    {
        if (!s_supported.test()) [[unlikely]] {
            m_fallback.store(desired, order);
            return;
        }

        asm volatile(
            "movq %1, %%xmm0\n\t"
            "movq %2, %%xmm1\n\t"
            "punpcklqdq %%xmm1, %%xmm0\n\t"
            "movdqa %%xmm0, %0\n"
            : "+m" (m_value)
            : "r" (*low_qword(desired)), "r" (*high_qword(desired))
            : "xmm0", "xmm1"
        );
        std::atomic_thread_fence(order);
    }

    inline T Load(std::memory_order order = std::memory_order_seq_cst) const noexcept
    {
        if (!s_supported.test(std::memory_order_relaxed)) [[unlikely]] {
            return m_fallback.load(order);
        }

        T ret;
        std::atomic_thread_fence(order);
        asm volatile(
            "movdqa %2, %%xmm0\n\t"
            "movq %%xmm0, %0\n\t"
            "movhlps %%xmm0, %%xmm0\n\t"
            "movq %%xmm0, %1\n"
            : "+r" (*low_qword(ret)), "+r" (*high_qword(ret))
            : "m" (m_value)
            : "xmm0"
        );
        return ret;
    }

    inline bool CompareExchange(T& expected, T desired,
        std::memory_order success = std::memory_order_seq_cst,
        std::memory_order failure = std::memory_order_seq_cst) noexcept
    {
        bool result;
        if (!s_supported.test(std::memory_order_relaxed)) [[unlikely]] {
            return m_fallback.compare_exchange_strong(expected, desired, 
                success, failure);
        }

        /* The CMPXCH16B instruction will atomically load the existing 
         * value into RDX:RAX if the comparison fails. Add a fence to 
         * sequence the load.
         */
        std::atomic_thread_fence(failure);
        asm volatile(
            "lock cmpxchg16b %1\n"
            : "=@ccz" (result) , "+m" (m_value)
            , "+a" (*low_qword(expected)), "+d" (*high_qword(expected))
            :  "b" (*low_qword(desired)),   "c" (*high_qword(desired))
            : "cc"
        );
        if(result) {
            /* We have successfully written the value to memory. Place
             * a fence to "publish" the new value. 
             */
            std::atomic_thread_fence(success);
        }
        return result;
    }
};

/*****************************************************************************/
/* HAZARD POINTER                                                            */
/*****************************************************************************/

/* Implementation based on the paper "Hazard Pointers: Safe Memory 
 * Reclamation for Lock-Free Objects" by Maged M. Michael. Must be
 * a singleton due to the use of thread-local objects. The 'Tag'
 * template parameter can be used for instantiating multiple 
 * instances in a static fashion.
 */
export
template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
struct HPContext
{
private:

    struct HPRecord
    {
        std::atomic<NodeType*> m_hp[K]{};
        std::atomic<HPRecord*> m_next{};
        std::atomic<bool>      m_active{};

        /* Only touched by the owning thread */
        std::vector<NodeType*> m_rlist{};
        std::size_t            m_rcount{};

        static_assert(std::remove_reference_t<decltype(m_hp[0])>::is_always_lock_free);
        static_assert(decltype(m_next)::is_always_lock_free);
        static_assert(decltype(m_active)::is_always_lock_free);
    };

    class HazardPtr
    {
    private:

        friend struct HPContext;

        NodeType  *m_raw; 
        int        m_idx;
        HPContext& m_ctx;

        HazardPtr(NodeType *raw, int index, HPContext& ctx)
            : m_raw{raw}
            , m_idx{index}
            , m_ctx{ctx}
        {}

    public:

        HazardPtr(HPContext& ctx)
            : m_raw{}
            , m_idx{}
            , m_ctx{ctx}
        {}

        HazardPtr(HazardPtr const&) = delete;
        HazardPtr& operator=(HazardPtr const&) = delete;

        HazardPtr(HazardPtr&& other)
            : m_raw{other.m_raw}
            , m_idx{other.m_idx}
            , m_ctx{other.m_ctx}
        {
            other.m_raw = nullptr;
        }

        HazardPtr& operator=(HazardPtr&& other)
        {
            m_raw = other.m_raw;
            m_idx = other.m_idx;
            other.m_raw = nullptr;
            return *this;
        }

        ~HazardPtr()
        {
            if(m_raw) {
                m_ctx.ReleaseHazard(m_idx);
            }
        }

        NodeType* operator->() const noexcept
        {
            return m_raw;
        }

        NodeType* operator*() const noexcept
        {
            return m_raw;
        }
    };

    struct HPRecordGuard
    {
    private:

        HPContext&               m_ctx;
        HPRecord                *m_record;
        std::atomic_flag         m_delete;

    public:

        HPRecordGuard(HPRecordGuard&&) = delete;
        HPRecordGuard(HPRecordGuard const&) = delete;
        HPRecordGuard& operator=(HPRecordGuard&&) = delete;
        HPRecordGuard& operator=(HPRecordGuard const&) = delete;

        HPRecordGuard(HPContext& ctx)
            : m_ctx{ctx}
            , m_record{ctx.AllocateHPRecord()}
            , m_delete{false}
        {}

        ~HPRecordGuard()
        {
            m_ctx.RetireHPRecord(m_record);
        }

        HPRecord *Ptr() const
        {
            return m_record;
        }
    };

private:

    std::atomic<HPRecord*>       m_head;
    std::atomic_int              m_H;

    TLSAllocation<HPRecordGuard> t_myhprec;

    static_assert(decltype(m_head)::is_always_lock_free);
    static_assert(decltype(m_H)::is_always_lock_free);

private:

    HPContext(HPContext&&) = delete;
    HPContext(HPContext const&) = delete;
    HPContext& operator=(HPContext&&) = delete;
    HPContext& operator=(HPContext const&) = delete;

    void ReleaseHazard(int index);

    HPRecord *AllocateHPRecord();
    void RetireHPRecord(HPRecord *record);

    void Scan(HPRecord *head);
    void HelpScan();

public:

    using hazard_ptr_type = HazardPtr;

    HPContext()
        : m_head{}
        , m_H{}
        , t_myhprec{AllocTLS<HPRecordGuard>()}
    {}

    ~HPContext();

    [[nodiscard]] hazard_ptr_type AddHazard(int index, NodeType *node);
    void RetireHazard(NodeType *node);
};

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::ReleaseHazard(int index)
{
    auto ptr = t_myhprec.GetThreadSpecific(*this);
    ptr->Ptr()->m_hp[index].store(nullptr, std::memory_order_release);
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
typename HPContext<NodeType, K, R>::HPRecord *HPContext<NodeType, K, R>::AllocateHPRecord()
{
    /* First try to use a retired HP record */
    HPRecord *hprec;
    for(hprec = m_head.load(std::memory_order_acquire); 
        hprec; 
        hprec = hprec->m_next.load(std::memory_order_acquire)) {

        if(hprec->m_active.load(std::memory_order_acquire))
            continue;
        bool expected = false;
        if(!hprec->m_active.compare_exchange_weak(expected, true,
            std::memory_order_release, std::memory_order_relaxed))
            continue;
        /* Succeeded in locking an inactive HP record */
        return hprec;
    }

    /* No HP avaiable for reuse. Increment H, then allocate 
     * a new HP and push it.
     */
    int oldcount = m_H.load(std::memory_order_relaxed);
    while(!m_H.compare_exchange_weak(oldcount, oldcount + K,
        std::memory_order_release, std::memory_order_relaxed));

    /* Allocate and push a new HPRecord */
    hprec = new HPRecord{};
    hprec->m_active.store(true, std::memory_order_release);

    HPRecord *oldhead = m_head.load(std::memory_order_relaxed);
    do{
        hprec->m_next.store(oldhead, std::memory_order_release);
    }while(!m_head.compare_exchange_weak(oldhead, hprec, 
        std::memory_order_release, std::memory_order_relaxed));

    return hprec;
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::RetireHPRecord(HPRecord *record)
{
    for(int i = 0; i < K; i++)
        record->m_hp[i].store(nullptr, std::memory_order_release);
    record->m_active.store(false, std::memory_order_release);
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::Scan(HPRecord *head)
{
    /* Stage 1: Scan HP list and insert non-null values in plist */
    std::vector<NodeType*> plist;
    HPRecord *hprec = m_head.load(std::memory_order_acquire);
    while(hprec) {
        for(int i = 0; i < K; i++) {
            NodeType *hptr = hprec->m_hp[i].load(std::memory_order_acquire);
            if(hptr)
                plist.push_back(hptr);
        }
        hprec = hprec->m_next.load(std::memory_order_acquire);
    }
    std::sort(plist.begin(), plist.end());

    /* Stage 2: Search plist */
    HPRecord *myrec = t_myhprec.GetThreadSpecific(*this)->Ptr();
    std::vector<NodeType*> tmplist = std::move(myrec->m_rlist);
    myrec->m_rlist.clear();
    myrec->m_rcount = 0;

    auto node = tmplist.cbegin();
    while(node != tmplist.cend()) {
        if(std::binary_search(plist.begin(), plist.end(), *node)) {
            myrec->m_rlist.push_back(*node);
            myrec->m_rcount++;
        }else{
            delete *node;
        }
        node++;
    }
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::HelpScan()
{
    HPRecord *hprec;
    for(hprec = m_head.load(std::memory_order_acquire); 
        hprec; 
        hprec = hprec->m_next.load(std::memory_order_acquire)) {

        if(hprec->m_active.load(std::memory_order_acquire))
            continue;

        /* Acquire-Release ordering is required here to guaranteed 
         * that changes to rlist from another thread running HelpScan
         * are visible.
         */
        bool expected = false;
        if(!hprec->m_active.compare_exchange_weak(expected, true,
            std::memory_order_acq_rel, std::memory_order_relaxed))
            continue;

        auto it = hprec->m_rlist.cbegin();
        for(; it != hprec->m_rlist.cend(); it++) {

            NodeType *node = *it;
            HPRecord *myrec = t_myhprec.GetThreadSpecific(*this)->Ptr();
            myrec->m_rlist.push_back(node);
            myrec->m_rcount++;

            if(myrec->m_rcount >= R)
                Scan(m_head.load(std::memory_order_relaxed));
        }
        hprec->m_rlist.clear();
        hprec->m_rcount = 0;

        hprec->m_active.store(false, std::memory_order_release);
    }
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
HPContext<NodeType, K, R>::~HPContext()
{
    t_myhprec.ClearAllThreadSpecific();

    HPRecord *hprec = m_head.load(std::memory_order_acquire);
    while(hprec) {

        while(hprec->m_active.load(std::memory_order_acquire));

        auto it = hprec->m_rlist.cbegin();
        for(; it != hprec->m_rlist.cend(); it++) {
            if(*it)
                delete *it;
        }
        HPRecord *old = hprec;
        hprec = hprec->m_next.load(std::memory_order_acquire);
        delete old;
    }
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
[[nodiscard]] typename HPContext<NodeType, K, R>::hazard_ptr_type 
HPContext<NodeType, K, R>::AddHazard(int index, NodeType *node)
{
    if(index >= K) [[unlikely]]
        throw std::out_of_range{"Hazard index out of range."};

    auto ptr = t_myhprec.GetThreadSpecific(*this);
    ptr->Ptr()->m_hp[index].store(node, std::memory_order_release);
    return {node, index, *this};
}

template <typename NodeType, std::size_t K, std::size_t R>
requires (R <= K)
void HPContext<NodeType, K, R>::RetireHazard(NodeType *node)
{
    HPRecord *myrec = t_myhprec.GetThreadSpecific(*this)->Ptr();
    myrec->m_rlist.push_back(node);
    myrec->m_rcount++;

    HPRecord *head = m_head.load(std::memory_order_relaxed);
    if(myrec->m_rcount >= R) {
        Scan(head);
        HelpScan();
    }
}

export 
template <typename NodeType, std::size_t K, std::size_t R>
using HazardPtr = typename HPContext<NodeType, K, R>::hazard_ptr_type;

}; // namespace pe

