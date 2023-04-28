export module alloc;

import assert;
import platform;
import concurrency;
import logger;

import <cmath>;
import <array>;

namespace pe{

inline constexpr unsigned kMinBlockSize = 8;
inline constexpr unsigned kMaxBlockSize = 16 * 1024;
inline constexpr unsigned kTotalVirtualMemory = 32 * 1024 * 1024;

template <unsigned x>
inline consteval unsigned log2()
{
    if constexpr(x == 1)
        return 1;
    return 1 + log2<x/2>();
}

template <unsigned x>
inline consteval unsigned pow2()
{
    if constexpr(x == 0)
        return 1;
    if constexpr(x == 1)
        return 2;
    else
        return 2 * pow2<x-1>();
}

template <unsigned x>
inline consteval bool valid_block_size()
{
    if constexpr(x < kMinBlockSize || x > kMaxBlockSize)
        return false;
    if constexpr(x % sizeof(uintptr_t))
        return false;
    return true;
}

template <unsigned x>
inline consteval unsigned num_size_classes()
{
    if constexpr(x < log2<kMinBlockSize>())
        return 1;
    else{
        return (valid_block_size<pow2<x>()>() ? 1 : 0)
             + (valid_block_size<pow2<x>() + pow2<x-2>()>() ? 1 : 0)
             + (valid_block_size<pow2<x>() + pow2<x-1>()>() ? 1 : 0)
             + (valid_block_size<pow2<x>() + pow2<x-1>() + pow2<x-2>()>() ? 1 : 0)
             + num_size_classes<x - 1>();
    }
}

export
inline constinit unsigned kNumSizeClasses = num_size_classes<log2<kMaxBlockSize>()>();

template <std::size_t Start, std::size_t End, std::size_t Inc, typename F>
constexpr inline void constexpr_for(F&& lambda)
{
    if constexpr (Start < End) {
        lambda.template operator()<Start>();
        constexpr_for<Start + Inc, End, Inc>(lambda);
    }
}

inline constinit auto size_classes{[]() constexpr{
    int j = 1;
    std::array<unsigned, num_size_classes<log2<kMaxBlockSize>()>() + 1> sc{};
    constexpr_for<3, log2<kMaxBlockSize>()+1, 1>([&]<std::size_t X>{
        if(valid_block_size<pow2<X>()>())
            sc[j++] = pow2<X>();
        if(valid_block_size<pow2<X>() + pow2<X-2>()>())
            sc[j++] = pow2<X>() + pow2<X-2>();
        if(valid_block_size<pow2<X>() + pow2<X-1>()>())
            sc[j++] = pow2<X>() + pow2<X-1>();
        if(valid_block_size<pow2<X>() + pow2<X-1>() + pow2<X-2>()>())
            sc[j++] = pow2<X>() + pow2<X-1>() + pow2<X-2>();
    });
    return sc;
}()};

std::size_t compute_size_class(std::size_t size)
{
    if(size > kMaxBlockSize)
        return 0;
    if(size == kMaxBlockSize)
        return kNumSizeClasses;
    return std::distance(std::begin(size_classes),
        std::upper_bound(std::begin(size_classes), std::end(size_classes), size));
}

enum class SuperblockState
{
    /* If every block in the superblock is being used by a thread cache or by the application */
    eFull,
    /* If the superblock has some used blocks and some free blocks remaining */
    ePartial,
    /* If the superblock has only free blocks */
    eEmpty
};

struct alignas(16) SuperblockAnchor
{
    uint64_t m_state : 2;
    uint64_t m_avail : 31;
    uint64_t m_count : 31;
};
using AtomicSuperblockAnchor = DoubleQuadWordAtomic<SuperblockAnchor>;

struct alignas(16) SuperblockDescriptor
{
    AtomicSuperblockAnchor m_anchor;
    std::uintptr_t         m_superblock;
    std::size_t            m_blocksize;
    std::size_t            m_maxcount;
    std::size_t            m_sizeclass;
};

struct ThreadCache
{

};

void *allocate_large_block(std::size_t size)
{
    return nullptr;
}

void *malloc(std::size_t size)
{
    std::size_t sc = compute_size_class(size);
    if(sc == 0)
        return allocate_large_block(size);
    return nullptr;
}

void free(void *ptr)
{

}

} // namespace pe

