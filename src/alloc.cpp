export module alloc;

import assert;
import platform;
import concurrency;
import logger;
import mman;
import meta;
import lockfree_stack;

import <cmath>;
import <array>;
import <concepts>;

namespace pe{

inline constexpr unsigned kMinBlockSize = 8;
inline constexpr unsigned kMaxBlockSize = 16 * 1024;
inline constexpr unsigned kSuperblockSize = kMaxBlockSize;

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
inline constinit const unsigned kNumSizeClasses = num_size_classes<log2<kMaxBlockSize>()>();

inline constinit auto s_size_classes{[]() constexpr{
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

/*****************************************************************************/
/* SUPERBLOCK STATE                                                          */
/*****************************************************************************/

enum class SuperblockState
{
    /* If every block in the superblock is being used by a thread cache or by the application */
    eFull,
    /* If the superblock has some used blocks and some free blocks remaining */
    ePartial,
    /* If the superblock has only free blocks */
    eEmpty
};

/*****************************************************************************/
/* SUPERBLOCK ANCHOR                                                         */
/*****************************************************************************/

struct alignas(16) SuperblockAnchor
{
    uint64_t m_state : 2;
    uint64_t m_avail : 31; /* index of the first free block */
    uint64_t m_count : 31; /* number of free blocks */
};
using AtomicSuperblockAnchor = std::atomic<SuperblockAnchor>;

/*****************************************************************************/
/* SUPERBLOCK DESCRIPTOR                                                     */
/*****************************************************************************/

export // TODO: temp export
struct alignas(16) SuperblockDescriptor
{
    AtomicSuperblockAnchor m_anchor;
    std::uintptr_t         m_superblock; /* pointer to superblock */
    std::size_t            m_blocksize;  /* size of each block in superblock */
    std::size_t            m_maxcount;   /* number of blocks */
    std::size_t            m_sizeclass;  /* size class of blocks in superblock */
};

static_assert(std::is_standard_layout_v<SuperblockDescriptor>);

/*****************************************************************************/
/* DESCRIPTOR FREELIST                                                       */
/*****************************************************************************/
/*
 * The superblock descriptors can be recycled, but the memory allocated for
 * them is not returned to the OS until the application terminates.
 */

export // TODO: temp export
class DescriptorFreelist
{
private:

    static constexpr unsigned kDescriptorBlockSize = 16384; 

    struct DescriptorNode
    {
        SuperblockDescriptor         m_desc;
        std::atomic<DescriptorNode*> m_next;
    };

    std::atomic<DescriptorNode*> m_freehead;

    constexpr std::size_t descs_per_block();
    DescriptorNode       *initialize_block(void *ptr);

public:

    DescriptorFreelist();

    SuperblockDescriptor &Allocate();
    void                  Retire(SuperblockDescriptor& desc);
};

/*****************************************************************************/
/* HEAP                                                                      */
/*****************************************************************************/

class Heap
{
    // TODO:
    // - list of 'free' superblocks
    // - list of 'partial' superblocks

    SuperblockDescriptor& GetPartialSB();
    void                  PutPartialSB(SuperblockDescriptor& desc);
};

/*****************************************************************************/
/* PAGEMAP                                                                   */
/*****************************************************************************/

class Pagemap
{

};

/*****************************************************************************/
/* BLOCK FREELIST                                                            */
/*****************************************************************************/
/*
 * A freelist of blocks for a specific size class.
 */
class BlockFreelist
{
private:

    Heap&       m_heap;
    std::size_t m_sizeclass;

    /* lock-free stack of descriptors */

    void FillFromPartialSB();
    void FillFromNewSB();

public:

    BlockFreelist(Heap&, std::size_t);

    void  Fill();
    void  Flush();
    bool  IsEmpty();
    bool  IsFull();
    void *PopBlock();
};

/*****************************************************************************/
/* SIMPLE SEGREGATED STORAGE                                                 */
/*****************************************************************************/
/*
 * Holds a sepagage block freelist for each size class.
 */
class SimpleSegregatedStorage
{
private:

    std::array<BlockFreelist, kNumSizeClasses> m_lists;

    template<typename T, std::size_t N, std::size_t... I>
    constexpr auto create_array_impl(Heap& heap, decltype(s_size_classes) scs, 
        std::index_sequence<I...>)
    {
        return std::array<T, N>{ BlockFreelist{heap, scs[I]}... };
    }

    template<typename T, std::size_t N>
    constexpr auto create_array(Heap& heap, decltype(s_size_classes) scs)
    {
        return create_array_impl<T, N>(heap, scs, std::make_index_sequence<N>{});
    }

public:

    SimpleSegregatedStorage(Heap& heap)
        : m_lists{ create_array<BlockFreelist, kNumSizeClasses>(heap, s_size_classes) }
    {}

    BlockFreelist& GetForSizeClass(std::size_t sc)
    {
        return m_lists[sc];
    }
};

/*****************************************************************************/
/* THREAD CACHE                                                              */
/*****************************************************************************/
/*
 * Thread-local cache of blocks for different size classes. 
 * Ultimately, the thread cahes' goal is to provide extremely
 * fast, synchronization-free memory allocations and deallocations.
 * It does so by ensuring that most malloc() calls are just simple
 * stack pops and most free() calls are just simple stack pushes.
 * This simple and speed-efficient average case is needed to
 * amortize the costs of synchronization that is necessary to
 * transfer blocks to and from the thread caches.
 */
class ThreadCache
{
private:

    SimpleSegregatedStorage m_blocklists;
    [[maybe_unused]] Heap&  m_heap;

public:

    ThreadCache(Heap& heap)
        : m_blocklists{heap}
        , m_heap{heap}
    {}

    BlockFreelist& GetBlocksForSizeClass(std::size_t sc)
    {
        if(sc >= kNumSizeClasses)
            throw std::out_of_range{"Invalid size class."};
        return m_blocklists.GetForSizeClass(sc);
    }
};

/*****************************************************************************/
/* ALLOCATOR                                                                 */
/*****************************************************************************/
/*
 * A lock-free general-purpose memory allocator, based on the 
 * paper of LRMalloc.
 */
export
class Allocator
{
private:

    static inline Heap                     m_heap{};
    static inline Pagemap                  m_pagemap{};
    static inline thread_local ThreadCache m_thread_cache{m_heap};

    // TODO: global descriptor freelist
    // descriptor memory can be recycled, but never returned to the OS
    // so we need to implement some logic for growing the descriptor list

    Allocator() {}

    void       *allocate_large_block(std::size_t size);
    void        deallocate_large_block(void *ptr);
    std::size_t compute_size_class(std::size_t size);

public:

    Allocator& Instance();
    void *Allocate(std::size_t size);
    void  Free(void *ptr);
};

/*****************************************************************************/
/* IMPLEMENTATION                                                            */
/*****************************************************************************/

DescriptorFreelist::DescriptorFreelist()
    : m_freehead{nullptr}
{}

constexpr std::size_t DescriptorFreelist::descs_per_block()
{
    return kDescriptorBlockSize / sizeof(DescriptorNode);
}

DescriptorFreelist::DescriptorNode *DescriptorFreelist::initialize_block(void *block)
{
    DescriptorNode *nodes = std::launder(reinterpret_cast<DescriptorNode*>(block));
    const std::size_t nnodes = descs_per_block();

    for(int i = 0; i < nnodes - 1; i++) {
        new (&nodes[i]) DescriptorNode{};
        nodes[i].m_next.store(&nodes[i + 1], std::memory_order_release);
    }

    new (&nodes[nnodes - 1]) DescriptorNode{};
    nodes[nnodes - 1].m_next.store(nullptr, std::memory_order_release);
    return nodes;
}

SuperblockDescriptor &DescriptorFreelist::Allocate()
{
restart:

    DescriptorNode *freehead = m_freehead.load(std::memory_order_acquire);
    if(!freehead) {

        void *newblock = mmap(0, kDescriptorBlockSize, PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if(!newblock)
            throw std::bad_alloc{};

        DescriptorNode *next = initialize_block(newblock);
        while(m_freehead.compare_exchange_weak(freehead, next,
            std::memory_order_release, std::memory_order_acquire)) {

            /* If we found that we have freehead already,
             * then chain the existing block after the
             * newly allocated one.
             */
            if(freehead) {
                const std::size_t nnodes = descs_per_block();
                assert(next[nnodes - 1].m_next.load(std::memory_order_relaxed) == nullptr);
                next[nnodes - 1].m_next.store(freehead, std::memory_order_relaxed);
            }
        }
    }

    DescriptorNode *nextfree = freehead->m_next.load(std::memory_order_relaxed);
    while(!m_freehead.compare_exchange_weak(freehead, nextfree,
        std::memory_order_release, std::memory_order_acquire)) {

        if(!freehead)
            goto restart;
    }

    return freehead->m_desc;
}

void DescriptorFreelist::Retire(SuperblockDescriptor& desc)
{
    /* For standard-layout classes, the address of the struct/class 
     * is the same as the address of its first non-static member.
     */
    static_assert(offsetof(DescriptorNode, m_desc) == 0);
    DescriptorNode *curr = reinterpret_cast<DescriptorNode*>(&desc);

    do{
        DescriptorNode *freehead = m_freehead.load(std::memory_order_relaxed);
        curr->m_next.store(freehead, std::memory_order_relaxed);

        if(m_freehead.compare_exchange_weak(freehead, curr,
            std::memory_order_release, std::memory_order_relaxed)) {
            break;
        }

    }while(true);
}

BlockFreelist::BlockFreelist(Heap& heap, std::size_t sc)
    : m_heap{heap}
    , m_sizeclass{sc}
{}

void BlockFreelist::Fill()
{
}

void BlockFreelist::Flush()
{
}

bool BlockFreelist::IsEmpty()
{
    return false;
}

bool BlockFreelist::IsFull()
{
    return false;
}

void *BlockFreelist::PopBlock()
{
    return nullptr;
}

std::size_t Allocator::compute_size_class(std::size_t size)
{
    if(size > kMaxBlockSize)
        return 0;
    if(size == kMaxBlockSize)
        return kNumSizeClasses;
    return std::distance(std::begin(s_size_classes),
        std::upper_bound(std::begin(s_size_classes), std::end(s_size_classes), size));
}

void *Allocator::allocate_large_block(std::size_t size)
{
    void *ret = mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if(!ret)
        throw std::bad_alloc{};
    return ret;
}

void Allocator::deallocate_large_block(void *ptr)
{

}

Allocator& Allocator::Instance()
{
    static Allocator s_instance{};
    return s_instance;
}

void *Allocator::Allocate(std::size_t size)
{
    std::size_t sc = compute_size_class(size);
    if(sc == 0)
        return allocate_large_block(size);
    auto& cache = m_thread_cache.GetBlocksForSizeClass(sc);
    if(cache.IsEmpty())
        cache.Fill();
    return cache.PopBlock();
}

void Allocator::Free(void *ptr)
{

}

} // namespace pe

