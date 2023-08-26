export module alloc;

import assert;
import platform;
import concurrency;
import logger;
import mman;
import unistd;
import pthread;
import resource;
import meta;
import static_stack;

import <cmath>;
import <array>;
import <concepts>;
import <optional>;

namespace pe{

export{
inline constexpr unsigned kMinBlockSize = 8;
inline constexpr unsigned kMaxBlockSize = 16 * 1024;
inline constexpr unsigned kSuperblockSize = 4 * kMaxBlockSize;
inline constexpr unsigned kAddressUsedBits = 48;
}

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
    std::array<unsigned, kNumSizeClasses + 1> sc{0};
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

inline std::size_t s_page_size = 4096;

/*****************************************************************************/
/* SUPERBLOCK STATE                                                          */
/*****************************************************************************/

enum class SuperblockState
{
    /* If every block in the superblock is being used by a thread cache or by the application */
    eFull = 0,
    /* If the superblock has some used blocks and some free blocks remaining */
    ePartial = 1,
    /* If the superblock has only free blocks */
    eEmpty = 2
};

/*****************************************************************************/
/* SUPERBLOCK ANCHOR                                                         */
/*****************************************************************************/

struct alignas(8) SuperblockAnchor
{
    uint64_t m_state : 2;
    uint64_t m_avail : 31; /* index of the first free block */
    uint64_t m_count : 31; /* number of free blocks */
};

using AtomicSuperblockAnchor = std::atomic<SuperblockAnchor>;

static_assert(sizeof(SuperblockAnchor) == sizeof(uint64_t));
static_assert(AtomicSuperblockAnchor::is_always_lock_free);

/*****************************************************************************/
/* SUPERBLOCK DESCRIPTOR                                                     */
/*****************************************************************************/

export
struct alignas(16) SuperblockDescriptor
{
    AtomicSuperblockAnchor m_anchor;
    std::uintptr_t         m_superblock; /* pointer to superblock */
    std::size_t            m_blocksize;  /* size of each block in superblock */
    std::size_t            m_maxcount;   /* number of blocks */
    std::size_t            m_sizeclass;  /* size class of blocks in superblock */
};

static_assert(std::is_standard_layout_v<SuperblockDescriptor>);

struct DescriptorNode
{
    SuperblockDescriptor         m_desc;
    std::atomic<DescriptorNode*> m_next_free;
    std::atomic<DescriptorNode*> m_next_partial;

    struct alignas(16) Pointer
    {
        DescriptorNode *m_ptr{nullptr};
        uintptr_t       m_count{0};

        bool operator==(const Pointer& rhs) const
        {
            return (m_ptr == rhs.m_ptr) && (m_count == rhs.m_count);
        }

        bool operator!=(const Pointer& rhs) const
        {
            return !(*this == rhs);
        }
    };

    using AtomicPointer = DoubleQuadWordAtomic<Pointer>;
};

/*****************************************************************************/
/* DESCRIPTOR FREELIST                                                       */
/*****************************************************************************/
/*
 * The superblock descriptors can be recycled, but the memory allocated for
 * them is not returned to the OS until the application terminates.
 */

export
class DescriptorFreelist
{
private:

    static constexpr unsigned kDescriptorBlockSize = 16384; 

    DescriptorNode::AtomicPointer m_freehead;

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
private:

    DescriptorFreelist                                             m_desclist;
    std::array<DescriptorNode::AtomicPointer, kNumSizeClasses + 1> m_partial_superblocks;

public:

    Heap();

    SuperblockDescriptor *GetPartialSB(std::size_t size_class);
    void                  PutPartialSB(SuperblockDescriptor *desc);

    SuperblockDescriptor* AllocateSuperblock(std::size_t size_class);
    void                  RetireSuperblock(SuperblockDescriptor *desc); 

    SuperblockDescriptor *AllocateDescriptor();
    void                  RetireDescriptor(SuperblockDescriptor *desc); 
};

/*****************************************************************************/
/* PAGEMAP                                                                   */
/*****************************************************************************/
/*
 * The pagemap contains metadata for each OS page in use by the allocator.
 */

export
class Pagemap
{
private:

    uint64_t *m_metadata;

    std::size_t desc_array_size() const;
    std::size_t addr_to_key(std::byte *block) const;

public:

    Pagemap();

    SuperblockDescriptor *GetDescriptor(std::byte *block);
    void RegisterDescriptor(SuperblockDescriptor *desc);
    void UnregisterDescriptor(SuperblockDescriptor *desc);
    std::size_t GetSizeClass(std::byte *block);
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

    static constexpr std::size_t kMaxStackSize = kSuperblockSize / kMinBlockSize;

    Heap&                                  m_heap;
    Pagemap&                               m_pagemap;
    const std::size_t                      m_sizeclass;
    StaticStack<std::byte*, kMaxStackSize> m_blocks;

    std::size_t compute_idx(std::byte *superblock, 
        std::byte *ptr, std::size_t size_class) const;

    bool FillFromPartialSB();
    void FillFromNewSB();

public:

    BlockFreelist(Heap&, Pagemap&, std::size_t);

    void       Fill();
    void       Flush();
    bool       IsEmpty();
    bool       IsFull();
    std::byte *PopBlock();
    void       PushBlock(std::byte *block);
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

    std::array<BlockFreelist, kNumSizeClasses + 1> m_lists;

    template<typename T, std::size_t N, std::size_t... I>
    constexpr auto create_array_impl(Heap& heap, Pagemap& pagemap, 
        decltype(s_size_classes) scs, std::index_sequence<I...>)
    {
        return std::array<T, N>{ BlockFreelist{heap, pagemap, I}... };
    }

    template<typename T, std::size_t N>
    constexpr auto create_array(Heap& heap, Pagemap& pagemap, decltype(s_size_classes) scs)
    {
        return create_array_impl<T, N>(heap, pagemap, scs, std::make_index_sequence<N>{});
    }

public:

    SimpleSegregatedStorage(Heap& heap, Pagemap& pagemap)
        : m_lists{ create_array<BlockFreelist, kNumSizeClasses + 1>(heap, pagemap, s_size_classes) }
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

public:

    ThreadCache(Heap& heap, Pagemap& pagemap)
        : m_blocklists{heap, pagemap}
    {}

    BlockFreelist& GetBlocksForSizeClass(std::size_t sc)
    {
        if(sc > kNumSizeClasses)
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

    Heap&         m_heap;
    Pagemap&      m_pagemap;
    pthread_key_t m_thread_cache_key;

    void        *allocate_large_block(std::size_t size);
    void         deallocate_large_block(void *ptr);
    std::size_t  compute_size_class(std::size_t size);
    ThreadCache& get_thread_cache() const;

    Allocator(Heap& heap, Pagemap& pagemap);
    ~Allocator();

public:

    static inline Allocator& Instance();

    void  *Allocate(std::size_t size);
    void   Free(void *ptr);

    void  *AllocateAligned(std::size_t size, std::align_val_t align);
    void   FreeAligned(void *ptr, std::align_val_t align);

    std::size_t NextAlignedBlockSize(std::size_t size, std::size_t align);
    std::size_t AllocationSize(void *ptr);
};

/*****************************************************************************/
/* IMPLEMENTATION                                                            */
/*****************************************************************************/

DescriptorFreelist::DescriptorFreelist()
    : m_freehead{{nullptr, 0}}
{}

constexpr std::size_t DescriptorFreelist::descs_per_block()
{
    return kDescriptorBlockSize / sizeof(DescriptorNode);
}

DescriptorNode *DescriptorFreelist::initialize_block(void *block)
{
    DescriptorNode *nodes = std::launder(reinterpret_cast<DescriptorNode*>(block));
    const std::size_t nnodes = descs_per_block();

    for(int i = 0; i < nnodes - 1; i++) {
        new (&nodes[i]) DescriptorNode{};
        nodes[i].m_next_free.store(&nodes[i + 1], std::memory_order_relaxed);
    }

    new (&nodes[nnodes - 1]) DescriptorNode{};
    nodes[nnodes - 1].m_next_free.store(nullptr, std::memory_order_relaxed);
    return nodes;
}

SuperblockDescriptor &DescriptorFreelist::Allocate()
{
restart:

    DescriptorNode::Pointer freehead = m_freehead.Load(std::memory_order_acquire);
    while(!freehead.m_ptr) {

        void *newblock = mmap(0, kDescriptorBlockSize, PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if(newblock == reinterpret_cast<void*>(-1ull))
            throw std::bad_alloc{};

        DescriptorNode *next = initialize_block(newblock);
        AnnotateHappensBefore(__FILE__, __LINE__, &m_freehead);

        while(!m_freehead.CompareExchange(freehead, {next, freehead.m_count + 1},
            std::memory_order_release, std::memory_order_acquire)) {

            /* If we found that we have freehead already,
             * then chain the existing block after the
             * newly allocated one.
             */
            const std::size_t nnodes = descs_per_block();
            next[nnodes - 1].m_next_free.store(freehead.m_ptr, std::memory_order_release);
        }
        freehead = m_freehead.Load(std::memory_order_acquire);
    }

    AnnotateHappensAfter(__FILE__, __LINE__, &m_freehead);
    DescriptorNode *nextfree = freehead.m_ptr->m_next_free.load(std::memory_order_acquire);

    while(!m_freehead.CompareExchange(freehead, {nextfree, freehead.m_count + 1},
        std::memory_order_release, std::memory_order_acquire)) {

        if(!freehead.m_ptr)
            goto restart;
        nextfree = freehead.m_ptr->m_next_free.load(std::memory_order_acquire);
    }

    return freehead.m_ptr->m_desc;
}

void DescriptorFreelist::Retire(SuperblockDescriptor& desc)
{
    /* For standard-layout classes, the address of the struct/class 
     * is the same as the address of its first non-static member.
     */
    static_assert(offsetof(DescriptorNode, m_desc) == 0);
    DescriptorNode *curr = reinterpret_cast<DescriptorNode*>(&desc);
    DescriptorNode::Pointer freehead = m_freehead.Load(std::memory_order_acquire);

    do{
        curr->m_next_free.store(freehead.m_ptr, std::memory_order_release);
        AnnotateHappensBefore(__FILE__, __LINE__, &m_freehead);

        if(m_freehead.CompareExchange(freehead, {curr, freehead.m_count + 1},
            std::memory_order_release, std::memory_order_acquire)) {

            AnnotateHappensAfter(__FILE__, __LINE__, &m_freehead);
            break;
        }
    }while(true);
}

std::size_t Pagemap::desc_array_size() const
{
    return (std::exp2(kAddressUsedBits) / s_page_size);
}

std::size_t Pagemap::addr_to_key(std::byte *block) const
{
    uint64_t pageshift = std::log2(s_page_size);
    uintptr_t page = reinterpret_cast<uintptr_t>(block);
    uintptr_t idx = page >> pageshift;
    pe::assert(idx < desc_array_size());
    return idx;
}

Pagemap::Pagemap()
    : m_metadata{}
{
    const std::size_t size = sizeof(SuperblockDescriptor*) * desc_array_size();
    m_metadata = static_cast<uint64_t*>(
        mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0));
    if(m_metadata == reinterpret_cast<decltype(m_metadata)>(-1))
        throw std::bad_alloc{};
    madvise(m_metadata, size, MADV_DONTDUMP);
}

SuperblockDescriptor *Pagemap::GetDescriptor(std::byte *block)
{
    uint64_t mask = std::exp2(64 - kAddressUsedBits) - 1;
    mask <<= kAddressUsedBits;
    auto ret = reinterpret_cast<SuperblockDescriptor*>(m_metadata[addr_to_key(block)] & ~mask);
    pe::assert(ret != 0);
    return ret;
}

void Pagemap::RegisterDescriptor(SuperblockDescriptor *desc)
{
    std::size_t npages = kSuperblockSize / s_page_size;
    npages += !!(kSuperblockSize % s_page_size);

    std::byte *base = reinterpret_cast<std::byte*>(desc->m_superblock);
    for(int i = 0; i < npages; i++) {
        /* Although we can fetch the sizeclass by dereferencing
         * the descriptor pointer, we can pack it into the unused
         * higher bits in order to save on one pointer jump in the
         * hot case.
         */
        uint64_t value = desc->m_sizeclass << kAddressUsedBits | reinterpret_cast<uintptr_t>(desc);
        m_metadata[addr_to_key(base + (i * s_page_size))] = value;

        /* No need to register every page for large allocations.
         */
        if(desc->m_sizeclass == 0)
            break;
    }
}

void Pagemap::UnregisterDescriptor(SuperblockDescriptor *desc)
{
    std::size_t npages = kSuperblockSize / s_page_size;
    npages += !!(kSuperblockSize % s_page_size);

    std::byte *base = reinterpret_cast<std::byte*>(desc->m_superblock);
    for(int i = 0; i < npages; i++) {
        m_metadata[addr_to_key(base + (i * s_page_size))] = 0;
    }
}

std::size_t Pagemap::GetSizeClass(std::byte *block)
{
    uint64_t meta = m_metadata[addr_to_key(block)];
    return (meta >> kAddressUsedBits);
}

Heap::Heap()
    : m_desclist{}
    , m_partial_superblocks{DescriptorNode::Pointer{nullptr, 0}}
{}

SuperblockDescriptor* Heap::GetPartialSB(std::size_t size_class)
{
    DescriptorNode::Pointer head = m_partial_superblocks[size_class].Load(std::memory_order_acquire);
    DescriptorNode *next;
    do{
        if(!head.m_ptr)
            return nullptr;
        next = head.m_ptr->m_next_partial.load(std::memory_order_relaxed);

    }while(!m_partial_superblocks[size_class].CompareExchange(head, {next, head.m_count + 1},
        std::memory_order_release, std::memory_order_acquire));

    return &head.m_ptr->m_desc;
}

void Heap::PutPartialSB(SuperblockDescriptor *desc)
{
    std::size_t size_class = desc->m_sizeclass;
    static_assert(offsetof(DescriptorNode, m_desc) == 0);
    DescriptorNode *curr = reinterpret_cast<DescriptorNode*>(desc);

    [[maybe_unused]] SuperblockAnchor anchor = desc->m_anchor.load(std::memory_order_relaxed);
    pe::assert(anchor.m_state == static_cast<uint64_t>(SuperblockState::ePartial));

    DescriptorNode::Pointer head = m_partial_superblocks[size_class].Load(std::memory_order_acquire);
    do{
        curr->m_next_partial.store(head.m_ptr, std::memory_order_relaxed);

    }while(!m_partial_superblocks[size_class].CompareExchange(head, {curr, head.m_count + 1},
        std::memory_order_release, std::memory_order_acquire));
}

SuperblockDescriptor* Heap::AllocateSuperblock(std::size_t size_class)
{
    SuperblockDescriptor *ret = &m_desclist.Allocate();
    void *superblock = mmap(0, kSuperblockSize, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if(superblock == reinterpret_cast<void*>(-1ull))
        throw std::bad_alloc{};

    ret->m_superblock = reinterpret_cast<uintptr_t>(superblock);
    ret->m_blocksize = s_size_classes[size_class];
    ret->m_maxcount = kSuperblockSize / ret->m_blocksize;
    ret->m_sizeclass = size_class;

    /* Form an ad-hoc freelist of blocks inside the superblock.
     */
    size_t nblocks = ret->m_maxcount;
    for(int i = 0; i < nblocks; i++) {
        std::byte *block = reinterpret_cast<std::byte*>(superblock) + (i * ret->m_blocksize);
        std::byte *next = reinterpret_cast<std::byte*>(superblock) + ((i + 1) * ret->m_blocksize);
        if(i == nblocks - 1)
            next = nullptr;
        *reinterpret_cast<std::byte**>(block) = next;
    }

    /* Because all the blocks in a newly allocated superblock
     * are immediately given to a thread cache, a superblock
     * that was just allocated from the OS is full and will only
     * become partial/empty as blocks are flushed from thread
     * caches.
     */
    ret->m_anchor.store({static_cast<uint64_t>(SuperblockState::eFull), ret->m_maxcount, 0},
        std::memory_order_release);

    return ret;
}

void Heap::RetireSuperblock(SuperblockDescriptor *desc)
{
    munmap(reinterpret_cast<void*>(desc->m_superblock), kSuperblockSize);
}

SuperblockDescriptor* Heap::AllocateDescriptor()
{
    return &m_desclist.Allocate();
}

void Heap::RetireDescriptor(SuperblockDescriptor *desc)
{
    m_desclist.Retire(*desc);
}

BlockFreelist::BlockFreelist(Heap& heap, Pagemap& pagemap, std::size_t sc)
    : m_heap{heap}
    , m_pagemap{pagemap}
    , m_sizeclass{sc}
    , m_blocks{s_size_classes[sc] > 0 ? (kSuperblockSize / s_size_classes[sc]) : 0}
{
    pe::assert(sc <= kNumSizeClasses);
}

std::size_t BlockFreelist::compute_idx(std::byte *superblock, 
    std::byte *ptr, std::size_t size_class) const
{
    pe::assert(ptr >= superblock);
    uintptr_t diff = ptr - superblock;
    pe::assert(diff < kSuperblockSize);
    pe::assert(diff % s_size_classes[size_class] == 0);
    return diff / s_size_classes[size_class];
}

bool BlockFreelist::FillFromPartialSB()
{
    SuperblockDescriptor *desc = m_heap.GetPartialSB(m_sizeclass);
    if(!desc)
        return false;

    SuperblockAnchor new_anchor, old_anchor;
    do{
        old_anchor = desc->m_anchor.load(std::memory_order_acquire);
        if(old_anchor.m_state == static_cast<uint64_t>(SuperblockState::eEmpty)) {
            m_heap.RetireDescriptor(desc);
            return FillFromPartialSB();
        }
        pe::assert(desc->m_sizeclass != 0);
        pe::assert(old_anchor.m_state == static_cast<uint64_t>(SuperblockState::ePartial));
        new_anchor.m_state = static_cast<uint64_t>(SuperblockState::eFull);
        new_anchor.m_avail = desc->m_maxcount;
        new_anchor.m_count = 0;
    }while(!desc->m_anchor.compare_exchange_weak(old_anchor, new_anchor,
        std::memory_order_release, std::memory_order_relaxed));

    std::byte *block = reinterpret_cast<std::byte*>(desc->m_superblock)
                     + (old_anchor.m_avail * desc->m_blocksize);
    std::size_t block_count = old_anchor.m_count;
    pe::assert(block_count > 0);

    while(block_count--) {
        PushBlock(block);
        block = *reinterpret_cast<std::byte**>(block);
    }
    return true;
}

void BlockFreelist::FillFromNewSB()
{
    SuperblockDescriptor *desc = m_heap.AllocateSuperblock(m_sizeclass);
    pe::assert(desc->m_maxcount > 0);
    pe::assert(desc->m_superblock);
    pe::assert(desc->m_anchor.load(std::memory_order_relaxed).m_state 
        == static_cast<uint64_t>(SuperblockState::eFull));

    for(int i = 0; i < desc->m_maxcount; i++) {
        std::byte *block = reinterpret_cast<std::byte*>(desc->m_superblock)
                         + (i * desc->m_blocksize);
        PushBlock(block);
    }
    m_pagemap.RegisterDescriptor(desc);
}

void BlockFreelist::Fill()
{
    /* Try to fill the cache from a single partial superblock
     */
    bool result = FillFromPartialSB();
    /* If that fails, create a new superblock
     */
    if(!result)
        FillFromNewSB();
}

void BlockFreelist::Flush()
{
    while(!m_blocks.Empty()) {
        /* Form a list of blocks to return to a common superblock.
         */
        std::byte *head, *tail;
        head = tail = m_blocks.Pop().value();
        SuperblockDescriptor *desc = m_pagemap.GetDescriptor(head);
        [[maybe_unused]] SuperblockAnchor anchor = desc->m_anchor.load(std::memory_order_relaxed);
        pe::assert(anchor.m_state != static_cast<uint64_t>(SuperblockState::eEmpty));
        pe::assert(desc->m_sizeclass != 0);
        std::size_t block_count = 1;

        while(!m_blocks.Empty()) {
            std::byte *block = m_blocks.Peek().value();
            if(m_pagemap.GetDescriptor(block) != desc)
                break;
            m_blocks.Pop();
            ++block_count;
            *reinterpret_cast<std::byte**>(tail) = block;
            tail = block;
        }

        /* Add list to descriptor and update anchor.
         */
        std::byte *superblock = reinterpret_cast<std::byte*>(desc->m_superblock);
        size_t idx = compute_idx(superblock, head, desc->m_sizeclass);
        SuperblockAnchor old_anchor, new_anchor;
        do{
            old_anchor = new_anchor = desc->m_anchor.load(std::memory_order_relaxed);
            *reinterpret_cast<std::byte**>(tail) = superblock 
                                                 + (old_anchor.m_avail * desc->m_blocksize);
            new_anchor.m_state = static_cast<uint64_t>(SuperblockState::ePartial);
            new_anchor.m_avail = idx;
            new_anchor.m_count += block_count;

            pe::assert(new_anchor.m_count <= desc->m_maxcount);

            if(new_anchor.m_count == desc->m_maxcount) {
                /* Can free superblock */
                new_anchor.m_state = static_cast<uint64_t>(SuperblockState::eEmpty);
            }else if(old_anchor.m_state == static_cast<uint64_t>(SuperblockState::eFull)) {
                new_anchor.m_state = static_cast<uint64_t>(SuperblockState::ePartial);
            }

            pe::assert(new_anchor.m_state == static_cast<uint64_t>(SuperblockState::eFull)
                ? (new_anchor.m_count == 0) : true);
            pe::assert(new_anchor.m_state == static_cast<uint64_t>(SuperblockState::eEmpty)
                ? (new_anchor.m_count == desc->m_maxcount) : true);

        }while(!desc->m_anchor.compare_exchange_weak(old_anchor, new_anchor,
            std::memory_order_release, std::memory_order_relaxed));

        if(new_anchor.m_state == static_cast<uint64_t>(SuperblockState::eEmpty)) {
            m_pagemap.UnregisterDescriptor(desc);
            m_heap.RetireSuperblock(desc);
        }else if(old_anchor.m_state == static_cast<uint64_t>(SuperblockState::eFull)) {
            pe::assert(new_anchor.m_state == static_cast<uint64_t>(SuperblockState::ePartial));
            m_heap.PutPartialSB(desc);
        }
    }
}

bool BlockFreelist::IsEmpty()
{
    return m_blocks.Empty();
}

bool BlockFreelist::IsFull()
{
    return m_blocks.Full();
}

void BlockFreelist::PushBlock(std::byte *block)
{
    m_blocks.Push(block);
}

std::byte *BlockFreelist::PopBlock()
{
    auto ret = m_blocks.Pop();
    return ret.value_or(nullptr);
}

std::size_t Allocator::compute_size_class(std::size_t size)
{
    if(size > kMaxBlockSize)
        return 0;
    return std::distance(std::begin(s_size_classes),
        std::lower_bound(std::begin(s_size_classes), std::end(s_size_classes), size));
}

ThreadCache& Allocator::get_thread_cache() const
{
    static thread_local ThreadCache t_thread_cache{m_heap, m_pagemap};
    void *raw = pthread_getspecific(m_thread_cache_key);
    if(!raw) {
        raw = reinterpret_cast<void*>(&t_thread_cache);
        if(pthread_setspecific(m_thread_cache_key, raw)) [[unlikely]]
            throw std::runtime_error{"Failed to set Thread-Local Storage."};
    }
    return *reinterpret_cast<ThreadCache*>(raw);
}

void *Allocator::allocate_large_block(std::size_t size)
{
    void *ret = mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if(ret == reinterpret_cast<void*>(-1ull))
        throw std::bad_alloc{};

    SuperblockDescriptor *desc = m_heap.AllocateDescriptor();
    desc->m_superblock = reinterpret_cast<uintptr_t>(ret);
    desc->m_blocksize = size;
    desc->m_maxcount = 1;
    desc->m_sizeclass = 0;
    desc->m_anchor.store({static_cast<uint64_t>(SuperblockState::eFull), desc->m_maxcount, 0},
        std::memory_order_release);
    m_pagemap.RegisterDescriptor(desc);

    return ret;
}

void Allocator::deallocate_large_block(void *ptr)
{
    auto desc = m_pagemap.GetDescriptor(reinterpret_cast<std::byte*>(ptr));
    munmap(ptr, desc->m_blocksize);
    m_heap.RetireDescriptor(desc);
}

Allocator::Allocator(Heap& heap, Pagemap& pagemap)
    : m_heap{heap}
    , m_pagemap{pagemap}
    , m_thread_cache_key{}
{
    int result = pthread_key_create(&m_thread_cache_key, nullptr);
    if(result) [[unlikely]]
        throw std::runtime_error{"Failed to allocate Thread-Local Storage."};
}

Allocator::~Allocator()
{
    pthread_key_delete(m_thread_cache_key);
}

Allocator& Allocator::Instance()
{
    static bool initialized = false;
    if(!initialized) {
        s_page_size = getpagesize();
        initialized = true;
    }
    static Heap s_heap{};
    static Pagemap s_pagemap{};
    static Allocator s_instance{s_heap, s_pagemap};
    return s_instance;
}

void *Allocator::Allocate(std::size_t size)
{
    std::size_t sc = compute_size_class(size);
    if(sc == 0)
        return allocate_large_block(size);
    auto& cache = get_thread_cache().GetBlocksForSizeClass(sc);
    if(cache.IsEmpty())
        cache.Fill();
    return cache.PopBlock();
}

void Allocator::Free(void *ptr)
{
    std::byte *block = reinterpret_cast<std::byte*>(ptr);
    if(!block) [[unlikely]]
        return;
    size_t sc = m_pagemap.GetSizeClass(block);
    if(sc == 0)
        return deallocate_large_block(block);
    auto& cache = get_thread_cache().GetBlocksForSizeClass(sc);
    if(cache.IsFull())
        cache.Flush();
    cache.PushBlock(block);
}

void *Allocator::AllocateAligned(std::size_t size, std::align_val_t align)
{
    std::size_t alignment = static_cast<std::size_t>(align);
    if(alignment <= kMaxBlockSize) {
        size = NextAlignedBlockSize(size, alignment);
        return Allocate(size);
    }

    std::size_t extra_bytes = size + alignment - 1 + sizeof(void*);
    void *allocation = Allocate(size + extra_bytes);
    if(!allocation)
        return allocation;

    /* round up to a multiple of alignment 
     */
    uintptr_t uptr = reinterpret_cast<uintptr_t>(allocation);
    uptr = (uptr + extra_bytes) & ~(alignment - 1);
    void *ret = reinterpret_cast<void*>(uptr);

    /* store the original pointer in the allocation 
     */
    reinterpret_cast<void**>(ret)[-1] = allocation;
    return ret;
}

void Allocator::FreeAligned(void *ptr, std::align_val_t align)
{
    if(!ptr)
        return;

    std::size_t alignment = static_cast<std::size_t>(align);
    if(alignment <= kMaxBlockSize) {
        Free(ptr);
        return;
    }

    void *allocation = reinterpret_cast<void**>(ptr)[-1];
    Free(allocation);
}

std::size_t Allocator::NextAlignedBlockSize(std::size_t size, std::size_t align)
{
    auto it = std::lower_bound(std::begin(s_size_classes), std::end(s_size_classes), size);
    while(it != std::end(s_size_classes)) {
        std::size_t blocksize = *it;
        if(blocksize % align == 0)
            return blocksize;
        it++;
    }
    return size;
}

std::size_t Allocator::AllocationSize(void *ptr)
{
    return m_pagemap.GetDescriptor(reinterpret_cast<std::byte*>(ptr))->m_blocksize;
}

} // namespace pe

