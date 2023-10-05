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

export module bitwise_trie;

import assert;
import logger;

import <immintrin.h>;
import <optional>;
import <memory>;
import <type_traits>;
import <array>;
import <cstring>;
import <functional>;
import <iostream>;
import <stack>;
import <ranges>;

namespace pe{

/*****************************************************************************/
/* BIT KEY                                                                   */
/*****************************************************************************/
/* 
 * Any integral types, __int128, std::bitset, or custom 
 * optimized bitset implementations can all be used as
 * keys.
 */
export
template <typename T>
concept BitKey = requires (T a, T b, int i){
    std::is_standard_layout_v<T>;
    std::is_constructible_v<T>;
    std::is_constructible_v<int>;
    std::is_assignable_v<T, T>;
    {a & b  } -> std::same_as<T>;
    {a | b  } -> std::same_as<T>;
    {a ^ b  } -> std::same_as<T>;
    { ~a    } -> std::same_as<T>;
    {a &= b } -> std::same_as<T&>;
    {a |= b } -> std::same_as<T&>;
    {a ^= i } -> std::same_as<T&>;
    {a >> i } -> std::same_as<T>;
    {a << i } -> std::same_as<T>;
    {a >>= i} -> std::same_as<T&>;
    {a >>= i} -> std::same_as<T&>;
    {a == b } -> std::same_as<bool>;
    {a != b } -> std::same_as<bool>;
};

template <unsigned x>
inline consteval unsigned log2()
{
    if constexpr(x == 1)
        return 0;
    return 1 + log2<x/2>();
}

/*****************************************************************************/
/* TRIE VIEW                                                                 */
/*****************************************************************************/
/* 
 * Custom view objects which allow efficient lazy 
 * pruning of a Bitwise Trie. May be composed to
 * lazily produce a set of keys matching complex
 * set queries.
 */

template <BitKey Key>
class enable_trie_view;

export
template <BitKey Key>
class trie_view;

export
template <
    BitKey Key, 
    std::derived_from<enable_trie_view<Key>> AView, 
    std::derived_from<enable_trie_view<Key>> BView
> class trie_view_intersection;

export
template <
    BitKey Key,
    std::derived_from<enable_trie_view<Key>> AView, 
    std::derived_from<enable_trie_view<Key>> BView
> class trie_view_union;

export
template <
    BitKey Key,
    std::derived_from<enable_trie_view<Key>> AView, 
    std::derived_from<enable_trie_view<Key>> BView
> class trie_view_difference;

export
template <
    BitKey Key,
    std::derived_from<enable_trie_view<Key>> View
> class trie_view_range;

export
template <
    BitKey Key,
    std::derived_from<enable_trie_view<Key>> View
> class trie_view_match_mask;

/*****************************************************************************/
/* VIRTUAL NODE                                                              */
/*****************************************************************************/
/* 
 Allows statically specializing Iterator behavior.
 */
template <typename T, typename Key>
concept CVirtualNode =
requires (T node, Key key)
{
    BitKey<Key>;
    {node.get_bitmap()        } -> std::same_as<Key>;
    {node.get_bitmap_leaf(key)} -> std::same_as<uint64_t>;
    {node.get_child_node(key) } -> std::same_as<T>;
    {node.is_sentinel()       } -> std::same_as<bool>;
};

/*****************************************************************************/
/* BITWISE TRIE                                                              */
/*****************************************************************************/
/* 
 * Based on the paper "Efficient Use of Trie Data
 * Structures in Databases" by Walter Bauer and 
 * prior works.
 */
export
template <BitKey Key>
class BitwiseTrie
{
private:

    static constexpr std::size_t kKeyBits = sizeof(Key) * CHAR_BIT;
    static constexpr std::size_t kKeyWords = std::max(std::size_t(1),
        (kKeyBits / 64) + !!(kKeyBits % 64));

    /* There are (1 + kKeyBits) number of distinct node sizes
     * as each node can have up to a maximum of kKeyBits
     * children, as well as no children.
     */
    static constexpr std::size_t kNumFreelists = 1 + kKeyBits;

    /* Sentinel values 
     */
    static constexpr uint64_t kEmptyNode = 0;
    static constexpr uint64_t kDeletedNode = 1; 

    /* First two indices are reserved - Empty or Deleted.
     */
    static constexpr std::size_t kMemHeaderSize = 2;

    /* Use no more than 6 segment bits so that a bit shifted
     * by a segment value fits into a 64-bit word.
     */
    static constexpr std::size_t kKeyPathSegmentBits = std::min(6u, log2<kKeyBits>());
    static constexpr std::size_t kKeyPathSegments = 
        (kKeyBits / kKeyPathSegmentBits) + !!(kKeyBits % kKeyPathSegmentBits);

    /* Decomposes the key into 6-bit segments.
     */
    struct KeyPath
    {
    private:

        Key m_key;

        static constexpr std::uint32_t kSegmentMask = (0x1 << kKeyPathSegmentBits) - 1;

    public:

        KeyPath(const Key& key)
            : m_key{key}
        {}

        uint8_t operator[](std::size_t idx) const
        {
            Key shifted = (m_key >> (kKeyPathSegments - 1 - idx) * kKeyPathSegmentBits);
            if constexpr (std::is_integral_v<Key>) {
                return static_cast<uint8_t>(shifted & Key{kSegmentMask});
            }else{
                uint8_t ret{};
                for(int i = 0; i < kKeyPathSegmentBits; i++) {
                    if((shifted & Key{uint64_t(0b1) << i}) != Key{0})
                        ret |= (uint8_t(0b1) << i);
                }
                return ret;
            }
        }

        void SetSegment(std::size_t idx, uint8_t value)
        {
            pe::assert(idx < kKeyPathSegments);
            const std::size_t shift = (kKeyPathSegments - 1 - idx) * kKeyPathSegmentBits;
            Key mask{Key{kSegmentMask} << shift};
            Key shifted{(Key{value} & Key{kSegmentMask}) << shift};
            m_key &= ~mask;
            m_key |= shifted;
        }

        Key Raw() const
        {
            return m_key;
        }

        friend std::ostream& operator<<(std::ostream& stream, const KeyPath& key)
        {
            stream << "{";
            for(int i = 0; i < kKeyPathSegments; i++) {
                stream << i << ":" << fmt::hex{static_cast<uint64_t>(key[i])};
                if(i < kKeyPathSegments-1)
                    stream << " ";
            }
            stream << "}";
            return stream;
        }

        friend auto operator<=>(const KeyPath&, const KeyPath&) = default;
    };

    template <BitKey KeyType>
    friend class trie_view;

    template <
        BitKey KeyType,
        std::derived_from<enable_trie_view<KeyType>> ANode, 
        std::derived_from<enable_trie_view<KeyType>> BNode
    > friend class trie_view_intersection;

    template <
        BitKey KeyType,
        std::derived_from<enable_trie_view<KeyType>> AView, 
        std::derived_from<enable_trie_view<KeyType>> BView
    > friend class trie_view_union;

    template <
        BitKey KeyType,
        std::derived_from<enable_trie_view<KeyType>> AView, 
        std::derived_from<enable_trie_view<KeyType>> BView
    > friend class trie_view_difference;

    template <
        BitKey KeyType,
        std::derived_from<enable_trie_view<KeyType>> View
    > friend class trie_view_range;

    template <
        BitKey KeyType,
        std::derived_from<enable_trie_view<KeyType>> View
    > friend class trie_view_match_mask;

    /* The bitmask has a bit set for every valid subtrie 
     * of this node. There is a child pointer for every 
     * non-null subtrie following the node header, so the 
     * child subtrie corresponding to a particular bit can 
     * be indexed by counting the set bits up until the 
     * selected bit.
     *
     * Furthtermore, we employ a 'terminal branch optimization'.
     * Terminal branches are those which have only a single
     * child, and whose descendants all have only a single 
     * child. In case where only a single bit in the mask is 
     * set, the bit is instead interpreted as an N-bit section 
     * of the key. (For a 64-bit mask, the position of the bit 
     * within the bitmask is a 6-bit value). This allows
     * compressing a chain of N nodes into a single node and
     * saving a significant amount of memory when storing long
     * terminal branches.
     *
     * Leaf nodes are further inlined. The presence of the
     * leaf node (as well as the last N bits of the key can
     * be encoded as a single bit in an addtitional bitmap
     * of size 2^N.
     *
     * A variable number of child indices are appended after 
     * the node header. In the case of a leaf node, the final
     * N bits of the key (encoded as a bitmask of size 64) are 
     * stored instead.
     */
    struct NodeHeader
    {
        Key m_bitmask;
    };

    using node_ref_t = uint64_t;
    using index_or_segment_t = uint64_t;

    static constexpr std::size_t kNodeHeaderWords = 
        (sizeof(NodeHeader) / sizeof(uint64_t)) + !!(sizeof(NodeHeader) % sizeof(uint64_t));

    std::unique_ptr<uint64_t[]>           m_mem;
    std::array<node_ref_t, kNumFreelists> m_freelists;
    std::size_t                           m_memsize;
    node_ref_t                            m_free_idx;
    node_ref_t                            m_root_idx;
    std::size_t                           m_node_count;

    static std::size_t popcnt(const Key& key);
    static std::size_t tzcnt(const Key& key);
    static std::size_t clear_first_set(Key& key, Key& bit_pos);
    static Key         mask_up_to_bit(const Key& bit_pos);

    node_ref_t insert(node_ref_t node_idx, KeyPath key, std::size_t offset);
    node_ref_t remove(node_ref_t node_idx, KeyPath key, std::size_t offset);

    node_ref_t allocate(std::size_t num_children);
    void       deallocate(node_ref_t node_idx, std::size_t num_children);
    node_ref_t reallocate_insert(node_ref_t node_idx, std::size_t num_children, 
                                 std::size_t child_idx);
    node_ref_t reallocate_delete(node_ref_t node_idx, std::size_t num_children, 
                                 std::size_t child_idx);
    node_ref_t create_leaf_node(KeyPath key, std::size_t offset);
    node_ref_t insert_child(node_ref_t node_idx, Key key, Key bit_pos, 
                            std::size_t child_idx, index_or_segment_t value);
    uint64_t   remove_child(node_ref_t node_idx, Key key, Key bit_pos, std::size_t idx);

    struct IdentityVirtualNode
    {
    private:

        const uint64_t *m_mem;
        node_ref_t      m_node_idx;
        
    public:

        IdentityVirtualNode(const uint64_t *mem, node_ref_t node_idx)
            : m_mem{mem}
            , m_node_idx{node_idx}
        {}

        Key get_bitmap() const
        {
            const NodeHeader *header = reinterpret_cast<const NodeHeader*>(&m_mem[m_node_idx]);
            return header->m_bitmask;
        }

        uint64_t get_bitmap_leaf(Key bit_pos)
        {
            const NodeHeader *header = reinterpret_cast<const NodeHeader*>(&m_mem[m_node_idx]);
            Key bitmap = header->m_bitmask;
            uint64_t idx = popcnt(bitmap & mask_up_to_bit(bit_pos));
            return m_mem[m_node_idx + kNodeHeaderWords + idx];
        }

        IdentityVirtualNode get_child_node(Key bit_pos)
        {
            const NodeHeader *header = reinterpret_cast<const NodeHeader*>(&m_mem[m_node_idx]);
            Key bitmap = header->m_bitmask;
            uint64_t idx = popcnt(bitmap & mask_up_to_bit(bit_pos));
            return IdentityVirtualNode{m_mem, m_mem[m_node_idx + kNodeHeaderWords + idx]};
        }

        bool is_sentinel()
        {
            return (m_node_idx == kEmptyNode);
        }
    };

    template <CVirtualNode<Key> Node>
    class Iterator
    {
    public:

        using iterator_category = std::forward_iterator_tag;
        using difference_type = ptrdiff_t;
        using value_type = Key;
        using pointer = const Key*;
        using reference = const Key&;
        using node_type = Node;

        friend class BitwiseTrie<Key>;

    private:

        struct GeneratorContext
        {
            enum class Stage
            {
                eProcessRoot,
                eIterateChildren,
                eIterateLeaf,
                eFinished
            };
            Stage       m_stage;
            Node        m_node;
            KeyPath     m_key;
            std::size_t m_offset;
            Key         m_bits;
            Key         m_bit_pos;
            std::size_t m_bit_num;
            uint64_t    m_leaf_bits;
            uint64_t    m_leaf_bit_pos;
            std::size_t m_leaf_bit_num;
        };

        Node                         m_root_node;
        GeneratorContext             m_ctx;
        std::stack<GeneratorContext> m_recursion_stack;

        void     push_ctx();
        bool     try_pop_ctx();
        void     next_key();

    public:

        Iterator(Node root)
            : m_root_node{root}
            , m_ctx{
                .m_stage = GeneratorContext::Stage::eProcessRoot,
                .m_node = root,
                .m_key = Key{0},
                .m_offset = 0,
                .m_bits = Key{0},
                .m_bit_pos = Key{0},
                .m_bit_num = 0,
                .m_leaf_bits = 0,
                .m_leaf_bit_pos = 0,
                .m_leaf_bit_num = 0
            }
            , m_recursion_stack{}
        {
            next_key();
        }

        Iterator() {}
        Iterator(Iterator const& other) = default;
        Iterator& operator=(Iterator const&) = default;

        Node root() const
        {
            return m_root_node;
        }

        value_type operator*() const
        {
            return m_ctx.m_key.Raw();
        }

        Iterator& operator++()
        {
            next_key();
            return *this;
        }

        Iterator operator++(int)
        {
            Iterator ret = *this;
            ++(*this);
            return ret;
        }

        friend bool operator==(const Iterator& a, const Iterator& b)
        {
            return (a.m_ctx.m_stage == b.m_ctx.m_stage)
                && (a.m_ctx.m_key   == b.m_ctx.m_key);
        }

        friend bool operator!=(const Iterator& a, const Iterator& b)
        {
            return !(a == b);
        }
    };

public:

    using iterator = Iterator<IdentityVirtualNode>;
    constexpr static std::size_t kDefaultInitialSize = 1024;

    BitwiseTrie(std::size_t initial_size = kDefaultInitialSize);

    template <std::ranges::input_range Range>
    requires std::is_convertible_v<std::ranges::range_value_t<Range>, Key>
    BitwiseTrie(Range&& range, std::size_t initial_size = kDefaultInitialSize);

    bool        Get(Key key) const;
    bool        Insert(Key key);
    bool        Remove(Key key);
    std::size_t Size() const;

    /* Iterators
     */
    iterator begin() const noexcept;
    iterator end() const noexcept;
};

template <BitKey Key>
BitwiseTrie<Key>::BitwiseTrie(std::size_t initial_size)
    : m_mem{new uint64_t[initial_size]}
    , m_freelists{}
    , m_memsize{initial_size}
    , m_free_idx{kMemHeaderSize}
    , m_root_idx{kEmptyNode}
    , m_node_count{0}
{}

template <BitKey Key>
template <std::ranges::input_range Range>
requires std::is_convertible_v<std::ranges::range_value_t<Range>, Key>
BitwiseTrie<Key>::BitwiseTrie(Range&& range, std::size_t initial_size)
    : BitwiseTrie{initial_size}
{
    for(auto key : range) { Insert(key); }
}

template <BitKey Key>
std::size_t BitwiseTrie<Key>::popcnt(const Key& key)
{
    if constexpr (std::is_integral_v<Key>) {
        uint64_t word = static_cast<uint64_t>(key);
        uint64_t cnt;
        asm volatile(
            "popcnt %1, %0\n"
            : "=r" (cnt)
            : "r" (word)
        );
        return cnt;
    }else{
        std::size_t ret = 0;
        Key curr{key};
        while(curr != Key{0} && (ret < kKeyBits)) {
            if((curr & Key{0x1}) != Key{0})
                ret++;
            curr >>= 1;
        }
        return ret;
    }
}

template <BitKey Key>
std::size_t BitwiseTrie<Key>::tzcnt(const Key& key)
{
    if constexpr (std::is_integral_v<Key>) {
        uint64_t word = static_cast<uint64_t>(key);
        uint64_t cnt;
        asm volatile(
            "tzcnt %1, %0\n"
            : "=r" (cnt)
            : "r" (word)
        );
        return cnt;
    }else{
        std::size_t ret = 0;    
        Key curr{key};
        bool bit;
        do{
            bit = ((curr & Key{0x1}) != Key{0});
            if(!bit)
                ret++;
            curr >>= 1;
        }while(!bit && (ret < kKeyBits));
        return ret;
    }
}

template <BitKey Key>
std::size_t BitwiseTrie<Key>::clear_first_set(Key& key, Key& bit_pos)
{
    std::size_t cnt = tzcnt(key);
    if(cnt < kKeyBits) {
        bit_pos = (Key{1} << cnt);
        key ^= bit_pos;
    }else{
        bit_pos = Key{0};
    }
    return cnt;
}

template <BitKey Key>
Key BitwiseTrie<Key>::mask_up_to_bit(const Key& bit_pos)
{
    if constexpr (std::is_integral_v<Key>) {
        return static_cast<std::make_unsigned_t<Key>>(bit_pos) - 1;
    }else{
        std::size_t cnt = tzcnt(bit_pos);
        Key ret{};
        for(int i = 0; i < cnt; i++) {
            ret |= Key{1} << i;
        }
        return ret;
    }
}

template <BitKey Key>
auto BitwiseTrie<Key>::insert(node_ref_t node_idx, KeyPath key, 
    std::size_t offset) -> node_ref_t
{
    Key bit_map;
    if(node_idx == kEmptyNode) {
        bit_map = Key{0};
    }else{
        NodeHeader *header = reinterpret_cast<NodeHeader*>(&m_mem[node_idx]);
        bit_map = header->m_bitmask;
    }
    Key bit_pos = Key{1} << key[offset++];
    uint64_t idx = popcnt(bit_map & mask_up_to_bit(bit_pos));

    if((bit_map & bit_pos) == 0) {

        /* Child not present yet */
        index_or_segment_t value;
        if(offset == kKeyPathSegments - 1) {
            value = uint64_t(1) << key[offset];
        }else {
            value = create_leaf_node(key, offset);
        }
        return insert_child(node_idx, bit_map, bit_pos, idx, value);

    }else{
        /* Child present */
        if(offset == kKeyPathSegments - 1) {

            /* At leaf */
            index_or_segment_t value = m_mem[node_idx + kNodeHeaderWords + idx];
            index_or_segment_t bit_pos_leaf = index_or_segment_t{1} << key[offset];

            if((value & bit_pos_leaf) == 0) {
                /* Update leaf bitmap */
                m_mem[node_idx + kNodeHeaderWords + idx] |= bit_pos_leaf;
                return node_idx;
            }else{
                /* Key already present */
                return kEmptyNode;
            }
        }else{
            /* Not at leaf - recursion */
            node_ref_t child_node_idx = m_mem[node_idx + kNodeHeaderWords + idx];
            node_ref_t new_child_idx = insert(child_node_idx, key, offset);
            if(new_child_idx == kEmptyNode)
                return kEmptyNode;
            if(new_child_idx != child_node_idx)
                m_mem[node_idx + kNodeHeaderWords + idx] = new_child_idx;
            return node_idx;
        }
    }
}

template <BitKey Key>
auto BitwiseTrie<Key>::remove(node_ref_t node_idx, KeyPath key, std::size_t offset) -> node_ref_t
{
    if(m_root_idx == kEmptyNode)
        return kEmptyNode;

    NodeHeader *header = reinterpret_cast<NodeHeader*>(&m_mem[node_idx]);
    Key bit_map = header->m_bitmask;
    Key bit_pos = Key{1} << key[offset++];

    if((bit_map & bit_pos) == Key{0}) {
        /* Child not present, bit not found */
        return kEmptyNode;
    }else{

        uint64_t idx = popcnt(bit_map & mask_up_to_bit(bit_pos));
        index_or_segment_t value = m_mem[node_idx + kNodeHeaderWords + idx];
        if(offset == kKeyPathSegments - 1) {
            /* At leaf */
            index_or_segment_t bit_pos_leaf = index_or_segment_t{1} << key[offset];
            if((value & bit_pos_leaf) == index_or_segment_t{0}) {
                /* Node not present */
                return kEmptyNode;
            }else{
                /* Clear bit in leaf */
                value = value & ~bit_pos_leaf;
                if(value != index_or_segment_t{0}) {
                    /* Leaf still has some bits set, keep leaf but update */
                    m_mem[node_idx + kNodeHeaderWords + idx] = value;
                    return node_idx;
                }else{
                    return remove_child(node_idx, bit_map, bit_pos_leaf, idx);
                }
            }
        }else{
            /* Not at leaf */
            node_ref_t child_node_idx = value;
            node_ref_t new_child_node_idx = remove(child_node_idx, key, offset);
            if(new_child_node_idx == kEmptyNode)
                return kEmptyNode;
            if(new_child_node_idx == kDeletedNode)
                return remove_child(node_idx, bit_map, bit_pos, idx);
            if(new_child_node_idx != child_node_idx)
                m_mem[node_idx + kNodeHeaderWords + idx] = new_child_node_idx;
            return node_idx;
        }
    }
}

template <BitKey Key>
auto BitwiseTrie<Key>::allocate(std::size_t num_children) -> node_ref_t
{
    std::size_t size_class = num_children;
    assert(size_class < kNumFreelists);
    node_ref_t free = m_freelists[size_class];
    if(free != 0) {
        /* Requested size available in free list. Re-link and return head */
        m_freelists[size_class] = m_mem[free];
        NodeHeader *header = reinterpret_cast<NodeHeader*>(&m_mem[free]);
        header->m_bitmask = Key{0};
        return free;
    }else{
        /* Expansion required? */
        std::size_t size_words = kNodeHeaderWords + num_children;
        /* Round up to the next multiple of 2. This
         * ensueres that all our allocations are 
         * 16-byte aligned.
         */
        size_words = (size_words + 1) & -2;
        if(m_free_idx + size_words > m_memsize) {

            /* Double the size and assure this is enough */
            std::size_t newsize = std::max(m_memsize * 2, m_memsize + size_words);
            std::unique_ptr<uint64_t[]> newmem{new uint64_t[newsize]};
            std::memcpy(newmem.get(), m_mem.get(), m_free_idx * sizeof(uint64_t));
            m_memsize = newsize;
            m_mem = std::move(newmem);
        }
        node_ref_t idx = m_free_idx;
        m_free_idx += size_words;
        NodeHeader *header = reinterpret_cast<NodeHeader*>(&m_mem[idx]);
        header->m_bitmask = Key{0};
        return idx;
    }
}

template <BitKey Key>
void BitwiseTrie<Key>::deallocate(node_ref_t node_idx, std::size_t num_children)
{
    if(node_idx == kEmptyNode)
        return; /* Keep our known empty node */

    /* Add head to freelist */
    std::size_t size_class = num_children;
    assert(size_class < kNumFreelists);
    m_mem[node_idx] = m_freelists[size_class];
    m_freelists[size_class] = node_idx;
}

template <BitKey Key>
auto BitwiseTrie<Key>::reallocate_insert(node_ref_t node_idx, 
    std::size_t num_children, std::size_t child_idx) -> node_ref_t
{
    node_ref_t new_node_idx = allocate(num_children + 1);

    NodeHeader *newnode = reinterpret_cast<NodeHeader*>(&m_mem[new_node_idx]);
    NodeHeader *oldnode = reinterpret_cast<NodeHeader*>(&m_mem[node_idx]);
    if(node_idx != kEmptyNode) {
        newnode->m_bitmask = oldnode->m_bitmask;
    }else{
        newnode->m_bitmask = Key{0};
    }

    uint64_t a = new_node_idx + kNodeHeaderWords;
    uint64_t b = node_idx + kNodeHeaderWords;

    /* copy with gap for child */
    for(int i = 0; i < child_idx; i++)
        m_mem[a++] = m_mem[b++];
    a++; /* Inserted */
    for(int i = child_idx; i < num_children; i++)
        m_mem[a++] = m_mem[b++];

    deallocate(node_idx, num_children);
    return new_node_idx;
}

template <BitKey Key>
auto BitwiseTrie<Key>::reallocate_delete(node_ref_t node_idx, 
    std::size_t num_children, std::size_t child_idx) -> node_ref_t
{
    pe::assert(node_idx != kEmptyNode);
    node_ref_t new_node_idx = allocate(num_children - 1);

    NodeHeader *newnode = reinterpret_cast<NodeHeader*>(&m_mem[new_node_idx]);
    NodeHeader *oldnode = reinterpret_cast<NodeHeader*>(&m_mem[node_idx]);
    newnode->m_bitmask = oldnode->m_bitmask;

    uint64_t a = new_node_idx + kNodeHeaderWords;
    uint64_t b = node_idx + kNodeHeaderWords;

    /* copy with child removed */
    for(int i = 0; i < child_idx; i++)
        m_mem[a++] = m_mem[b++];
    b++; /* Removed */
    for(int i = child_idx + 1; i < num_children; i++)
        m_mem[a++] = m_mem[b++];

    deallocate(node_idx, num_children);
    return new_node_idx;
}

template <BitKey Key>
auto BitwiseTrie<Key>::create_leaf_node(KeyPath key, std::size_t offset) -> node_ref_t
{
    std::size_t len = kKeyPathSegments;
    node_ref_t new_node_idx = allocate(1);
    NodeHeader *header = reinterpret_cast<NodeHeader*>(&m_mem[new_node_idx]);
    header->m_bitmask = Key{1} << key[len - 2];
    m_mem[new_node_idx + kNodeHeaderWords] = index_or_segment_t{1} << key[len - 1];
    len -= 3;

    /* Create a chain of nodes from the leaf node 
     * to the subtrie root.
     */
    while(len >= offset) {
        node_ref_t new_parent_node_idx = allocate(1);
        NodeHeader *header = reinterpret_cast<NodeHeader*>(&m_mem[new_parent_node_idx]);
        header->m_bitmask = Key{1} << key[len--];
        m_mem[new_parent_node_idx + kNodeHeaderWords] = new_node_idx;
        new_node_idx = new_parent_node_idx;
    }
    return new_node_idx;
}

template <BitKey Key>
auto BitwiseTrie<Key>::insert_child(node_ref_t node_idx, Key key, 
    Key bit_pos, std::size_t child_idx, index_or_segment_t value) -> node_ref_t
{
    std::size_t size = popcnt(key);
    node_ref_t new_node_idx = reallocate_insert(node_idx, size, child_idx);
    NodeHeader *header = reinterpret_cast<NodeHeader*>(&m_mem[new_node_idx]);
    header->m_bitmask = key | bit_pos;
    m_mem[new_node_idx + kNodeHeaderWords + child_idx] = value;
    return new_node_idx;
}

template <BitKey Key>
auto BitwiseTrie<Key>::remove_child(node_ref_t node_idx, Key key, 
    Key bit_pos, std::size_t idx) -> node_ref_t
{
    std::size_t size = popcnt(key);
    if(size > 1) {
        /* Node still has other children/leafs */
        node_ref_t new_node_idx = reallocate_delete(node_idx, size, idx);
        NodeHeader *header = reinterpret_cast<NodeHeader*>(&m_mem[new_node_idx]);
        header->m_bitmask = key & ~bit_pos;
        return new_node_idx;
    }else{
        /* Node is now empty, remove it */
        deallocate(node_idx, size);
        return kDeletedNode;
    }
}

template <BitKey Key>
bool BitwiseTrie<Key>::Get(Key key) const
{
    if(m_root_idx == kEmptyNode)
        return false;

    KeyPath ikey{key};
    node_ref_t node_idx = m_root_idx;
    std::size_t offset = 0;

    while(true) {
        NodeHeader *header = reinterpret_cast<NodeHeader*>(&m_mem[node_idx]);
        Key bit_map = header->m_bitmask;
        Key bit_pos = Key{1} << ikey[offset++];

        if((bit_map & bit_pos) == 0)
            return false; /* Not found */

        uint64_t value_slot = 
            node_idx + kNodeHeaderWords + popcnt(bit_map & mask_up_to_bit(bit_pos));
        index_or_segment_t value = m_mem[value_slot];

        if(offset == kKeyPathSegments - 1) {
            /* At leaf */
            index_or_segment_t bit_pos_leaf = index_or_segment_t{1} << ikey[offset];
            if((value & bit_pos_leaf) != 0) {
                return true;
            }
            return false;
        }else{
            /* child pointer */
            node_idx = value;
        }
    }
}

template <BitKey Key>
bool BitwiseTrie<Key>::Insert(Key key)
{
    KeyPath ikey{key};
    node_ref_t node_idx = insert(m_root_idx, ikey, 0);
    if(node_idx != kEmptyNode) {
        /* Denotes change */
        m_node_count++;
        m_root_idx = node_idx;
        return true;
    }
    return false;
}

template <BitKey Key>
bool BitwiseTrie<Key>::Remove(Key key)
{
    KeyPath ikey{key};
    node_ref_t node_idx = remove(m_root_idx, ikey, 0);
    if(node_idx != kEmptyNode) {
        m_node_count--;
        if(node_idx == kDeletedNode) {
            m_root_idx = kEmptyNode;
        }else{
            m_root_idx = node_idx;
        }
        return true;
    }
    return false;
}

template <BitKey Key>
std::size_t BitwiseTrie<Key>::Size() const
{
    return m_node_count;
}

template <BitKey Key>
auto BitwiseTrie<Key>::begin() const noexcept -> iterator
{
    return iterator{IdentityVirtualNode{m_mem.get(), m_root_idx}};
}

template <BitKey Key>
auto BitwiseTrie<Key>::end() const noexcept -> iterator
{
    return iterator{IdentityVirtualNode{m_mem.get(), kEmptyNode}};
}

/*****************************************************************************/
/* ITERATOR                                                                  */
/*****************************************************************************/

template <BitKey Key>
template <CVirtualNode<Key> Node>
void BitwiseTrie<Key>::Iterator<Node>::push_ctx()
{
    m_recursion_stack.push(m_ctx);
}

template <BitKey Key>
template <CVirtualNode<Key> Node>
bool BitwiseTrie<Key>::Iterator<Node>::try_pop_ctx()
{
    if(m_recursion_stack.empty()) {
        m_ctx.m_stage = GeneratorContext::Stage::eFinished;
        m_ctx.m_key = Key{0};
        return false;
    }
    m_ctx = m_recursion_stack.top();
    m_recursion_stack.pop();
    return true;
}

template <BitKey Key>
template <CVirtualNode<Key> Node>
void BitwiseTrie<Key>::Iterator<Node>::next_key()
{
    switch(m_ctx.m_stage) {
    case GeneratorContext::Stage::eProcessRoot:
        if(m_ctx.m_node.is_sentinel()) {
            if(try_pop_ctx()) {
                return next_key();
            }
            return;
        }
        m_ctx.m_bits = m_ctx.m_node.get_bitmap();
        if(m_ctx.m_bits == Key{0}) {
            if(try_pop_ctx()) {
                return next_key();
            }
            return;
        }
        [[fallthrough]];
    case GeneratorContext::Stage::eIterateChildren:
        m_ctx.m_stage = GeneratorContext::Stage::eIterateChildren;
        while(m_ctx.m_bits != Key{0}) {
            m_ctx.m_bit_num = clear_first_set(m_ctx.m_bits, m_ctx.m_bit_pos);
            m_ctx.m_key.SetSegment(m_ctx.m_offset, static_cast<uint8_t>(m_ctx.m_bit_num));

            if(m_ctx.m_offset == kKeyPathSegments - 2) {
                m_ctx.m_leaf_bits = m_ctx.m_node.get_bitmap_leaf(m_ctx.m_bit_pos);
                if(m_ctx.m_leaf_bits != 0) {
                    m_ctx.m_stage = GeneratorContext::Stage::eIterateLeaf;
                    return next_key();
                }
            }else{
                /* recurse */
                push_ctx();
                m_ctx.m_node = m_ctx.m_node.get_child_node(m_ctx.m_bit_pos);
                m_ctx.m_offset++;
                m_ctx.m_stage = GeneratorContext::Stage::eProcessRoot;
                return next_key();
            }
        }
        if(try_pop_ctx()) {
            return next_key();
        }
        break;
    case GeneratorContext::Stage::eIterateLeaf:
        if(m_ctx.m_leaf_bits == 0) {
            m_ctx.m_stage = GeneratorContext::Stage::eIterateChildren;
            return next_key();
        }
        /* Get rightmost bit and clear it */
        m_ctx.m_leaf_bit_pos = m_ctx.m_leaf_bits & -int64_t(m_ctx.m_leaf_bits);
        m_ctx.m_leaf_bits ^= m_ctx.m_leaf_bit_pos;
        m_ctx.m_leaf_bit_num = tzcnt(m_ctx.m_leaf_bit_pos);
        m_ctx.m_key.SetSegment(m_ctx.m_offset + 1, static_cast<uint8_t>(m_ctx.m_leaf_bit_num));
        /* m_key now holds the next key */
        break;
    case GeneratorContext::Stage::eFinished:
        pe::assert(0);
        break;
    }
}

/*****************************************************************************/
/* TRIE VIEW                                                                 */
/*****************************************************************************/

template <BitKey Key>
class enable_trie_view
{};

export 
template <BitKey Key>
class trie_view : public std::ranges::view_interface<trie_view<Key>>
                , public enable_trie_view<Key>
{
private:

    BitwiseTrie<Key>::iterator m_begin;
    BitwiseTrie<Key>::iterator m_end;

public:

    using iterator = BitwiseTrie<Key>::iterator;
    using key_type = Key;

    trie_view(const BitwiseTrie<Key>& trie)
        : m_begin{trie.begin()}
        , m_end{trie.end()}
    {}

    auto begin() const { return m_begin; }
    auto end()   const { return m_end;   }
};

/*****************************************************************************/
/* TRIE VIEW INTERSECTION                                                    */
/*****************************************************************************/

export
template <
    BitKey Key, 
    std::derived_from<enable_trie_view<Key>> AView, 
    std::derived_from<enable_trie_view<Key>> BView
>
class trie_view_intersection : public std::ranges::view_interface<trie_view_intersection<Key, AView, BView>>
                             , public enable_trie_view<Key>
{
private:

    struct AndVirtualNode
    {
    private:

        using ANode = typename AView::iterator::node_type;
        using BNode = typename BView::iterator::node_type;

        ANode m_node_a;
        BNode m_node_b;
        Key   m_bitmap_a;
        Key   m_bitmap_b;
        
    public:

        AndVirtualNode(ANode a, BNode b)
            : m_node_a{a}
            , m_node_b{b}
            , m_bitmap_a{!a.is_sentinel() ? a.get_bitmap() : Key{0}}
            , m_bitmap_b{!b.is_sentinel() ? b.get_bitmap() : Key{0}}
        {}

        Key get_bitmap() const
        {
            /* This gives a nice optimization (pruning) */
            return m_bitmap_a & m_bitmap_b;
        }

        uint64_t get_bitmap_leaf(Key bit_pos)
        {
            return m_node_a.get_bitmap_leaf(bit_pos) & m_node_b.get_bitmap_leaf(bit_pos);
        }

        AndVirtualNode get_child_node(Key bit_pos)
        {
            auto child_a = m_node_a.get_child_node(bit_pos);
            auto child_b = m_node_b.get_child_node(bit_pos);
            return AndVirtualNode{child_a, child_b};
        }

        bool is_sentinel()
        {
            return m_node_a.is_sentinel() || m_node_b.is_sentinel();
        }
    };

    BitwiseTrie<Key>::template Iterator<AndVirtualNode> m_begin;
    BitwiseTrie<Key>::template Iterator<AndVirtualNode> m_end;

public:

    using iterator = BitwiseTrie<Key>::template Iterator<AndVirtualNode>;
    using key_type = Key;

    trie_view_intersection(const AView& a, const BView& b)
        : m_begin{iterator{AndVirtualNode{a.begin().root(), b.begin().root()}}}
        , m_end{iterator{AndVirtualNode{a.end().root(), b.end().root()}}}
    {}

    auto begin() const { return m_begin; }
    auto end()   const { return m_end;   }
};

template <typename AView, typename BView>
trie_view_intersection(const AView&, const BView&)
    -> trie_view_intersection<typename AView::key_type, AView, BView>;

/*****************************************************************************/
/* TRIE VIEW UNION                                                           */
/*****************************************************************************/

export
template <
    BitKey Key, 
    std::derived_from<enable_trie_view<Key>> AView, 
    std::derived_from<enable_trie_view<Key>> BView
>
class trie_view_union : public std::ranges::view_interface<trie_view_union<Key, AView, BView>>
                      , public enable_trie_view<Key>
{
private:

    struct OrVirtualNode
    {
    private:

        using ANode = typename AView::iterator::node_type;
        using BNode = typename BView::iterator::node_type;

        std::optional<ANode> m_node_a;
        std::optional<BNode> m_node_b;
        Key                  m_bitmap_a;
        Key                  m_bitmap_b;
        
    public:

        struct NoNodeA{};
        struct NoNodeB{};

        OrVirtualNode(ANode a, BNode b)
            : m_node_a{a}
            , m_node_b{b}
            , m_bitmap_a{!a.is_sentinel() ? a.get_bitmap() : Key{0}}
            , m_bitmap_b{!b.is_sentinel() ? b.get_bitmap() : Key{0}}
        {}

        OrVirtualNode(ANode a, ANode b, NoNodeA)
            : m_node_a{std::nullopt}
            , m_node_b{b}
            , m_bitmap_a{!b.is_sentinel() ? b.get_bitmap() : Key{0}}
            , m_bitmap_b{!b.is_sentinel() ? b.get_bitmap() : Key{0}}
        {}

        OrVirtualNode(ANode a, ANode b, NoNodeB)
            : m_node_a{a}
            , m_node_b{std::nullopt}
            , m_bitmap_a{!a.is_sentinel() ? a.get_bitmap() : Key{0}}
            , m_bitmap_b{!a.is_sentinel() ? a.get_bitmap() : Key{0}}
        {}

        Key get_bitmap() const
        {
            return m_bitmap_a | m_bitmap_b;
        }

        uint64_t get_bitmap_leaf(Key bit_pos)
        {
            if(m_node_a && m_node_b) {
                return m_node_a->get_bitmap_leaf(bit_pos) | m_node_b->get_bitmap_leaf(bit_pos);
            }else if(m_node_a){
                return m_node_a->get_bitmap_leaf(bit_pos);
            }else{
                return m_node_b->get_bitmap_leaf(bit_pos);
            }
        }

        OrVirtualNode get_child_node(Key bit_pos)
        {
            if(m_node_a && (m_bitmap_a & bit_pos) != Key{0}) {
                auto child_a = m_node_a->get_child_node(bit_pos);
                if(m_node_b && (m_bitmap_b & bit_pos) != Key{0}) {
                    auto child_b = m_node_b->get_child_node(bit_pos);
                    return OrVirtualNode{child_a, child_b};
                }
                return OrVirtualNode{child_a, *m_node_b, NoNodeB{}};
            }
            auto child_b = m_node_b->get_child_node(bit_pos);
            return OrVirtualNode{*m_node_a, child_b, NoNodeA{}};
        }

        bool is_sentinel()
        {
            return (m_node_a && m_node_a->is_sentinel()) 
                && (m_node_b && m_node_b->is_sentinel());
        }
    };

    BitwiseTrie<Key>::template Iterator<OrVirtualNode> m_begin;
    BitwiseTrie<Key>::template Iterator<OrVirtualNode> m_end;

public:

    using iterator = BitwiseTrie<Key>::template Iterator<OrVirtualNode>;
    using key_type = Key;

    trie_view_union(const AView& a, const BView& b)
        : m_begin{iterator{OrVirtualNode{a.begin().root(), b.begin().root()}}}
        , m_end{iterator{OrVirtualNode{a.end().root(), b.end().root()}}}
    {}

    auto begin() const { return m_begin; }
    auto end()   const { return m_end;   }
};

template <typename AView, typename BView>
trie_view_union(const AView&, const BView&)
    -> trie_view_union<typename AView::key_type, AView, BView>;

/*****************************************************************************/
/* TRIE VIEW DIFFERENCE                                                      */
/*****************************************************************************/

export
template <
    BitKey Key, 
    std::derived_from<enable_trie_view<Key>> AView, 
    std::derived_from<enable_trie_view<Key>> BView
>
class trie_view_difference : public std::ranges::view_interface<trie_view_difference<Key, AView, BView>>
                           , public enable_trie_view<Key>
{
private:

    struct MinusVirtualNode
    {
    private:

        using ANode = typename AView::iterator::node_type;
        using BNode = typename BView::iterator::node_type;

        ANode m_node_a;
        BNode m_node_b;
        Key   m_bitmap_a;
        Key   m_bitmap_b;
        
    public:

        struct NoMinuend{};

        MinusVirtualNode(ANode a, BNode b)
            : m_node_a{a}
            , m_node_b{b}
            , m_bitmap_a{!a.is_sentinel() ? a.get_bitmap() : Key{0}}
            , m_bitmap_b{!b.is_sentinel() ? b.get_bitmap() : Key{0}}
        {}

        MinusVirtualNode(ANode a, BNode b, NoMinuend)
            : m_node_a{a}
            , m_node_b{b}
            , m_bitmap_a{!a.is_sentinel() ? a.get_bitmap() : Key{0}}
            , m_bitmap_b{Key{0}}
        {}

        Key get_bitmap() const
        {
            /* bitmap B not useful here */
            return m_bitmap_a;
        }

        uint64_t get_bitmap_leaf(Key bit_pos)
        {
            uint64_t child_bitmap_a = m_node_a.get_bitmap_leaf(bit_pos);
            if((m_bitmap_b & bit_pos) == Key{0})
                return child_bitmap_a;

            uint64_t child_bitmap_b = m_node_b.get_bitmap_leaf(bit_pos);
            return child_bitmap_a & ~child_bitmap_b;
        }

        MinusVirtualNode get_child_node(Key bit_pos)
        {
            auto child_a = m_node_a.get_child_node(bit_pos);
            if((m_bitmap_b & bit_pos) == Key{0})
                return MinusVirtualNode(child_a, m_node_b, NoMinuend{});

            auto child_b = m_node_b.get_child_node(bit_pos);
            return MinusVirtualNode{child_a, child_b};
        }

        bool is_sentinel()
        {
            return m_node_a.is_sentinel();
        }
    };

    BitwiseTrie<Key>::template Iterator<MinusVirtualNode> m_begin;
    BitwiseTrie<Key>::template Iterator<MinusVirtualNode> m_end;

public:

    using iterator = BitwiseTrie<Key>::template Iterator<MinusVirtualNode>;
    using key_type = Key;

    trie_view_difference(const AView& a, const BView& b)
        : m_begin{iterator{MinusVirtualNode{a.begin().root(), b.begin().root()}}}
        , m_end{iterator{MinusVirtualNode{a.end().root(), b.end().root()}}}
    {}

    auto begin() const { return m_begin; }
    auto end()   const { return m_end;   }
};

template <typename AView, typename BView>
trie_view_difference(const AView&, const BView&)
    -> trie_view_difference<typename AView::key_type, AView, BView>;

/*****************************************************************************/
/* TRIE VIEW RANGE                                                           */
/*****************************************************************************/

export 
template <
    BitKey Key,
    std::derived_from<enable_trie_view<Key>> View
>
class trie_view_range : public std::ranges::view_interface<trie_view_range<Key, View>>
                      , public enable_trie_view<Key>
{
private:

    using trie_type = BitwiseTrie<Key>;

    struct RangeVirtualNode
    {
    private:

        using Node = typename View::iterator::node_type;
        
        Node    m_node;
        Key     m_a, m_b;
        uint8_t m_x, m_y;
        Key     m_mask;
        int     m_level;

        uint64_t mask(uint8_t x, uint8_t y)
        {
            /* Bit hack for: 
             * for(int i = x; i <= y; i++)
             *     m_bitmap |= (Key{1} << i);
             */
            uint64_t mask = uint64_t(1) << y;
            mask |= (mask - 1);
            mask &= ~((uint64_t(1) << x) - 1);
            return mask;
        }
        
    public:

        RangeVirtualNode(Node node, Key a, Key b, int level)
            : m_node{node}
            , m_a{a}
            , m_b{b}
            , m_x{typename trie_type::KeyPath{a}[level]}
            , m_y{typename trie_type::KeyPath{b}[level]}
            , m_mask{mask(m_x, m_y)}
            , m_level{level}
        {}

        Key get_bitmap() const
        {
            return m_node.get_bitmap() & Key{m_mask};
        }

        uint64_t get_bitmap_leaf(Key bit_pos)
        {
            uint8_t x = typename trie_type::KeyPath{m_a}[m_level + 1];
            uint8_t y = typename trie_type::KeyPath{m_b}[m_level + 1];
            return m_node.get_bitmap_leaf(bit_pos) & mask(x, y);
        }

        RangeVirtualNode get_child_node(Key bit_pos)
        {
            std::size_t bit_num = trie_type::tzcnt(bit_pos);
            auto child = m_node.get_child_node(bit_pos);
            if(m_x == m_y) {
                return RangeVirtualNode{child, m_a, m_b, m_level + 1};
            }else if(bit_num == m_x) {
                return RangeVirtualNode{child, m_a, ~Key{0}, m_level + 1};
            }else if(bit_num == m_y) {
                return RangeVirtualNode{child, Key{0}, m_b, m_level + 1};
            }else{
                return RangeVirtualNode{child, Key{0}, ~Key{0}, m_level + 1};
            }
        }

        bool is_sentinel()
        {
            return m_node.is_sentinel();
        }
    };

    BitwiseTrie<Key>::template Iterator<RangeVirtualNode> m_begin;
    BitwiseTrie<Key>::template Iterator<RangeVirtualNode> m_end;

public:

    using iterator = BitwiseTrie<Key>::template Iterator<RangeVirtualNode>;
    using key_type = Key;

    trie_view_range(const View& view, Key a, Key b)
        : m_begin{RangeVirtualNode{view.begin().root(), a, b, 0}}
        , m_end{RangeVirtualNode{view.end().root(), a, b, 0}}
    {}

    auto begin() const { return m_begin; }
    auto end()   const { return m_end;   }
};

template <typename View>
trie_view_range(const View&, typename View::key_type, typename View::key_type) 
    -> trie_view_range<typename View::key_type, View>;

/*****************************************************************************/
/* TRIE VIEW MATCH MASK                                                      */
/*****************************************************************************/

export
template <
    BitKey Key,
    std::derived_from<enable_trie_view<Key>> View
> class trie_view_match_mask : public std::ranges::view_interface<trie_view_match_mask<Key, View>>
                             , public enable_trie_view<Key>
{
private:

    using trie_type = BitwiseTrie<Key>;

    struct MatchMaskVirtualNode
    {
    private:

        using Node = typename View::iterator::node_type;
        
        Node     m_node;
        Key      m_mask;
        uint64_t m_segmask;
        int      m_level;

        template <uint8_t... I>
        constexpr static auto make_array(std::integer_sequence<uint8_t, I...> seq)
        {
            return std::array<uint8_t, sizeof...(I)>{I...};
        };

        uint64_t matching_segmask(Key mask, int level)
        {
            uint64_t ret = 0;
            uint8_t segment = typename trie_type::KeyPath{mask}[level];

            alignas(16) constexpr auto static s_indices = 
                make_array(std::make_integer_sequence<uint8_t, 64>{});

            for(uint8_t i = 0; i < 4; i++) {
                auto indices = _mm_load_si128(reinterpret_cast<const __m128i*>(&s_indices[i * 16]));
                auto segments = _mm_set1_epi8(segment);
                auto anded = _mm_and_si128(indices, segments);
                uint64_t mask = static_cast<uint64_t>(
                    _mm_movemask_epi8(_mm_cmpeq_epi8(anded, segments)));
                mask <<= (i * 16);
                ret |= mask;
            }
            return ret;
        }
        
    public:

        MatchMaskVirtualNode(Node node, Key mask, int level)
            : m_node{node}
            , m_mask{mask}
            , m_segmask{matching_segmask(mask, level)}
            , m_level{level}
        {}

        Key get_bitmap() const
        {
            return m_node.get_bitmap() & Key{m_segmask};
        }

        uint64_t get_bitmap_leaf(Key bit_pos)
        {
            uint64_t segmask = matching_segmask(m_mask, m_level + 1);
            return m_node.get_bitmap_leaf(bit_pos) & segmask;
        }

        MatchMaskVirtualNode get_child_node(Key bit_pos)
        {
            auto child = m_node.get_child_node(bit_pos);
            return MatchMaskVirtualNode{child, m_mask, m_level + 1};
        }

        bool is_sentinel()
        {
            return m_node.is_sentinel();
        }
    };

    BitwiseTrie<Key>::template Iterator<MatchMaskVirtualNode> m_begin;
    BitwiseTrie<Key>::template Iterator<MatchMaskVirtualNode> m_end;

public:

    using iterator = BitwiseTrie<Key>::template Iterator<MatchMaskVirtualNode>;
    using key_type = Key;

    trie_view_match_mask(const View& view, Key mask)
        : m_begin{MatchMaskVirtualNode{view.begin().root(), mask, 0}}
        , m_end{MatchMaskVirtualNode{view.end().root(), mask, 0}}
    {}

    auto begin() const { return m_begin; }
    auto end()   const { return m_end;   }
};

template <typename View>
trie_view_match_mask(const View&, typename View::key_type)
    -> trie_view_match_mask<typename View::key_type, View>;

} //namespace pe

