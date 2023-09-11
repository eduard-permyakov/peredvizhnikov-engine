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

export module flat_hash_map;

import meta;

import <functional>;
import <utility>;
import <iterator>;
import <ranges>;
import <compare>;
import <initializer_list>;
import <memory>;
import <algorithm>;
import <tuple>;
import <functional>;
import <limits>;
import <exception>;

namespace pe{

/*
 * Implementation of dense array map structure based on the design of 
 * Google's absl::flat_hash_map from the talk "Designing a Fast, Efficient,
 * Cache-friendly Hash Table, Step by Step." It allows efficient iteration 
 * of all elements due to them being stored in a single flat array.
 * Furthermore, it allows for fast probing as most probes only access the
 * metadata bytes of the bin, which are packed together for efficient
 * forward iteration. The container invalidates all iterators when rehashing.
 * The API is similar to that of C++23's std::flat_hash_map but focusing on 
 * highly optimized table scans and forward iteration rather than trying to 
 * be a completely generic container adapter. Ordering of elements is not 
 * maintained.
 */

template <typename T>
concept CopyableOrMovable = std::copyable<T> or std::movable<T>;

export
template <CopyableOrMovable Key, 
          CopyableOrMovable T,
          typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>,
          typename KeyAllocator = std::allocator<Key>,
          typename MappedAllocator = std::allocator<T>>
class FlatHashMap
{
    template <typename KeyType, typename ValueType, 
              typename IteratorTag, bool Reverse = false> class Iterator;

public:

    using key_type               = Key;
    using mapped_type            = T;
    using value_type             = std::pair<key_type, mapped_type>;
    using key_equal              = KeyEqual;
    using key_allocator_type     = KeyAllocator;
    using mapped_allocator_type  = MappedAllocator;
    using reference              = std::pair<const key_type&, mapped_type&>;
    using const_reference        = std::pair<const key_type&, const mapped_type&>;
    using size_type              = size_t;
    using difference_type        = ptrdiff_t;
    using hasher                 = Hash;
    using iterator               = Iterator<key_type, mapped_type, std::bidirectional_iterator_tag>;
    using const_iterator         = Iterator<const key_type, const mapped_type, 
                                            std::bidirectional_iterator_tag>;
    using reverse_iterator       = Iterator<key_type, mapped_type, 
                                            std::bidirectional_iterator_tag, true>;
    using const_reverse_iterator = Iterator<const key_type, const mapped_type, 
                                            std::bidirectional_iterator_tag, true>;
    using ctrl_t                 = int8_t;

    static inline constexpr size_type kGroupSize = 16;
    static inline constexpr float kMaxLoadFactor = 0.75f;

    FlatHashMap() : FlatHashMap(kGroupSize) {}

    FlatHashMap(size_type min_bucket_count, 
        const Hash& hash = Hash{}, const key_equal& equal = KeyEqual{},
        const KeyAllocator& key_alloc = KeyAllocator{},
        const MappedAllocator& mapped_alloc = MappedAllocator{});

    template <std::input_iterator InputIterator>
    requires requires (InputIterator it) {
        {std::tuple_size_v<decltype(*it)> == 2};
        requires std::convertible_to<
            std::tuple_element_t<0, std::iter_value_t<std::remove_pointer_t<decltype(it)>>>, Key>;
        requires std::convertible_to<
            std::tuple_element_t<1, std::iter_value_t<std::remove_pointer_t<decltype(it)>>>, T>;
    }
    FlatHashMap(InputIterator first, InputIterator last, size_type min_bucket_count = kGroupSize,
        const Hash& hash = Hash{}, const key_equal& equal = KeyEqual{},
        const KeyAllocator& key_alloc = KeyAllocator{}, 
        const MappedAllocator& mapped_alloc = MappedAllocator{});

    template <std::ranges::input_range Range>
    requires requires (std::ranges::range_value_t<Range> value) {
        {std::tuple_size_v<std::ranges::range_value_t<Range>> == 2};
        {std::get<0>(value)} -> std::convertible_to<Key>;
        {std::get<1>(value)} -> std::convertible_to<T>;
    }
    FlatHashMap(Range&& range, size_type min_bucket_count = kGroupSize,
        const Hash& hash = Hash{}, const key_equal& equal = KeyEqual{},
        const KeyAllocator& key_alloc = KeyAllocator{},
        const MappedAllocator& mapped_alloc = MappedAllocator{});

    template <typename K, typename V>
    FlatHashMap(std::initializer_list<std::pair<K, V>> init, size_type min_bucket_count = kGroupSize,
        const Hash& hash = Hash{}, const key_equal& equal = KeyEqual{},
        const KeyAllocator& key_alloc = KeyAllocator{},
        const MappedAllocator& mapped_alloc = MappedAllocator{});

    FlatHashMap(FlatHashMap&&);
    FlatHashMap& operator=(FlatHashMap&&);

    FlatHashMap(FlatHashMap const&);
    FlatHashMap& operator=(FlatHashMap const&);

    ~FlatHashMap() = default;

    /* Iterators
     */
    iterator               begin() noexcept;
    const_iterator         begin() const noexcept;
    iterator               end() noexcept;
    const_iterator         end() const noexcept;
 
    reverse_iterator       rbegin() noexcept;
    const_reverse_iterator rbegin() const noexcept;
    reverse_iterator       rend() noexcept;
    const_reverse_iterator rend() const noexcept;
 
    const_iterator         cbegin() const noexcept;
    const_iterator         cend() const noexcept;
    const_reverse_iterator crbegin() const noexcept;
    const_reverse_iterator crend() const noexcept;

    /* Capacity
     */
    [[nodiscard]] bool empty() const noexcept;
    size_type size() const noexcept;
    size_type max_size() const noexcept;

    /* Element access
     */
    mapped_type& operator[](const key_type& x);
    mapped_type& operator[](key_type&& x);
    template<class K> mapped_type& operator[](K&& x);
    mapped_type& at(const key_type& x);
    const mapped_type& at(const key_type& x) const;
    template<class K> mapped_type& at(const K& x);
    template<class K> const mapped_type& at(const K& x) const;

    /* Modifiers
     */
    template<class... Args> std::pair<iterator, bool> emplace(Args&&... args);
    template<class... Args>
    iterator emplace_hint(const_iterator position, Args&&... args);
 
    std::pair<iterator, bool> insert(const value_type& x)
    { return emplace(x); }
    std::pair<iterator, bool> insert(value_type&& x)
    { return emplace(std::move(x)); }
    iterator insert(const_iterator position, const value_type& x)
    { return emplace_hint(position, x); }
    iterator insert(const_iterator position, value_type&& x)
    { return emplace_hint(position, std::move(x)); }

    template<class P> 
    std::pair<iterator, bool> insert(P&& x);
    template<class P> 
    iterator insert(const_iterator position, P&&);
    template<std::input_iterator InputIterator> 
    void insert(InputIterator first, InputIterator last);
    template<std::ranges::input_range R> void insert_range(R&& rg);
    void insert(std::initializer_list<value_type> il)
    { insert(il.begin(), il.end()); }

    template<class M>
    std::pair<iterator, bool> insert_or_assign(const key_type& k, M&& obj);
    template<class M>
    std::pair<iterator, bool> insert_or_assign(key_type&& k, M&& obj);
    template<class K, class M>
    std::pair<iterator, bool> insert_or_assign(K&& k, M&& obj);
    template<class M>
    iterator insert_or_assign(const_iterator hint, const key_type& k, M&& obj);
    template<class M>
    iterator insert_or_assign(const_iterator hint, key_type&& k, M&& obj);
    template<class K, class M>
    iterator insert_or_assign(const_iterator hint, K&& k, M&& obj);

    iterator erase(const_iterator position);
    iterator erase(iterator position) { return erase(const_iterator{position}); };
    size_type erase(const key_type& x);
    template<class K> size_type erase(K&& x);
    iterator erase(const_iterator first, const_iterator last);
 
    void swap(FlatHashMap& y) noexcept;
    void clear() noexcept;
    void rehash(size_type min_bucket_count);

    /* Map Operations
     */
    iterator find(const key_type& x);
    const_iterator find(const key_type& x) const;
    template<class K> iterator find(const K& x);
    template<class K> const_iterator find(const K& x) const;
 
    bool contains(const key_type& x) const;
    template<class K> bool contains(const K& x) const;
 
    bool operator==(const FlatHashMap& y) const;

    friend void swap(FlatHashMap& x, FlatHashMap& y) noexcept
    { x.swap(y); }

    float load_factor() const
    {
        return ((float)m_loaded_bins) / m_capacity;
    }

private:

    enum Ctrl : ctrl_t
    {
        eEmpty = -128,  // 0b10000000
        eDeleted = -1,  // 0b11111111
        // Full         // 0b0xxxxxxx
    };

    std::size_t H1(std::size_t hash) const noexcept { return (hash >> 7);   }
    ctrl_t      H2(std::size_t hash) const noexcept { return (hash & 0x7f); }

    static constexpr std::size_t ngroups(std::size_t min_bucket_count)
    {
        return (min_bucket_count / kGroupSize) + !!(min_bucket_count % kGroupSize);
    }

    static std::size_t next_free_bin(std::size_t capacity, std::size_t start, const Ctrl *metadata);
    static std::size_t next_full_bin(std::size_t capacity, std::size_t start, const Ctrl *metadata);
    static std::size_t first_full_bin(std::size_t capacity, const Ctrl *metadata);
    static std::size_t last_full_bin(std::size_t capacity, const Ctrl *metadata);
    static std::size_t prev_full_bin(std::size_t capacity, std::size_t start, const Ctrl *metadata);

    inline iterator iterator_at(std::size_t bin) const noexcept
    {
        return {bin, m_capacity, m_metadata.get(), m_keys.get(), m_values.get()};
    }

    inline const_iterator const_iterator_at(std::size_t bin) const noexcept
    {
        return {bin, m_capacity, m_metadata.get(), m_keys.get(), m_values.get()};
    }

    inline reverse_iterator reverse_iterator_at(std::size_t bin) const noexcept
    {
        return {bin, m_capacity, m_metadata.get(), m_keys.get(), m_values.get()};
    }

    inline const_reverse_iterator const_reverse_iterator_at(std::size_t bin) const noexcept
    {
        return {bin, m_capacity, m_metadata.get(), m_keys.get(), m_values.get()};
    }

    iterator find(const key_type& key, std::size_t hash) const;

    /* Since the keys and values are stored disjointly and we don't want to
     * copy them, we package the references/pointers to the keys and values
     * into a std::pair. Note, however, that this means that the semantics of
     * the iterator are such that the reference and pointer types must be
     * 'unpacked' before writing to them.
     */
    template <typename KeyType, typename ValueType, typename IteratorTag, bool Reverse>
    class Iterator
    {
    public:

        using iterator_category = IteratorTag;
        using difference_type   = std::ptrdiff_t;
        using value_type        = const std::pair<KeyType&, ValueType&>;
        using pointer           = const std::pair<KeyType*, ValueType*>;
        using reference         = const std::pair<KeyType&, ValueType&>;

        friend class FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>;

    private:

        std::size_t  m_bin_idx;
        std::size_t  m_capacity;
        const Ctrl  *m_metadata;
        key_type    *m_keys;
        mapped_type *m_values;

        Iterator(std::size_t bin_idx, std::size_t capacity, 
            Ctrl *ctrl, key_type *keys, mapped_type *values)
            : m_bin_idx{bin_idx}
            , m_capacity{capacity}
            , m_metadata{ctrl}
            , m_keys{keys}
            , m_values{values}
        {}

    public:

        Iterator() = default;
        Iterator(Iterator const& other) = default;
        Iterator& operator=(Iterator const&) = default;

        /* construct const_iterator from iterator */
        template <typename IterType = Iterator>
        requires std::is_same_v<IterType, const_iterator>
        Iterator(iterator const& other)
            : m_bin_idx{other.m_bin_idx}
            , m_capacity{other.m_capacity}
            , m_metadata{other.m_metadata}
            , m_keys{other.m_keys}
            , m_values{other.m_values}
        {}

        template <typename IterType = Iterator>
        requires std::is_same_v<IterType, const_reverse_iterator>
        Iterator(reverse_iterator const& other)
            : m_bin_idx{other.m_bin_idx}
            , m_capacity{other.m_capacity}
            , m_metadata{other.m_metadata}
            , m_keys{other.m_keys}
            , m_values{other.m_values}
        {}

        reference operator*() const
        {
            return reference{m_keys[m_bin_idx], m_values[m_bin_idx]};
        }

        pointer operator->()
        {
            return pointer{&m_keys[m_bin_idx], &m_values[m_bin_idx]};
        }

        Iterator& operator++()
        {
            if constexpr (Reverse) {
                m_bin_idx = prev_full_bin(m_capacity, m_bin_idx, m_metadata);
                return *this;
            }else{
                m_bin_idx = next_full_bin(m_capacity, m_bin_idx, m_metadata);
                return *this;
            }
        }

        Iterator operator++(int)
        {
            Iterator ret = *this;
            ++(*this);
            return ret;
        }

        template <typename Tag = IteratorTag>
        requires (std::is_same_v<Tag, std::bidirectional_iterator_tag>)
        Iterator& operator--()
        {
            if constexpr (Reverse) {
                m_bin_idx = next_full_bin(m_capacity, m_bin_idx, m_metadata);
                return *this;
            }else{
                m_bin_idx = prev_full_bin(m_capacity, m_bin_idx, m_metadata);
                return *this;
            }
        }

        template <typename Tag = IteratorTag>
        requires (std::is_same_v<Tag, std::bidirectional_iterator_tag>)
        Iterator operator--(int)
        {
            Iterator ret = *this;
            --(*this);
            return ret;
        }

        friend bool operator==(const Iterator& a, const Iterator& b)
        {
            return a.m_bin_idx == b.m_bin_idx; 
        };

        friend bool operator!=(const Iterator& a, const Iterator& b)
        {
            return a.m_bin_idx != b.m_bin_idx; 
        };
    };

    template <std::integral Integral>
    struct BitMask
    {
        constexpr static inline std::size_t kNumBits = sizeof(Integral) * 8;
        static_assert(kNumBits >= kGroupSize);

        alignas(16) Integral m_value;
        std::size_t          m_curr;

        BitMask(Integral value, std::size_t start = {})
            : m_value{value}
            , m_curr{start}
        {}

        operator bool() const
        {
            return m_value;
        }

        std::size_t LastSet() const
        {
            Integral trailing;
            asm volatile(
                "tzcnt %1, %0\n"
                : "=r" (trailing)
                : "r" (m_value)
            );
            return (31 - trailing);
        }

        std::size_t FirstSet() const
        {
            Integral first;
            asm volatile(
                "lzcnt %1, %0\n"
                : "=r" (first)
                : "r" (m_value)
            );
            return first;
        }

        BitMask begin()
        {
            return {m_value, FirstSet()};
        }

        BitMask end()
        {
            return {m_value, kNumBits};
        }

        BitMask& operator++()
        {
            std::size_t shift = m_curr + 1;
            if(shift == kNumBits) {
                m_curr = kNumBits;
                return *this;
            }

            Integral shifted = m_value << shift;
            Integral first;
            asm volatile(
                "lzcnt %1, %0\n"
                : "=r" (first)
                : "r" (shifted)
            );

            if(first == kNumBits) {
                m_curr = kNumBits;
                return *this;
            }

            m_curr = shift + first;
            return *this;
        }

        BitMask operator++(int)
        {
            BitMask ret = *this;
            ++(*this);
            return ret;
        }

        std::size_t operator*() const
        {
            return m_curr;
        }

        bool operator!=(const BitMask& other) const
        {
            return m_curr != other.m_curr; 
        }
    };

    struct Group
    {
        const Ctrl *m_group_base;

        Group(const Ctrl *group_base)
            : m_group_base{group_base}
        {}

        BitMask<uint32_t> Match(ctrl_t value) const
        {
            uint32_t ret = 0;
            for(int i = 0; i < kGroupSize; i++) {
                if(value == m_group_base[i]) {
                    ret |= (0b1 << (31 - i));
                }
            }
            return {ret};
        }

        BitMask<uint32_t> MatchEmpty() const
        {
            uint32_t ret = 0;
            for(int i = 0; i < kGroupSize; i++) {
                if(Ctrl::eEmpty == m_group_base[i]) {
                    ret |= (0b1 << (31 - i));
                }
            }
            return {ret};
        }

        BitMask<uint32_t> MatchDeleted() const
        {
            uint32_t ret = 0;
            for(int i = 0; i < kGroupSize; i++) {
                if(Ctrl::eDeleted == m_group_base[i]) {
                    ret |= (0b1 << (31 - i));
                }
            }
            return {ret};
        }

        BitMask<uint32_t> MatchEmptyOrDeleted() const
        {
            uint32_t empty = MatchEmpty().m_value;
            uint32_t deleted = MatchDeleted().m_value;
            return {empty | deleted};
        }

        BitMask<uint32_t> MatchNotEmptyOrDeleted() const
        {
            auto flipped = MatchEmptyOrDeleted();
            uint32_t mask = std::exp2(kGroupSize)-1;
            mask <<= (32 - kGroupSize);
            return {(~flipped.m_value) & mask};
        }

        BitMask<uint32_t> MatchEmptyOrDeletedFrom(std::size_t start) const
        {
            auto value = MatchEmptyOrDeleted();
            uint32_t mask = 0;
            if(start > 0) {
                mask = std::exp2(start) - 1;
                mask <<= 32 - start;
            }
            return {value.m_value & ~mask};
        }

        BitMask<uint32_t> MatchNotEmptyOrDeletedFrom(std::size_t start) const
        {
            auto flipped = MatchEmptyOrDeleted();
            uint32_t mask = 0;
            if(start > 0) {
                mask = std::exp2(start) - 1;
                mask <<= 32 - start;
            }
            return {(~flipped.m_value) & ~mask};
        }

        BitMask<uint32_t> MatchNotEmptyOrDeletedUntil(std::size_t end) const
        {
            auto flipped = MatchEmptyOrDeleted();
            uint32_t mask = 0;
            if(end < kGroupSize) {
                mask = std::exp2(kGroupSize - 1 - end) - 1;
                mask <<= (32 - kGroupSize);
                mask |= static_cast<uint32_t>(std::exp2(kGroupSize) - 1);
            }
            return {(~flipped.m_value) & ~mask};
        }
    };

    key_equal                    m_comparator;
    key_allocator_type           m_key_allocator;
    mapped_allocator_type        m_mapped_allocator;
    hasher                       m_hasher;
    size_type                    m_capacity;
    size_type                    m_size;
    size_type                    m_loaded_bins;

    std::unique_ptr<Ctrl[]>                                           m_metadata;
    std::unique_ptr<key_type[], std::function<void(key_type*)>>       m_keys;
    std::unique_ptr<mapped_type[], std::function<void(mapped_type*)>> m_values;
};

/* Template deduction guides
 */

template <std::input_iterator InputIterator>
using IteratorKeyType = std::tuple_element_t<0,
    typename std::iterator_traits<InputIterator>::value_type>;

template <std::input_iterator InputIterator>
using IteratorValueType = std::tuple_element_t<1,
    typename std::iterator_traits<InputIterator>::value_type>;

template <
    std::input_iterator InputIterator,
    typename Hash = std::hash<IteratorKeyType<InputIterator>>,
    typename KeyEqual = std::equal_to<IteratorKeyType<InputIterator>>,
    typename KeyAllocator = std::allocator<IteratorKeyType<InputIterator>>,
    typename MappedAllocator = std::allocator<IteratorValueType<InputIterator>>
>
FlatHashMap(InputIterator, InputIterator, 
    std::size_t = FlatHashMap<IteratorKeyType<InputIterator>, 
                              IteratorValueType<InputIterator>,
                              Hash, KeyEqual, KeyAllocator, MappedAllocator>::kGroupSize, 
    const Hash& = Hash{}, const KeyEqual& = KeyEqual{}, const KeyAllocator& = KeyAllocator{}, 
    const MappedAllocator& = MappedAllocator{})
    ->  FlatHashMap<IteratorKeyType<InputIterator>,
                    IteratorValueType<InputIterator>,
                    Hash, KeyEqual, KeyAllocator, MappedAllocator>;

template <std::ranges::input_range Range>
using RangeKeyType = std::remove_cvref_t<
    decltype(std::get<0>(std::ranges::range_value_t<Range>{}))>;

template <std::ranges::input_range Range>
using RangeValueType = std::remove_cvref_t<
    decltype(std::get<1>(std::ranges::range_value_t<Range>{}))>;

template <
    std::ranges::input_range Range,
    typename Hash = std::hash<RangeKeyType<Range>>,
    typename KeyEqual = std::equal_to<RangeKeyType<Range>>,
    typename KeyAllocator = std::allocator<RangeKeyType<Range>>,
    typename MappedAllocator = std::allocator<RangeValueType<Range>>
>
FlatHashMap(Range&&, 
    std::size_t = FlatHashMap<RangeKeyType<Range>, 
                              RangeValueType<Range>,
                              Hash, KeyEqual, KeyAllocator, MappedAllocator>::kGroupSize, 
    const Hash& = Hash{}, const KeyEqual& = KeyEqual{},
    const KeyAllocator& = KeyAllocator{}, const MappedAllocator& = MappedAllocator{})
    -> FlatHashMap<RangeKeyType<Range>,
                   RangeValueType<Range>,
                   Hash, KeyEqual, KeyAllocator, MappedAllocator>;

template <
    typename K, 
    typename V,
    typename Hash = std::hash<K>,
    typename KeyEqual = std::equal_to<K>,
    typename KeyAllocator = std::allocator<K>,
    typename MappedAllocator = std::allocator<V>
>
FlatHashMap(std::initializer_list<std::pair<K, V>>, 
    std::size_t = FlatHashMap<K, V, Hash, KeyEqual, KeyAllocator, MappedAllocator>::kGroupSize, 
    const Hash& = Hash{}, const KeyEqual& = KeyEqual{},
    const KeyAllocator& = KeyAllocator{}, const MappedAllocator& = MappedAllocator{})
    -> FlatHashMap<K, V,
                   Hash, KeyEqual, KeyAllocator, MappedAllocator>;

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::FlatHashMap(
    size_type min_bucket_count, const Hash& hash, const key_equal& equal,
    const KeyAllocator& key_alloc, const MappedAllocator& mapped_alloc)
    : m_comparator{equal}
    , m_key_allocator{key_alloc}
    , m_mapped_allocator{mapped_alloc}
    , m_hasher{hash}
    , m_capacity{ngroups(min_bucket_count) * kGroupSize}
    , m_size{}
    , m_loaded_bins{}
    , m_metadata{new Ctrl[m_capacity]}
    , m_keys{m_key_allocator.allocate(m_capacity), 
        [this, cap = this->m_capacity](key_type *ptr){m_key_allocator.deallocate(ptr, cap);}}
    , m_values{m_mapped_allocator.allocate(m_capacity),
        [this, cap = this->m_capacity](mapped_type *ptr){m_mapped_allocator.deallocate(ptr, cap);}}
{
    std::fill(m_metadata.get(), m_metadata.get() + m_capacity, Ctrl::eEmpty);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template <std::input_iterator InputIterator>
requires requires (InputIterator it) {
    {std::tuple_size_v<decltype(*it)> == 2};
    requires std::convertible_to<
        std::tuple_element_t<0, std::iter_value_t<std::remove_pointer_t<decltype(it)>>>, Key>;
    requires std::convertible_to<
        std::tuple_element_t<1, std::iter_value_t<std::remove_pointer_t<decltype(it)>>>, T>;
}
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::FlatHashMap(
    InputIterator first, InputIterator last, size_type min_bucket_count,
    const Hash& hash, const key_equal& equal, const KeyAllocator& key_alloc, 
    const MappedAllocator& mapped_alloc)
    : FlatHashMap{min_bucket_count, hash, equal, key_alloc, mapped_alloc}
{
    insert(first, last);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template <std::ranges::input_range Range>
requires requires (std::ranges::range_value_t<Range> value) {
    {std::tuple_size_v<std::ranges::range_value_t<Range>> == 2};
    {std::get<0>(value)} -> std::convertible_to<Key>;
    {std::get<1>(value)} -> std::convertible_to<T>;
}
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::FlatHashMap(
    Range&& range, size_type min_bucket_count, const Hash& hash, const key_equal& equal,
    const KeyAllocator& key_alloc, const MappedAllocator& mapped_alloc)
    : FlatHashMap{min_bucket_count, hash, equal, key_alloc, mapped_alloc}
{
    insert_range(std::forward<Range>(range));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template <typename K, typename V>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::FlatHashMap(
    std::initializer_list<std::pair<K, V>> init, size_type min_bucket_count,
    const Hash& hash, const key_equal& equal, const KeyAllocator& key_alloc,
    const MappedAllocator& mapped_alloc)
    : FlatHashMap{min_bucket_count, hash, equal, key_alloc, mapped_alloc}
{
    insert(std::begin(init), std::end(init));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::FlatHashMap(FlatHashMap&& other)
    : m_comparator{std::move(other.m_comparator)}
    , m_key_allocator{std::move(other.m_key_allocator)}
    , m_mapped_allocator{std::move(other.m_mapped_allocator)}
    , m_hasher{std::move(other.m_hasher)}
    , m_capacity{other.m_capacity}
    , m_size{other.m_size}
    , m_loaded_bins{other.m_loaded_bins}
    , m_metadata{std::move(other.m_metadata)}
    , m_keys{std::move(other.m_keys)}
    , m_values{std::move(other.m_values)}
{
    other.m_size = 0;
    other.m_loaded_bins = 0;
    other.m_capacity = 0;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>& 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::operator=(FlatHashMap&& other)
{
    FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator> tmp{std::move(other)};
    swap(tmp);
    return *this;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::FlatHashMap(
    FlatHashMap const& other)
    : m_comparator{KeyEqual{}}
    , m_key_allocator{KeyAllocator{}}
    , m_mapped_allocator{MappedAllocator{}}
    , m_hasher{Hash{}}
    , m_capacity{other.m_capacity}
    , m_size{other.m_size}
    , m_loaded_bins{other.m_loaded_bins}
    , m_metadata{new Ctrl[other.m_capacity]}
    , m_keys{m_key_allocator.allocate(other.m_capacity),
        [this, cap = other.m_capacity](key_type *ptr){
            m_key_allocator.deallocate(ptr, cap);}
        }
    , m_values{m_mapped_allocator.allocate(other.m_capacity),
        [this, cap = other.m_capacity](mapped_type *ptr){
            m_mapped_allocator.deallocate(ptr, cap);}
        }
{
    std::copy(other.m_metadata.get(), other.m_metadata.get() + other.m_capacity, m_metadata.get());
    std::copy(other.m_keys.get(), other.m_keys.get() + other.m_capacity, m_keys.get());
    std::copy(other.m_values.get(), other.m_values.get() + other.m_capacity, m_values.get());
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>& 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::operator=(
    FlatHashMap const& other)
{
    if(m_capacity != other.m_capacity) {

        decltype(m_metadata) new_metadata{new Ctrl[other.m_capacity]};
        decltype(m_keys) new_keys{m_key_allocator.allocate(other.m_capacity),
            [this, cap = other.m_capacity](key_type *ptr){
                m_key_allocator.deallocate(ptr, cap);}
            };
        decltype(m_values) new_values{m_mapped_allocator.allocate(other.m_capacity),
            [this, cap = other.m_capacity](mapped_type *ptr){
                m_mapped_allocator.deallocate(ptr, cap);}
            };

        m_metadata = std::move(new_metadata);
        m_keys = std::move(new_keys);
        m_values = std::move(new_values);
    }

    m_capacity = other.m_capacity;
    m_size = other.m_size;
    m_loaded_bins = other.m_loaded_bins;

    std::copy(other.m_metadata.get(), other.m_metadata.get() + other.m_capacity, m_metadata.get());
    std::copy(other.m_keys.get(), other.m_keys.get() + other.m_capacity, m_keys.get());
    std::copy(other.m_values.get(), other.m_values.get() + other.m_capacity, m_values.get());
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
std::size_t FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::next_free_bin(
    std::size_t capacity, std::size_t start, const Ctrl *metadata)
{
    size_t num_groups = capacity / kGroupSize;
    std::size_t group = start / kGroupSize;

    while(true) {
        Group g{metadata + group * kGroupSize};
        if(start / kGroupSize == group) {
            auto bits = g.MatchEmptyOrDeletedFrom(start % kGroupSize);
            if(auto idx = bits.FirstSet(); idx != *bits.end())
                return {group * kGroupSize + idx};
        }else{
            auto bits = g.MatchEmptyOrDeleted();
            if(auto idx = bits.FirstSet(); idx != *bits.end())
                return {group * kGroupSize + idx};
        }
        group = (group + 1) % num_groups;
    }
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
std::size_t FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::next_full_bin(
    std::size_t capacity, std::size_t start, const Ctrl *metadata)
{
    size_t num_groups = capacity / kGroupSize;
    std::size_t group = (start + 1) / kGroupSize;

    while(group != num_groups) {
        Group g{metadata + group * kGroupSize};
        if((start + 1) / kGroupSize == group) {
            auto bits = g.MatchNotEmptyOrDeletedFrom((start + 1) % kGroupSize);
            if(auto idx = bits.FirstSet(); idx != *bits.end())
                return {group * kGroupSize + idx};
        }else{
            auto bits = g.MatchNotEmptyOrDeleted();
            if(auto idx = bits.FirstSet(); idx != *bits.end())
                return {group * kGroupSize + idx};
        }
        group++;
    }
    return capacity;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
std::size_t FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::first_full_bin(
    std::size_t capacity, const Ctrl *metadata)
{
    size_t num_groups = capacity / kGroupSize;
    std::size_t group = 0;

    while(group != num_groups) {
        Group g{metadata + group * kGroupSize};
        auto bits = g.MatchNotEmptyOrDeleted();
        if(auto idx = bits.FirstSet(); idx != *bits.end())
            return {group * kGroupSize + idx};
        group++;
    }
    return capacity;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
std::size_t FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::last_full_bin(
    std::size_t capacity, const Ctrl *metadata)
{
    size_t num_groups = capacity / kGroupSize;
    std::size_t group = num_groups - 1;

    while(group != static_cast<std::size_t>(-1)) {
        Group g{metadata + group * kGroupSize};
        auto bits = g.MatchNotEmptyOrDeleted();
        if(auto idx = bits.LastSet(); idx != *bits.end())
            return {group * kGroupSize + idx};
        group--;
    }
    return capacity;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
std::size_t FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::prev_full_bin(
    std::size_t capacity, std::size_t start, const Ctrl *metadata)
{
    if(start == 0) [[unlikely]]
        return capacity;

    std::size_t group = (start - 1) / kGroupSize;
    while(group != static_cast<std::size_t>(-1)) {
        Group g{metadata + group * kGroupSize};
        if((start - 1) / kGroupSize == group) {
            auto bits = g.MatchNotEmptyOrDeletedUntil((start - 1) % kGroupSize);
            if(auto idx = bits.LastSet(); idx != *bits.end())
                return {group * kGroupSize + idx};
        }else{
            auto bits = g.MatchNotEmptyOrDeleted();
            if(auto idx = bits.LastSet(); idx != *bits.end())
                return {group * kGroupSize + idx};
        }
        group--;
    }
    return capacity;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::begin() noexcept
{
    return iterator_at(first_full_bin(m_capacity, m_metadata.get()));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::begin() const noexcept
{
    return const_iterator_at(first_full_bin(m_capacity, m_metadata.get()));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::end() noexcept
{
    return iterator_at(m_capacity);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::end() const noexcept
{
    return const_iterator_at(m_capacity);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::reverse_iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::rbegin() noexcept
{
    return reverse_iterator_at(last_full_bin(m_capacity, m_metadata.get()));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_reverse_iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::rbegin() const noexcept
{
    return const_reverse_iterator_at(last_full_bin(m_capacity, m_metadata.get()));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::reverse_iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::rend() noexcept
{
    return reverse_iterator_at(m_capacity);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_reverse_iterator
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::rend() const noexcept
{
    return const_reverse_iterator_at(m_capacity);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::cbegin() const noexcept
{
    return const_iterator_at(first_full_bin(m_capacity, m_metadata.get()));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_iterator
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::cend() const noexcept
{
    return const_iterator_at(m_capacity);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_reverse_iterator
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::crbegin() const noexcept
{
    return const_reverse_iterator_at(last_full_bin(m_capacity, m_metadata.get()));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_reverse_iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::crend() const noexcept
{
    return const_reverse_iterator_at(m_capacity);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
[[nodiscard]] bool FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::empty() const noexcept
{
    return (m_size == 0);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::size_type 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::size() const noexcept
{
    return m_size;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::size_type 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::max_size() const noexcept
{
    return m_capacity;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::find(
    const key_type& key, std::size_t hash) const
{
    if(m_capacity == 0) [[unlikely]]
        return iterator_at(m_capacity);

    size_t num_groups = m_capacity / kGroupSize;
    size_t group = H1(hash) % num_groups;

    while(true) {
        Group g{m_metadata.get() + group * kGroupSize};
        for(auto i : g.Match(H2(hash))) {
            if(m_comparator(key, m_keys[group * num_groups + i]))
                return iterator_at(group * num_groups + i);
        }
        if(g.MatchEmpty())
            return iterator_at(m_capacity);
        group = (group + 1) % num_groups;
    }
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::mapped_type& 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::operator[](const key_type& x)
{
    std::size_t hash = m_hasher(x);
    auto it = find(x, hash);
    if(it == end()) {
        return *emplace(value_type{x, {}}).first;
    }
    return it->second;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::mapped_type& 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::operator[](key_type&& x)
{
    std::size_t hash = m_hasher(x);
    auto it = find(x, hash);
    if(it == end()) {
        return (*emplace(value_type{std::move(x), {}}).first).second;
    }
    return (*it).second;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class K> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::mapped_type& 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::operator[](K&& x)
{
    std::size_t hash = m_hasher(x);
    auto it = find(x, hash);
    if(it == end()) {
        return (*emplace(value_type{std::forward<K>(x), {}}).first).second;
    }
    return (*it).second;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::mapped_type& 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::at(const key_type& x)
{
    std::size_t hash = m_hasher(x);
    auto it = find(x, hash);
    if(it == end()) {
        throw std::out_of_range{"The container does not have an element with the specified key"};
    }
    return (*it).second;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::mapped_type const& 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::at(const key_type& x) const
{
    std::size_t hash = m_hasher(x);
    auto it = find(x, hash);
    if(it == end()) {
        throw std::out_of_range{"The container does not have an element with the specified key"};
    }
    return (*it).second;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class K> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::mapped_type& 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::at(const K& x)
{
    std::size_t hash = m_hasher(x);
    auto it = find(x, hash);
    if(it == end()) {
        throw std::out_of_range{"The container does not have an element with the specified key"};
    }
    return (*it).second;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class K> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::mapped_type const& 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::at(const K& x) const
{
    std::size_t hash = m_hasher(x);
    auto it = find(x, hash);
    if(it == end()) {
        throw std::out_of_range{"The container does not have an element with the specified key"};
    }
    return (*it).second;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class... Args> 
std::pair<typename FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator, bool> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::emplace(Args&&... args)
{
    std::size_t bin;
    Ctrl ctrl;

    /* 'args' is pair already... */
    if constexpr (sizeof...(args) == 1) {

        auto&& tuple_args = std::forward_as_tuple(std::forward<Args>(args)...);
        auto&& pair = std::get<0>(tuple_args);
        auto&& key = std::get<0>(pair);

        std::size_t hash = m_hasher(key);
        if(auto it = find(key, hash); it != end())
            return {it, false};

        if(((float)(m_loaded_bins + 1) / m_capacity) > kMaxLoadFactor)
            rehash(std::max(m_capacity * 2, kGroupSize));

        bin = next_free_bin(m_capacity, H1(hash) % m_capacity, m_metadata.get());
        ctrl = static_cast<Ctrl>(H2(hash));

        new (&m_keys[bin]) key_type(std::move(key));
        new (&m_values[bin]) mapped_type(std::move(std::get<1>(pair)));

    }else{
        auto&& tuple_args = std::forward_as_tuple(std::forward<Args>(args)...);
        key_type key{std::get<0>(tuple_args)};

        std::size_t hash = m_hasher(key);
        if(auto it = find(key, hash); it != end())
            return {it, false};

        if(((float)(m_loaded_bins + 1) / m_capacity) > kMaxLoadFactor)
            rehash(std::max(m_capacity * 2, kGroupSize));

        bin = next_free_bin(m_capacity, H1(hash) % m_capacity, m_metadata.get());
        ctrl = static_cast<Ctrl>(H2(hash));

        [this, bin](auto&& first, auto&&... rest){
            new (&m_keys[bin]) key_type(std::forward<decltype(first)>(first));
            new (&m_values[bin]) mapped_type(std::forward<decltype(rest)>(rest)...);
        }(std::forward<Args>(args)...);
    }

    if(m_metadata[bin] == Ctrl::eDeleted)
        m_loaded_bins--;

    m_metadata[bin] = ctrl;
    m_size++;
    m_loaded_bins++;
    return {iterator_at(bin), true};
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class... Args>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::emplace_hint(
    const_iterator position, Args&&... args)
{
    if(position == cend()) [[unlikely]] {
        return emplace(std::forward<Args>(args)...).first;
    }

    std::size_t bin = position.m_bin_idx;
    auto bin_state = m_metadata[bin];
    bool empty = (bin_state == Ctrl::eEmpty) || (bin_state == Ctrl::eDeleted);
    if(!empty) [[unlikely]] {
        return emplace(std::forward<Args>(args)...).first;
    }

    /* 'args' is pair already... */
    Ctrl ctrl;
    if constexpr (sizeof...(args) == 1) {

        auto&& tuple_args = std::forward_as_tuple(std::forward<Args>(args)...);
        auto&& pair = std::get<0>(tuple_args);
        auto&& key = std::get<0>(pair);

        std::size_t hash = m_hasher(key);
        if(((float)(m_loaded_bins + 1) / m_capacity) > kMaxLoadFactor)
            rehash(std::max(m_capacity * 2, kGroupSize));

        bin = next_free_bin(m_capacity, H1(hash) % m_capacity, m_metadata.get());
        ctrl = static_cast<Ctrl>(H2(hash));

        new (&m_keys[bin]) key_type(std::move(key));
        new (&m_values[bin]) mapped_type(std::move(std::get<1>(pair)));

    }else{
        auto&& tuple_args = std::forward_as_tuple(std::forward<Args>(args)...);
        key_type key{std::get<0>(tuple_args)};

        std::size_t hash = m_hasher(key);
        if(((float)(m_loaded_bins + 1) / m_capacity) > kMaxLoadFactor)
            rehash(std::max(m_capacity * 2, kGroupSize));

        bin = next_free_bin(m_capacity, H1(hash) % m_capacity, m_metadata.get());
        ctrl = static_cast<Ctrl>(H2(hash));

        [this, bin](auto&& first, auto&&... rest){
            new (&m_keys[bin]) key_type(std::forward<decltype(first)>(first));
            new (&m_values[bin]) mapped_type(std::forward<decltype(rest)>(rest)...);
        }(std::forward<Args>(args)...);
    }

    if(m_metadata[bin] == Ctrl::eDeleted)
        m_loaded_bins--;

    m_metadata[bin] = ctrl;
    m_size++;
    m_loaded_bins++;
    return iterator_at(bin);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class P> 
std::pair<typename FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator, bool> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert(P&& x)
{
    auto&& key = std::get<0>(std::forward<P>(x));
    auto&& value = std::get<1>(std::forward<P>(x));

    std::size_t hash = m_hasher(key);
    if(auto it = find(key, hash); it != end())
        return {it, false};

    if(((float)(m_loaded_bins + 1) / m_capacity) > kMaxLoadFactor)
        rehash(std::max(m_capacity * 2, kGroupSize));

    std::size_t bin = next_free_bin(m_capacity, H1(hash) % m_capacity, m_metadata.get());
    Ctrl ctrl = static_cast<Ctrl>(H2(hash));

    if(m_metadata[bin] == Ctrl::eDeleted)
        m_loaded_bins--;

    m_metadata[bin] = ctrl;
    m_keys[bin] = key;
    m_values[bin] = value;
    m_size++;
    m_loaded_bins++;
    return {iterator_at(bin), true};
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class P> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert(
    const_iterator position, P&& pair)
{
    return emplace_hint(position, std::forward<P>(pair));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<std::input_iterator InputIterator> 
void FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert(
    InputIterator first, InputIterator last)
{
    while(first != last) {
        insert(*(first++));
    }
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<std::ranges::input_range R>
void FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert_range(R&& rg)
{
    if constexpr (std::is_lvalue_reference_v<R>) {
        insert(rg.begin(), rg.end());
    }else{
        insert(std::make_move_iterator(rg.begin()), std::make_move_iterator(rg.end()));
    }
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
bool FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::operator==(
    const FlatHashMap& y) const
{
    if(m_size != y.m_size)
        return false;
    for(const auto& pair : *this) {
        const auto& key = pair.first;
        const auto& value = pair.second;
        if(auto it = y.find(key); it == y.end() || (*it).second != value)
            return false;
    }
    return true;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
void FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::swap(
    FlatHashMap& other) noexcept
{
    std::swap(m_comparator, other.m_comparator);
    std::swap(m_key_allocator, other.m_key_allocator);
    std::swap(m_mapped_allocator, other.m_mapped_allocator);
    std::swap(m_hasher, other.m_hasher);
    std::swap(m_capacity, other.m_capacity);
    std::swap(m_size, other.m_size);
    std::swap(m_loaded_bins, other.m_loaded_bins);
    std::swap(m_metadata, other.m_metadata);
    std::swap(m_keys, other.m_keys);
    std::swap(m_values, other.m_values);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
void FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::clear() noexcept
{
    erase(begin(), end());
    m_loaded_bins = 0;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
void FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::rehash(
    size_type min_bucket_count)
{
    if(m_capacity >= min_bucket_count)
        return;

    std::size_t new_capacity = ngroups(min_bucket_count) * kGroupSize;
    decltype(m_metadata) new_metadata{new Ctrl[new_capacity]};
    std::fill(new_metadata.get(), new_metadata.get() + new_capacity, Ctrl::eEmpty);

    decltype(m_keys) new_keys{m_key_allocator.allocate(new_capacity),
        [this, new_capacity](key_type *ptr){m_key_allocator.deallocate(ptr, new_capacity);}};
    decltype(m_values) new_values{m_mapped_allocator.allocate(new_capacity),
        [this, new_capacity](mapped_type *ptr){m_mapped_allocator.deallocate(ptr, new_capacity);}};

    for(int i = 0; i < m_capacity; i++) {
        Ctrl ctrl = m_metadata[i];
        if(ctrl == Ctrl::eEmpty || ctrl == Ctrl::eDeleted)
            continue;
        std::size_t hash = m_hasher(m_keys[i]);
        std::size_t bin = next_free_bin(m_capacity, H1(hash) % new_capacity, new_metadata.get());

        new_metadata[bin] = ctrl;
        new_keys[bin] = std::move(m_keys[i]);
        new_values[bin] = std::move(m_values[i]);
    }

    m_capacity = new_capacity;
    m_metadata = std::move(new_metadata);
    m_keys = std::move(new_keys);
    m_values = std::move(new_values);
    m_loaded_bins = m_size;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
bool FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::contains(
    const key_type& x) const
{
    std::size_t hash = m_hasher(x);
    return (find(x, hash) != end());
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template <class K>
bool FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::contains(const K& x) const
{
    std::size_t hash = m_hasher(x);
    return (find(x, hash) != end());
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class M>
std::pair<typename FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator, bool> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert_or_assign(
    const key_type& k, M&& obj)
{
    std::size_t hash = m_hasher(k);
    std::size_t bin = H1(hash) % m_capacity;
    bool inserted = !(m_metadata[bin] == H2(hash));
    if(m_metadata[bin] != Ctrl::eEmpty && m_metadata != Ctrl::eDeleted) {
        m_keys[bin].~key_type();
        m_values[bin].~mapped_type();
        m_metadata[bin] = Ctrl::eDeleted;
    }
    return {emplace_hint(iterator_at(bin), k, std::forward<M>(obj)), inserted};
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class M>
std::pair<typename FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator, bool> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert_or_assign(
    key_type&& k, M&& obj)
{
    std::size_t hash = m_hasher(k);
    std::size_t bin = H1(hash) % m_capacity;
    bool inserted = !(m_metadata[bin] == H2(hash));
    m_metadata[bin] = Ctrl::eDeleted;
    return {emplace_hint(const_iterator_at(bin), std::forward<key_type>(k), std::forward<M>(obj)),
        inserted};
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class K, class M>
std::pair<typename FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator, bool> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert_or_assign(
    K&& k, M&& obj)
{
    std::size_t hash = m_hasher(k);
    std::size_t bin = H1(hash) % m_capacity;
    bool inserted = !(m_metadata[bin] == H2(hash));
    m_metadata[bin] = Ctrl::eDeleted;
    return {emplace_hint(iterator_at(bin), std::forward<K>(k), std::forward<M>(obj)), inserted};
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class M>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert_or_assign(
    const_iterator hint, const key_type& k, M&& obj)
{
    std::size_t hash = m_hasher(k);
    std::size_t bin;
    if(H2(hash) == m_metadata[hint] && m_comparator(k, m_keys[hint])) {
        bin = hint;
    }else{
        bin = next_free_bin(m_capacity, hint, m_metadata.get());
    }
    m_metadata[bin] = Ctrl::eDeleted;
    return emplace_hint(const_iterator_at(bin), k, std::forward<M>(obj));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class M>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert_or_assign(
    const_iterator hint, key_type&& k, M&& obj)
{
    std::size_t hash = m_hasher(k);
    std::size_t bin;
    if(H2(hash) == m_metadata[hint] && m_comparator(k, m_keys[hint])) {
        bin = hint;
    }else{
        bin = next_free_bin(m_capacity, hint, m_metadata.get());
    }
    m_metadata[bin] = Ctrl::eDeleted;
    return emplace_hint(const_iterator_at(bin), std::move(k), std::forward<M>(obj));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class K, class M>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::insert_or_assign(
    const_iterator hint, K&& k, M&& obj)
{
    std::size_t hash = m_hasher(k);
    std::size_t bin;
    if(H2(hash) == m_metadata[hint] && m_comparator(k, m_keys[hint])) {
        bin = hint;
    }else{
        bin = next_free_bin(m_capacity, hint, m_metadata.get());
    }
    m_metadata[bin] = Ctrl::eDeleted;
    return emplace_hint(const_iterator_at(bin), std::move(k), std::forward<M>(obj));
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::erase(const_iterator position)
{
    if(position == cend())
        return end();

    std::size_t bin = position.m_bin_idx;
    if(m_metadata[bin] != Ctrl::eEmpty && m_metadata[bin] != Ctrl::eDeleted) {
        m_keys[bin].~key_type();
        m_values[bin].~mapped_type();
        m_metadata[bin] = Ctrl::eDeleted;
        m_size--;
    }
    return iterator_at((++position).m_bin_idx);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::size_type 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::erase(const key_type& x)
{
    std::size_t hash = m_hasher(x);
    std::size_t bin = H1(hash) % m_capacity;
    if(m_metadata[bin] != H2(hash)) {
        auto it = find(x, hash);
        if(it == end())
            return 0;
        bin = (*it).first.m_bin_idx;
    }

    m_keys[bin].~key_type();
    m_values[bin].~mapped_type();
    m_metadata[bin] = Ctrl::eDeleted;
    m_size--;
    return 1;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class K> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::size_type 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::erase(K&& x)
{
    std::size_t hash = m_hasher(x);
    std::size_t bin = H1(hash) % m_capacity;
    if(m_metadata[bin] != H2(hash)) {
        auto it = find(x, hash);
        if(it == end())
            return 0;
        bin = it.m_bin_idx;
    }

    m_keys[bin].~key_type();
    m_values[bin].~mapped_type();
    m_metadata[bin] = Ctrl::eDeleted;
    m_size--;
    return 1;
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::erase(
    const_iterator first, const_iterator last)
{
    while(first != last) {
        erase(first);
        first++;
    }
    if(last == cend())
        return end();
    return iterator_at((++last).m_bin_idx);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::find(const key_type& x)
{
    std::size_t hash = m_hasher(x);
    return find(x, hash);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::find(const key_type& x) const
{
    std::size_t hash = m_hasher(x);
    return find(x, hash);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class K> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::find(const K& x)
{
    std::size_t hash = m_hasher(std::forward<K>(x));
    return find(x, hash);
}

template <CopyableOrMovable Key, CopyableOrMovable T,
    typename Hash, typename KeyEqual, typename KeyAllocator, typename MappedAllocator>
template<class K> 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::const_iterator 
FlatHashMap<Key, T, Hash, KeyEqual, KeyAllocator, MappedAllocator>::find(const K& x) const
{
    std::size_t hash = m_hasher(std::forward<K>(x));
    return find(x, hash);
}

} //namespace pe

