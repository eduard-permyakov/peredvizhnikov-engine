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

import <functional>;
import <utility>;
import <iterator>;
import <ranges>;
import <compare>;
import <initializer_list>;
import <memory>;

namespace pe{

/*
 * Implementation of a data structure similar to C++23's std::flat_map, 
 * based on the design of Google's absl::flat_hash_map from the talk 
 * "Designing a Fast, Efficient, Cache-friendly Hash Table, Step by Step." 
 * It allows efficient iteration of all elements due to them being stored 
 * in a single flat array. The container invalidates all iterators when
 * rehashing.
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
    using iterator               = std::size_t;
    using const_iterator         = std::size_t;
    using reverse_iterator       = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    using ctrl_t                 = uint8_t;

    static inline constexpr size_type kGroupSize = 16;

    FlatHashMap() = default;
    FlatHashMap(size_type min_bucket_count);

    FlatHashMap(size_type min_bucket_count, 
        const Hash& hash = Hash{}, const key_equal& = KeyEqual{},
        const KeyAllocator& key_alloc = KeyAllocator{},
        const MappedAllocator& mapped_alloc = MappedAllocator{});

    template <std::input_iterator InputIterator>
    requires requires (InputIterator it) {
        {std::tuple_size<decltype(*it)>()} -> std::same_as<std::integral_constant<std::size_t, 2>>;
        {std::get<0>(*it)} -> std::convertible_to<key_type>;
        {std::get<1>(*it)} -> std::convertible_to<value_type>;
    }
    FlatHashMap(InputIterator first, InputIterator last, size_type min_bucket_count = kGroupSize,
        const Hash& hash = Hash{}, const key_equal& equal = KeyEqual{},
        const KeyAllocator& key_alloc = KeyAllocator{}, 
        const MappedAllocator& mapped_alloc = MappedAllocator{});

    template <std::ranges::input_range Range>
    requires requires (std::ranges::range_value_t<Range> value) {
        {std::tuple_size<decltype(value)>()} -> std::same_as<std::integral_constant<std::size_t, 2>>;
        {std::get<0>(value)} -> std::convertible_to<key_type>;
        {std::get<1>(value)} -> std::convertible_to<value_type>;
    }
    FlatHashMap(Range&& range, size_type min_bucket_count = kGroupSize,
        const Hash& hash = Hash{}, const key_equal& equal = KeyEqual{},
        const KeyAllocator& key_alloc = KeyAllocator{},
        const MappedAllocator& mapped_alloc = MappedAllocator{});

    template <typename Input>
    FlatHashMap(std::initializer_list<Input> init, size_type min_bucket_count = kGroupSize,
        const Hash& hash = Hash{}, const key_equal& equal = KeyEqual{},
        const KeyAllocator& key_alloc = KeyAllocator{},
        const MappedAllocator& mapped_alloc = MappedAllocator{})
        : FlatHashMap{std::begin(init), std::end(init), min_bucket_count, hash, equal,
            key_alloc, mapped_alloc}
    {}

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

    template<class... Args>
    std::pair<iterator, bool> try_emplace(const key_type& k, Args&&... args);
    template<class... Args>
    std::pair<iterator, bool> try_emplace(key_type&& k, Args&&... args);
    template<class K, class... Args>
    std::pair<iterator, bool> try_emplace(K&& k, Args&&... args);
    template<class... Args>
    iterator try_emplace(const_iterator hint, const key_type& k, Args&&... args);
    template<class... Args>
    iterator try_emplace(const_iterator hint, key_type&& k, Args&&... args);
    template<class K, class... Args>
    iterator try_emplace(const_iterator hint, K&& k, Args&&... args);

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
 
    size_type count(const key_type& x) const;
    template<class K> size_type count(const K& x) const;
 
    bool contains(const key_type& x) const;
    template<class K> bool contains(const K& x) const;
 
    iterator lower_bound(const key_type& x);
    const_iterator lower_bound(const key_type& x) const;
    template<class K> iterator lower_bound(const K& x);
    template<class K> const_iterator lower_bound(const K& x) const;
 
    iterator upper_bound(const key_type& x);
    const_iterator upper_bound(const key_type& x) const;
    template<class K> iterator upper_bound(const K& x);
    template<class K> const_iterator upper_bound(const K& x) const;
 
    std::pair<iterator, iterator> equal_range(const key_type& x);
    std::pair<const_iterator, const_iterator> equal_range(const key_type& x) const;
    template<class K> std::pair<iterator, iterator> equal_range(const K& x);
    template<class K> std::pair<const_iterator, const_iterator> equal_range(const K& x) const;
 
    friend bool operator==(const FlatHashMap& x, const FlatHashMap& y);

    friend std::strong_ordering
    operator<=>(const FlatHashMap& x, const FlatHashMap& y);
 
    friend void swap(FlatHashMap& x, FlatHashMap& y) noexcept
    { x.swap(y); }

private:

    std::size_t H1(std::size_t hash) { return (hash >> 7);   }
    ctrl_t      H2(std::size_t hash) { return (hash & 0x7f); }

    enum Ctrl : ctrl_t
    {
        eEmpty = -128,  // 0b10000000
        eDeleted = -2,  // 0b11111110
        eSentinel = -1, // 0b11111111
        // Full         // 0b0xxxxxxx
    };

    key_equal                    m_comparator;
    key_allocator_type           m_key_allocator;
    mapped_allocator_type        m_mapped_allocator;
    hasher                       m_hasher;
    size_type                    m_size;

    std::unique_ptr<Ctrl>        m_metadata;
    std::unique_ptr<key_type>    m_keys;
    std::unique_ptr<mapped_type> m_values;
};

/* Template deduction guides
 */

template <std::input_iterator InputIterator>
requires requires { std::get<0>(std::iterator_traits<InputIterator>::value_type); }
using IteratorKeyType = decltype(std::get<0>(std::iterator_traits<InputIterator>::value_type));

template <std::input_iterator InputIterator>
requires requires { std::get<1>(std::iterator_traits<InputIterator>::value_type); }
using IteratorValueType = decltype(std::get<1>(std::iterator_traits<InputIterator>::value_type));

template <
    std::input_iterator InputIterator,
    typename Hash = std::hash<IteratorKeyType<InputIterator>>,
    typename KeyEqual = std::equal_to<IteratorKeyType<InputIterator>>,
    typename KeyAllocator = std::allocator<IteratorKeyType<InputIterator>>,
    typename MappedAllocator = std::allocator<IteratorValueType<InputIterator>>
>
FlatHashMap(InputIterator, InputIterator, std::size_t, const Hash&, const KeyEqual&,
    const KeyAllocator&, const MappedAllocator&)
    ->  FlatHashMap<IteratorKeyType<InputIterator>,
                    IteratorValueType<InputIterator>,
                    Hash, KeyEqual, KeyAllocator, MappedAllocator>;

template <std::ranges::input_range Range>
requires requires { std::get<0>(std::ranges::range_value_t<Range>{}); }
using RangeKeyType = decltype(std::get<0>(std::ranges::range_value_t<Range>{}));

template <std::ranges::input_range Range>
requires requires { std::get<1>(std::ranges::range_value_t<Range>{}); }
using RangeValueType = decltype(std::get<1>(std::ranges::range_value_t<Range>{}));

template <
    std::ranges::input_range Range,
    typename Hash = std::hash<RangeKeyType<Range>>,
    typename KeyEqual = std::equal_to<RangeKeyType<Range>>,
    typename KeyAllocator = std::allocator<RangeKeyType<Range>>,
    typename MappedAllocator = std::allocator<RangeValueType<Range>>
>
FlatHashMap(Range&&, std::size_t, const Hash&, const KeyEqual& equal,
    const KeyAllocator&, const MappedAllocator&)
    -> FlatHashMap<RangeKeyType<Range>,
                   RangeValueType<Range>,
                   Hash, KeyEqual, KeyAllocator, MappedAllocator>;

template <typename Input>
requires requires { std::get<0>(Input{}); }
using InputKeyType = decltype(std::get<0>(Input{}));

template <typename Input>
requires requires { std::get<1>(Input{}); }
using InputValueType = decltype(std::get<1>(Input{}));

template <
    typename Input,
    typename Hash = std::hash<InputKeyType<Input>>,
    typename KeyEqual = std::equal_to<InputKeyType<Input>>,
    typename KeyAllocator = std::allocator<InputKeyType<Input>>,
    typename MappedAllocator = std::allocator<InputValueType<Input>>
>
FlatHashMap(std::initializer_list<Input>, std::size_t, const Hash&, const KeyEqual&,
    const KeyAllocator&, const MappedAllocator&)
    -> FlatHashMap<InputKeyType<Input>,
                   InputValueType<Input>,
                   Hash, KeyEqual, KeyAllocator, MappedAllocator>;

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

} //namespace pe

