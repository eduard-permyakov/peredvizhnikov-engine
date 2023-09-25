export module ecs;

import flat_hash_map;
import assert;
import logger;
import meta;

import <tuple>;
import <vector>;
import <any>;
import <atomic>;
import <array>;
import <iostream>;
import <array>;

namespace pe{

export using entity_t = uint64_t;
export using component_id_t = uint64_t;

/*****************************************************************************/
/* UNIQUE ID                                                                 */
/*****************************************************************************/
/* 
 * Use ADL friend injection technique (i.e. "stateful
 * metaprogramming") in order to statically generate a 
 * unique ID for every invocation.
 */

template <auto Id>
struct Counter
{
    using tag = Counter;

    struct generator
    {
        friend consteval auto is_defined(tag)
        { return true; }
    };
    friend consteval auto is_defined(tag);

    template<typename Tag = tag, auto = is_defined(Tag{})>
    static consteval auto exists(auto)
    { return true; }

    static consteval auto exists(...)
    { return generator(), false; }
};

template <auto Id = uint64_t{}, auto = []{}>
consteval auto unique_id()
{
    if constexpr (Counter<Id>::exists(Id)) {
        return unique_id<Id + 1>();
    }else {
        return Id;
    }
}

/*****************************************************************************/
/* STRONG TYPEDEF                                                            */
/*****************************************************************************/

export
template <typename T, auto Id = []{}>
class StrongTypedef
{
public:

    template <typename U = T>
    requires (std::is_default_constructible_v<U>)
    StrongTypedef()
    noexcept(std::is_nothrow_copy_constructible_v<T>)
        : m_value()
    {}

    template <typename U = T>
    requires (std::is_copy_constructible_v<U>)
    explicit StrongTypedef(const T& value)
    noexcept(std::is_nothrow_move_constructible_v<T>)
        : m_value{value}
    {}

    template <typename U = T>
    requires (std::is_move_constructible_v<U>)
    explicit StrongTypedef(T&& value)
        : m_value{std::move(value)}
    {}

    explicit operator T&() noexcept
    {
        return m_value;
    }

    explicit operator const T&() const noexcept
    {
        return m_value;
    }

    T& operator=(const T& other) const
    noexcept(std::is_nothrow_assignable_v<T, T>)
    {
        m_value = other;
        return *this;
    }

    T& operator=(const StrongTypedef& other) const
    noexcept(std::is_nothrow_assignable_v<T, T>)
    {
        m_value = other.m_value;
        return *this;
    }

    std::strong_ordering operator<=>(const StrongTypedef& other) const noexcept
    {
        return (m_value <=> other.m_value);
    }

    friend void swap(StrongTypedef& a, StrongTypedef& b) noexcept
    {
        using std::swap;
        swap(static_cast<T&>(a), static_cast<T&>(b));
    }

private:

    T m_value;
};

/*****************************************************************************/
/* BITFIELD                                                                  */
/*****************************************************************************/

template <std::size_t Count>
class Bitfield
{
private:

    static constexpr std::size_t kBitsPerWord = sizeof(uint64_t) * 8;
    static inline constexpr std::size_t kNumWords =
        Count / kBitsPerWord + !!(Count % kBitsPerWord);

    std::array<uint64_t, kNumWords> m_words{};

public:

    constexpr Bitfield() = default;

    template <std::size_t Idx>
    static constexpr Bitfield Bit()
    {
        Bitfield ret{};
        ret.Set<Idx>();
        return ret;
    }

    template <std::size_t Idx> requires (Idx < Count)
    constexpr void Set()
    {
        constexpr std::size_t word = Idx / kBitsPerWord;
        constexpr std::size_t bit = Idx % kBitsPerWord;
        m_words[word] |= (0b1 << bit);
    }

    template <std::size_t Idx> requires (Idx < Count)
    constexpr void Clear()
    {
        constexpr std::size_t word = Idx / kBitsPerWord;
        constexpr std::size_t bit = Idx % kBitsPerWord;
        m_words[word] &= ~(0b1 << bit);
    }

    template <std::size_t Idx> requires (Idx < Count)
    constexpr void Test() const
    {
        constexpr std::size_t word = Idx / kBitsPerWord;
        constexpr std::size_t bit = Idx % kBitsPerWord;
        return m_words[word] & (0b1 << bit);
    }

    constexpr Bitfield operator|(const Bitfield& other) const
    {
        Bitfield ret{};
        for(int i = 0; i < kNumWords; i++) {
            ret.m_words[i] = m_words[i] | other.m_words[i];
        }
        return ret;
    }

    constexpr Bitfield operator&(const Bitfield& other) const
    {
        Bitfield ret{};
        for(int i = 0; i < kNumWords; i++) {
            ret.m_words[i] = m_words[i] & other.m_words[i];
        }
        return ret;
    }

    Bitfield& operator|=(const Bitfield& other)
    {
        *this = (*this | other);
        return *this;
    }

    Bitfield& operator&=(const Bitfield& other)
    {
        *this = (*this & other);
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& stream, const Bitfield& bits)
    {
        for(int i = 0; i < kNumWords; i++) {
        for(int j = 0; j < kBitsPerWord; j++) {
            uint64_t curr_word = bits.m_words[i];
            if(curr_word & (uint64_t(0b1) << j))
                stream << "1";
            else
                stream << "0";
        }}
        return stream;
    }
};

inline constexpr std::size_t kMaxComponents = 64;

using ComponentBitfield = Bitfield<kMaxComponents>;

/*****************************************************************************/
/* WORLD                                                                     */
/*****************************************************************************/

export
struct DefaultWorldTag {};

export
template <typename Tag>
class World;

template <typename T>
concept CWorld = pe::is_template_instance_v<T, World>;

export 
template <typename Derived, CWorld World>
struct Entity;

export
template <typename Tag = DefaultWorldTag>
class World
{
    template <typename Derived, CWorld World>
    friend struct Entity;

    static void Register(entity_t id, ComponentBitfield components);
    static void Unregister(entity_t id);

    static void AddComponents(entity_t id, ComponentBitfield components);
    static void RemoveComponents(entity_t id, ComponentBitfield components);
};

/*****************************************************************************/
/* ENTITY                                                                    */
/*****************************************************************************/

export 
template <typename Derived, CWorld World>
struct Entity
{
    static inline std::atomic_uint64_t s_next_entity_id{0};
    const uint64_t m_id{s_next_entity_id.fetch_add(1, std::memory_order_relaxed)};

    using world_type = World;
    using derived_type = Derived;

    Entity();
    ~Entity();

    template <typename Component>
    Component Get();

    template <typename Component>
    Component Set(Component&& value);

    template <typename Component>
    void AddComponent();

    template <typename Component>
    void RemoveComponent();

    template <typename Component>
    bool HasComponent();
};

/*****************************************************************************/
/* COMPONENT                                                                 */
/*****************************************************************************/

template <typename Component, auto Id = uint64_t{}, auto = []{}>
inline consteval component_id_t ecs_component_id()
{
    if constexpr (Counter<Id>::exists(Id)) {
        return ecs_component_id<Component, Id + 1>();
    }else {
        return Id;
    }
}

template <typename Component>
struct component_traits
{
    static constexpr component_id_t id = ecs_component_id<Component>();
};

template <typename Tuple>
struct component_bitfield
{
    template <typename... Args>
    struct helper;

    template <>
    struct helper<>
    {
        static constexpr auto value = ComponentBitfield{};
    };

    template <typename Head, typename... Tail>
    struct helper<Head, Tail...>
    {
        static constexpr auto bits()
        {
            using tag_type = Head::type;
            using component_type = tag_type::type;
            return ComponentBitfield::Bit<component_traits<component_type>::id>()
                 | helper<Tail...>::value;
        }

        static constexpr auto value = bits();
    };

    template <typename... Args>
    static constexpr auto value(std::tuple<Args...>&& args)
    {
        return helper<Args...>::value;
    }
};

template <typename Tuple>
inline consteval ComponentBitfield ecs_component_mask(Tuple&& tuple)
{
    return component_bitfield<Tuple>::value(std::forward<Tuple>(tuple));
}

/*****************************************************************************/
/* WITH COMPONENT                                                            */
/*****************************************************************************/
/*
 * Mixin base to add components to an entity...
 */

export
template <typename Derived, typename Component>
struct WithComponent : Base<WithComponent<Derived, Component>>
{
    using type = Component;
};

/*****************************************************************************/
/* INHERIT COMPONENTS                                                        */
/*****************************************************************************/
/*
 * Mixin base to inherit all components from another entity type.
 */

template <typename... Args>
struct InheritAll;

template <typename... Args>
struct InheritAll<std::tuple<Args...>> : public Args...
{};

template <typename Derived, typename Tuple>
struct transform_components
{
    using type = decltype(transform_tuple(std::declval<Tuple>(), []<typename T>() constexpr{
        using with_components_type = typename std::remove_cvref_t<T>::type;
        using component_type = typename with_components_type::type;
        return std::declval<WithComponent<Derived, component_type>>();
    }));
};

template <typename Derived, typename Tuple>
using transform_components_t = typename transform_components<Derived, Tuple>::type;

export
template <typename Derived, typename Base>
struct InheritComponents : public Entity<Derived, typename Base::world_type>
                         , public InheritAll<transform_components_t<Derived, base_list_t<Base>>>
{};

/*****************************************************************************/
/* ARCHETYPE                                                                 */
/*****************************************************************************/

template <typename... Components>
struct Archetype
{
    std::tuple<FlatHashMap<entity_t, Components>...> m_colums;
};

struct TypeErasedArchetype
{
    ComponentBitfield                                m_key;
    std::size_t                                      m_num_children;
    std::array<TypeErasedArchetype*, kMaxComponents> m_children;
    std::any                                         m_archetype;
};

/*****************************************************************************/
/* MODULE IMPLEMENTATION                                                     */
/*****************************************************************************/

template <typename Derived, CWorld World>
Entity<Derived, World>::Entity()
{
    using components = base_list_t<derived_type>;
    constexpr auto mask = ecs_component_mask(components{});
    world_type::Register(m_id, mask);
}

template <typename Derived, CWorld World>
Entity<Derived, World>::~Entity()
{
    world_type::Unregister(m_id);
}

template <typename Derived, CWorld World>
template <typename Component>
Component Entity<Derived, World>::Get()
{
}

template <typename Derived, CWorld World>
template <typename Component>
Component Entity<Derived, World>::Set(Component&& value)
{
}

template <typename Derived, CWorld World>
template <typename Component>
void Entity<Derived, World>::AddComponent()
{
}

template <typename Derived, CWorld World>
template <typename Component>
void Entity<Derived, World>::RemoveComponent()
{
}

template <typename Derived, CWorld World>
template <typename Component>
bool Entity<Derived, World>::HasComponent()
{
    return true;
}

template <typename Tag>
void World<Tag>::Register(entity_t id, ComponentBitfield components)
{
}

template <typename Tag>
void World<Tag>::Unregister(entity_t id)
{
}

template <typename Tag>
void World<Tag>::AddComponents(entity_t id, ComponentBitfield components)
{
}

template <typename Tag>
void World<Tag>::RemoveComponents(entity_t id, ComponentBitfield components)
{
}

} // namespace pe

