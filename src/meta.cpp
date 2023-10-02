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

export module meta;

import <type_traits>;
import <utility>;
import <tuple>;
import <iostream>;

namespace pe{

/*****************************************************************************/
/* seq                                                                       */
/*****************************************************************************/
/*
 * Like std::integer_sequence, but not necessarily starting 
 * at zero.
 */
export
template <std::size_t... Is> struct seq
{
    typedef seq<Is...> type;
};

export
template <std::size_t Count, std::size_t Begin, std::size_t... Is>
struct make_seq : make_seq<Count-1, Begin+1, Is..., Begin> {};

export
template <std::size_t Begin, std::size_t... Is>
struct make_seq<0, Begin, Is...> : seq<Is...> {};

export
template <std::size_t... Is>
std::ostream& operator<<(std::ostream& stream, pe::seq<Is...> sequence)
{
    const char *sep = "";
    (((stream << sep << Is), sep = ", "), ...);
    return stream;
}

/*****************************************************************************/
/* extract_tuple                                                             */
/*****************************************************************************/
/*
 * Helper to extract a specific subrange from a tuple.
 */
export
template <std::size_t... Is, typename Tuple>
constexpr auto extract_tuple(seq<Is...>, const Tuple& tup)
{
    return std::forward_as_tuple(std::get<Is>(tup)...);
}

/*****************************************************************************/
/* extract_matching                                                          */
/*****************************************************************************/
/*
 * Helper to extract all elements matching a predicate from a tuple.
 */

export
template <typename Tuple, typename Pred>
consteval auto extract_matching(const Tuple& /* deduction only */, const Pred&& /* deduction only */)
{
    constexpr std::size_t size = std::tuple_size_v<Tuple>;
    if constexpr (size == 0) {
        return std::tuple<>{};
    }else{
        using Head = std::tuple_element_t<0, Tuple>;
        using Rest = decltype(extract_tuple(make_seq<size-1, 1>(), std::declval<Tuple>()));

        constexpr bool match = Pred{}.template operator()<Head>();
        if constexpr (match) {
            return std::tuple_cat(std::declval<std::tuple<Head>>(), 
                extract_matching(std::declval<Rest>(), Pred{}));
        }else{
            return extract_matching(std::declval<Rest>(), Pred{});
        }
    }
}

/*****************************************************************************/
/* contains_type                                                             */
/*****************************************************************************/
/*
 * Helper to query if a tuple contains a particular type.
 */

export
template <typename T, typename Tuple>
struct contains_type;

export
template <typename T, typename... Us>
struct contains_type<T, std::tuple<Us...>> : std::disjunction<std::is_same<T, Us>...> {};

export
template <typename T, typename Tuple>
inline constexpr bool contains_type_v = contains_type<T, Tuple>::value;

/*****************************************************************************/
/* extract_common                                                            */
/*****************************************************************************/
/*
 * Helper to extract all common types from two tuples.
 */

export
template <typename A, typename B>
consteval auto extract_common(const A& /* deduction only */, const B& /* deduction only */)
{
    constexpr std::size_t size = std::tuple_size_v<A>;
    if constexpr (size == 0) {
        return std::tuple<>{};
    }else{
        using Head = std::tuple_element_t<0, A>;
        using Rest = decltype(extract_tuple(make_seq<size-1, 1>(), std::declval<A>()));

        if constexpr (contains_type_v<Head, B>) {
            return std::tuple_cat(std::declval<std::tuple<Head>>(),
                extract_common(std::declval<Rest>(), std::declval<B>()));
        }else{
            return extract_common(std::declval<Rest>(), std::declval<B>());
        }
    }
}

/*****************************************************************************/
/* transform_tuple                                                           */
/*****************************************************************************/
/*
 * Helper to apply a transformation to every element of a tuple.
 */

export
template <typename Tuple, typename F>
consteval auto transform_tuple(const Tuple& /* deduction only */, const F&& /* deduction only */)
{
    constexpr std::size_t size = std::tuple_size_v<Tuple>;
    if constexpr (size == 0) {
        return std::tuple<>{};
    }else{
        using Head = std::tuple_element_t<0, Tuple>;
        using Rest = decltype(extract_tuple(make_seq<size-1, 1>(), std::declval<Tuple>()));
        using TransformedHead = decltype(F{}.template operator()<Head>());

        return std::tuple_cat(std::declval<std::tuple<TransformedHead>>(),
            transform_tuple(std::declval<Rest>(), F{}));
    }
}

/*****************************************************************************/
/* tuple_indexof                                                             */
/*****************************************************************************/
/*
 * Helper to query the index of a type in a tuple.
 */

template <std::size_t I, typename T, typename Tuple>
constexpr std::size_t tuple_indexof_helper()
{
    static_assert(I < std::tuple_size_v<Tuple>, "Element not found");

    if constexpr (std::is_same_v<T, std::tuple_element_t<I, Tuple>>) {
        return I;
    }else{
        return tuple_indexof_helper<I + 1, T, Tuple>();
    }
}

export
template <typename T, typename Tuple>
struct tuple_indexof
{
    static constexpr std::size_t value = tuple_indexof_helper<0, T, Tuple>();
};

export
template <typename T, typename Tuple>
inline constexpr std::size_t tuple_indexof_v = tuple_indexof<T, Tuple>::value;

/*****************************************************************************/
/* function_traits                                                           */
/*****************************************************************************/
/*
 * Helper to query for the return type and argument types of 
 * various different callables: lambdas, functors, and functions.
 */
export
template <typename T>
struct function_traits : public function_traits<decltype(&T::operator())>
{};

export
template <typename T, typename R, typename... Args>
struct function_traits<R(T::*)(Args...)>
{
    using return_type = R;
    using args_type = std::tuple<Args...>;
};

export
template <typename T, typename R, typename... Args>
struct function_traits<R(T::*)(Args...) const>
{
    using return_type = R;
    using args_type = std::tuple<Args...>;
};

export
template <typename T, typename R, typename... Args>
struct function_traits<R(T::*)(Args...) const noexcept>
{
    using return_type = R;
    using args_type = std::tuple<Args...>;
};

export
template <typename R, typename... Args>
struct function_traits<R(*)(Args...)>
{
    using return_type = R;
    using args_type = std::tuple<Args...>;
};

/*****************************************************************************/
/* is_template                                                               */
/*****************************************************************************/
/* 
 *Check if a given type is an instance of a specific template.
 */

export
template <class, template <class...> class>
struct is_template_instance : public std::false_type{};

export
template <class... Args, template <class...> class T>
struct is_template_instance<T<Args...>, T> : public std::true_type{};

export
template <class Class, template <class...> class T>
inline constexpr bool is_template_instance_v = is_template_instance<Class, T>::value;

/*****************************************************************************/
/* constexpr_for                                                             */
/*****************************************************************************/

export
template <std::size_t Start, std::size_t End, std::size_t Inc, typename F>
constexpr inline void constexpr_for(F&& lambda)
{
    if constexpr (Start < End) {
        lambda.template operator()<Start>();
        constexpr_for<Start + Inc, End, Inc>(lambda);
    }
}

/*****************************************************************************/
/* is_unique                                                                 */
/*****************************************************************************/
/* 
 * Check that a variadic typelist has unique types.
 */

export
template <typename...>
struct is_unique : public std::true_type{};

export
template <typename T, typename... Rest>
struct is_unique<T, Rest...> : std::bool_constant<
    (!std::is_same_v<T, Rest> && ...) && is_unique<Rest...>::value
>{};

export
template <typename... Args>
inline constexpr bool is_unique_v = is_unique<Args...>::value;

/*****************************************************************************/
/* contains                                                                  */
/*****************************************************************************/
/* 
 * Check that a variadic typelist contains a particular type.
 */

export
template <typename T, typename... Args>
struct contains 
{
    static constexpr bool value{(std::is_same_v<T, Args> || ...)};
};

export
template <typename T, typename... Args>
inline constexpr bool contains_v = contains<T, Args...>::value;

/*****************************************************************************/
/* constructible_with                                                        */
/*****************************************************************************/

export
template <typename Type, typename Tuple>
struct constructible_with
{
    template <typename... Args>
    static constexpr auto unpack(std::tuple<Args...>&& args)
    {
        return std::declval<std::is_constructible<Type, Args...>>();
    }

    static constexpr bool value = decltype(unpack(std::declval<Tuple>()))::value;
};

export
template <typename T, typename... Args>
inline constexpr bool constructible_with_v = constructible_with<T, Args...>::value;

/*****************************************************************************/
/* COMPILE-TIME MUTABLE LIST                                                 */
/*****************************************************************************/
/* 
 * (Ab)use argument-dependent lookup to create a compile-time
 * mutable list. (i.e. "The Stateful Metaprogramming Hack").
 */

export
template <typename ListTag, unsigned N, typename List>
struct ctl_state_t 
{
    static constexpr unsigned n = N;
    using list = List;
};

template <typename ListTag, unsigned N>
struct ctl_reader 
{
    friend auto ctl_state_func(ctl_reader<ListTag, N>);
};

template <typename ListTag, unsigned N, typename List>
struct ctl_setter 
{
    friend auto ctl_state_func(ctl_reader<ListTag, N>) 
    {
        return List{};
    }

    static constexpr ctl_state_t<ListTag, N, List> state{};
};

export
template <typename ListTag, auto EvalTag, unsigned N = 0>
[[nodiscard]]
consteval auto ctl_get_state() 
{
    if constexpr (N == 0) {
        /* Force template instantiation */
        [[maybe_unused]] ctl_setter<ListTag, 0, std::tuple<>> s;
    }
    constexpr bool counted_past_n = requires(ctl_reader<ListTag, N> r) {
        ctl_state_func(r);
    };
    if constexpr (counted_past_n) {
        return ctl_get_state<ListTag, EvalTag, N + 1>();
    }else {
        constexpr ctl_reader<ListTag, N - 1> r;
        return ctl_state_t<ListTag, N - 1, decltype(ctl_state_func(r))>{};
    }
}

export
template <
    typename ListTag, 
    auto EvalTag = []{}, 
    auto State = ctl_get_state<ListTag, EvalTag>()
>
using ctl_get = typename std::remove_cvref_t<decltype(State)>::list;

template <typename ListTag, typename T, auto EvalTag>
[[nodiscard]] consteval auto ctl_append_impl()
{
    using cur_state = decltype(ctl_get_state<ListTag, EvalTag>());
    using cur_list = typename cur_state::list;
    using new_list = decltype(std::tuple_cat(
        std::declval<cur_list>(), std::declval<std::tuple<T>>()));
    ctl_setter<ListTag, cur_state::n + 1, new_list> s;
    return s.state;
}

export
template <
    typename ListTag, 
    typename T, 
    auto EvalTag = []{}, 
    auto State = ctl_append_impl<ListTag, T, EvalTag>()
> 
inline constexpr auto ctl_append = [] { return State; };

export
template <
    typename ListTag, 
    auto EvalTag = []{},
    auto State = ctl_get_state<ListTag, EvalTag>
>
inline constexpr bool ctl_empty = (State.n == 0);

/*****************************************************************************/
/* BASE CLASS REGISTRY                                                       */
/*****************************************************************************/
/*
 * A CRTP base that allows detecting all registered base classes of a
 * particular derived type. We add a friend function to each base that needs 
 * to be detectable in this manner, and merely considering it during overload
 * resolution instantiates the template that appends the corresponding base
 * class to a compile-time list.
 */

template <typename T>
struct tag
{
    using type = T;
};

template <typename T>
struct BaseRegistry;

template <typename T>
constexpr void adl_register_bases(void*) {} /* A dummy ADL target. */

template <typename D, typename B>
struct BaseInserter : decltype(ctl_append<BaseRegistry<D>, tag<B>>(), tag<void>{})
{};

template <typename T>
struct RegisterBases : decltype(adl_register_bases<T>((T *)nullptr), tag<void>{})
{};

template <typename T>
struct BaseList
{
    using type = std::remove_cvref_t<decltype(
        (void)RegisterBases<T>(), 
        std::declval<ctl_get<BaseRegistry<T>>>()
    )>;
};

export
template <typename T>
using base_list_t = typename BaseList<T>::type;

export
template <typename T>
struct Base
{
    /* When the base list is viewed and adl_register_bases
     * is evaluated, the compiler will try  to instantiate 
     * this template as a candidate for overload resolution.
     * If T is a base of D, then BaseInserter<D, T> will be
     * instantiated, thereby inserting T into the compile-time
     * list of D's registered bases.
     */
    template <
        typename D,
        std::enable_if_t<std::is_base_of_v<T, D>, std::nullptr_t> = nullptr,
        BaseInserter<D, T> = nullptr
    >
    friend constexpr void adl_register_bases(void *) {}
};

}; //namespace pe

