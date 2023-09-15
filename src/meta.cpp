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

}; //namespace pe

