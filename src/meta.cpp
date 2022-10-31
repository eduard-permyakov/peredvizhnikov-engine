export module meta;

import <type_traits>;
import <utility>;
import <tuple>;
import <iostream>;

namespace pe{

/*
 * Like std::integer_sequence, but not necessarily starting 
 * at zero.
 */
export
template<std::size_t... Is> struct seq
{
    typedef seq<Is...> type;
};

export
template<std::size_t Count, std::size_t Begin, std::size_t... Is>
struct make_seq : make_seq<Count-1, Begin+1, Is..., Begin> {};

template<std::size_t Begin, std::size_t... Is>
struct make_seq<0, Begin, Is...> : seq<Is...> {};

/*
 * Helper to extract a specific subrange from a tuple.
 */
export
template<std::size_t... Is, typename Tuple>
auto extract_tuple(seq<Is...>, Tuple& tup) 
{
    return std::forward_as_tuple(std::get<Is>(tup)...);
}

/* 
 * Helper to invoke a function requiring a parameter 
 * pack (T&&...) with a tuple (std::tuple<T&&...>) holding 
 * the arugments.
 */
export
template<typename, typename>
struct forward_args;

export
template<typename F, typename... T>
struct forward_args<F, std::tuple<T...>>
{
    F                m_func;
    std::tuple<T...> m_tuple;

    explicit forward_args(F func, std::tuple<T...> tuple)
        : m_func{func}
        , m_tuple{tuple}
    {}

    template <typename Sequence>
    struct helper {};

    template <std::size_t... Is>
    struct helper<std::index_sequence<Is...>>
    {
        auto operator()(F func, std::tuple<T...> tuple)
        {
            static_assert(sizeof...(Is) == sizeof...(T));
            return func(std::get<Is>(tuple)...);
        }
    };

    auto operator()()
    {
        auto sequence = std::make_index_sequence<sizeof...(T)>();
        return helper<decltype(sequence)>{}(m_func, m_tuple);
    }
};

template<typename F, typename... T>
forward_args(F func, std::tuple<T...>) -> forward_args<F, std::tuple<T...>>;

}; //namespace pe

template <std::size_t... Is>
std::ostream& operator<<(std::ostream& stream, pe::seq<Is...> sequence)
{
    const char *sep = "";
    (((stream << sep << Is), sep = ", "), ...);
    return stream;
}

