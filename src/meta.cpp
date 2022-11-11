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


}; //namespace pe

template <std::size_t... Is>
std::ostream& operator<<(std::ostream& stream, pe::seq<Is...> sequence)
{
    const char *sep = "";
    (((stream << sep << Is), sep = ", "), ...);
    return stream;
}

