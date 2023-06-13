export module static_stack;

import assert;

import <array>;
import <optional>;

namespace pe{

export
template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
class StaticStack
{
private:

    std::array<T, Capacity> m_array;
    std::size_t             m_size;
    std::size_t             m_capacity;

public:

    StaticStack();
    StaticStack(std::size_t reduced_capacity);

    template <typename U = T>
    bool Push(U&& value);
    std::optional<T> Pop();
    std::optional<T> Peek();

    std::size_t GetSize();
    std::size_t GetCapacity();
    bool        Empty();
    bool        Full();
};

template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
StaticStack<T, Capacity>::StaticStack()
    : m_array{}
    , m_size{0}
    , m_capacity{Capacity}
{}

template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
StaticStack<T, Capacity>::StaticStack(std::size_t reduced_capacity)
    : m_array{}
    , m_size{0}
    , m_capacity{reduced_capacity}
{
    assert(reduced_capacity <= Capacity);
}

template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
template <typename U>
bool StaticStack<T, Capacity>::Push(U&& value)
{
    if(m_size == m_capacity)
        return false;
    m_array[m_size++] = std::forward<U>(value);
    return true;
}

template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
std::optional<T> StaticStack<T, Capacity>::Pop()
{
    if(m_size == 0)
        return std::nullopt;
    return m_array[--m_size];
}

template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
std::optional<T> StaticStack<T, Capacity>::Peek()
{
    if(m_size == 0)
        return std::nullopt;
    return m_array[m_size - 1];
}

template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
std::size_t StaticStack<T, Capacity>::GetSize()
{
    return m_size;
}

template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
std::size_t StaticStack<T, Capacity>::GetCapacity()
{
    return m_capacity;
}

template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
bool StaticStack<T, Capacity>::Empty()
{
    return (m_size == 0);
}

template <typename T, std::size_t Capacity>
requires (std::is_default_constructible_v<T>)
bool StaticStack<T, Capacity>::Full()
{
    return (m_size == m_capacity);
}

}; //namespace pe

