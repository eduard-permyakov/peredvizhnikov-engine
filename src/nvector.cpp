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

export module nvector;

import <cstddef>;
import <concepts>;
import <iostream>;
import <initializer_list>;
import <exception>;
import <cmath>;
import <cstddef>;
import <algorithm>;
import <sstream>;

namespace pe{

export
template<std::floating_point T>
constexpr auto abs(const T num) -> T
{
    return (num >= T{}) ? num : -num;
}

constexpr double sqrt_helper(double x, double curr, double prev)
{
    return (curr == prev)
         ? curr
         : sqrt_helper(x, 0.5 * (curr + x / curr), curr);
}

constexpr double sqrt(double x)
{
    return (x >= 0 && x < std::numeric_limits<double>::infinity())
         ? sqrt_helper(x, x, 0)
         : std::numeric_limits<double>::quiet_NaN();
}

export template <typename T>
concept Number = std::integral<T> || std::floating_point<T>;

export
template <std::size_t N, typename T>
requires Number<T> and (N > 0)
class Vec
{
private:

    T m_raw[N];

    template <std::floating_point U = T> 
    static inline constexpr T kEpsilon{1.0/1000.0};

    template <std::integral U = T> 
    static inline constexpr bool equal(const U &a, const U &b)
    {
        return a == b;
    }

    template <std::floating_point U = T> 
    static inline bool constexpr equal(const U &a, const U &b)
    {
        if(std::is_constant_evaluated())
            return abs(b - a) < kEpsilon<T>;
        return std::fabs(b - a) < kEpsilon<T>;
    }

public:

    constexpr Vec() : m_raw{} {}

    constexpr Vec(std::initializer_list<T> list)
    {
        if(!std::is_constant_evaluated()) {
            if(list.size() != N)
                throw std::length_error{"Invalid length of initializer list"};
        }
        std::copy(std::begin(list), std::end(list), std::begin(m_raw));
    }

    Vec(Vec&&) = default;
    Vec(Vec const&) = default;
    Vec& operator=(Vec&&) = default;
    Vec& operator=(Vec const&) = default;
    ~Vec() = default;

    constexpr Vec<N,T> operator+(const Vec<N,T>& b)
    {
        Vec<N,T> ret;
        for(std::size_t i = 0; i < N; i++) {
            ret.m_raw[i] = m_raw[i] + b.m_raw[i];
        }
        return ret;
    }

    constexpr Vec<N,T> operator-(const Vec<N,T>& b)
    {
        Vec<N,T> ret;
        for(std::size_t i = 0; i < N; i++) {
            ret.m_raw[i] = m_raw[i] - b.m_raw[i];
        }
        return ret;
    }

    constexpr Vec<N,T> operator*(T factor)
    {
        Vec<N,T> ret;
        for(std::size_t i = 0; i < N; i++) {
            ret.m_raw[i] = m_raw[i] * factor;
        }
        return ret;
    }

    constexpr Vec<N,T> operator/(T factor)
    {
        Vec<N,T> ret;
        for(std::size_t i = 0; i < N; i++) {
            ret.m_raw[i] = m_raw[i] / factor;
        }
        return ret;
    }

    constexpr Vec<N,T>& operator+=(const Vec<N,T>& b) { *this = *this + b; return *this; }
    constexpr Vec<N,T>& operator-=(const Vec<N,T>& b) { *this = *this - b; return *this; }
    constexpr Vec<N,T>& operator*=(T factor) { *this = *this * factor; return *this; }
    constexpr Vec<N,T>& operator/=(T factor) { *this = *this / factor; return *this; }

    constexpr T Dot(const Vec<N,T>& b)
    {
        T ret{};
        for(std::size_t i = 0; i < N; i++) {
            ret += m_raw[i] * b.m_raw[i];
        }
        return ret;
    }

    constexpr Vec<N,T> Normal()
    {
        Vec<N,T> ret;
        T len = Length();
        for(std::size_t i = 0; i < N; i++) {
            ret.m_raw[i] = m_raw[i] / len;
        }
        return ret;
    }

    constexpr Vec<N,T>& Normalize()
    {
        T len = Length();
        std::for_each(std::begin(m_raw), std::end(m_raw), 
            [len](T& val){ val /= len; }
        );
        return *this;
    }

    constexpr T Length() const
    {
        T accum{};
        for(std::size_t i = 0; i < N; i++) {
            accum += m_raw[i] * m_raw[i];
        }
        if(std::is_constant_evaluated()) {
            return sqrt(accum);
        }
        return std::sqrt(accum); 
    }

    template<std::size_t M = N> requires (M == 3)
    constexpr Vec<N,T> Cross(const Vec<N,T>& b)
    {
        return {
            y() * b.z() - z() * b.y(),
            z() * b.x() - x() * b.z(),
            x() * b.y() - y() * b.x()
        };
    }

    template <std::size_t M = N> requires (M > 0)
    constexpr T x() const { return m_raw[0]; }
    template <std::size_t M = N> requires (M > 1)
    constexpr T y() const { return m_raw[1]; }
    template <std::size_t M = N> requires (M > 2)
    constexpr T z() const { return m_raw[2]; }
    template <std::size_t M = N> requires (M > 3)
    constexpr T w() const { return m_raw[3]; }
    
    template <std::size_t M = N> requires (M > 1)
    constexpr Vec<2,T> xy() const { return Vec<2,T>{x(), y()}; }
    template <std::size_t M = N> requires (M > 2)
    constexpr Vec<3,T> xyz() const { return Vec<3,T>{x(), y(), z()}; }

    constexpr T& operator[](std::size_t index)
    {
        if(!std::is_constant_evaluated()) {
            if (index >= N) [[unlikely]]
                throw std::out_of_range{"Vector subscript out of range"};
        }
        return m_raw[index];
    }

    constexpr T const& operator[](std::size_t index) const
    {
        if(!std::is_constant_evaluated()) {
            if (index >= N) [[unlikely]]
                throw std::out_of_range{"Vector subscript out of range"};
        }
        return m_raw[index];
    }

    constexpr static inline Vec<N, T> Zero()
    {
        Vec<N, T> ret;
        for(std::size_t i = 0; i < N; i++) {
            ret.m_raw[i] = T{};
        }
        return ret;
    }

    constexpr bool operator==(const Vec<N, T>& rhs) const
    {
        for(std::size_t i = 0; i < N; i++) {
            if(!equal(m_raw[i], rhs[i]))
                return false;
        }
        return true;
    }

    constexpr bool operator!=(const Vec<N, T>& rhs) const
    {
        return !(*this == rhs);
    }

    std::string PrettyString() const
    {
        std::stringstream stream{};
        stream << "(";
        for(std::size_t i = 0; i < N; i++) {
            stream << m_raw[i];
            if(i < N-1)
                stream << ", ";
        }
        stream << ")";
        return stream.str();
    }

    friend std::ostream& operator<<(std::ostream& stream, const Vec& vec) 
    {
        return stream << vec.PrettyString();
    }
};

export using Vec2i = Vec<2, int32_t>;
export using Vec2u = Vec<2, uint32_t>;
export using Vec2f = Vec<2, float>;
export using Vec2d = Vec<2, double>;
export using Vec3i = Vec<3, int32_t>;
export using Vec3u = Vec<3, uint32_t>;
export using Vec3f = Vec<3, float>;
export using Vec3d = Vec<3, double>;
export using Vec4i = Vec<4, int32_t>;
export using Vec4u = Vec<4, uint32_t>;
export using Vec4f = Vec<4, float>;
export using Vec4d = Vec<4, double>;

} // namespace pe

