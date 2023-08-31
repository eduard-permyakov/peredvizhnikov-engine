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

export module nmatrix;

import meta;
import nvector;

import <immintrin.h>;
import <cstddef>;
import <concepts>;
import <iostream>;
import <string>;
import <sstream>;
import <exception>;
import <cmath>;

namespace pe{

template <Number T, int Base, int Exp>
inline consteval T pow()
{
    if constexpr (Exp == 0)
        return T{1};
    T ret{1};
    constexpr_for<0, Exp, 1>([&]<std::size_t I>{
        ret *= Base;
    });
    return ret;
}

export
template <std::size_t N, Number T> requires (N > 0)
class alignas(64) Mat
{
private:

    T m_raw[N * N];

    template <std::size_t M = N, Number U = T> requires (M > 0)
    using Row = const U(&)[M];

    using iterator = T*;
    using const_iterator = const T*;

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

    struct MatRowRef
    {
    private:
        T *m_ref;
    public:
        constexpr MatRowRef(T *ref) : m_ref{ref} {}
        constexpr T& operator[](std::size_t index)
        {
            if(!std::is_constant_evaluated()) {
                if (index >= N) [[unlikely]]
                    throw std::out_of_range{"Matrix subscripts out of range"};
            }
            return m_ref[index];
        }
    };
    struct ConstMatRowRef
    {
    private:
        const T *m_ref;
    public:
        constexpr ConstMatRowRef(const T *ref) : m_ref{ref} {}
        constexpr const T& operator[](std::size_t index)
        {
            if(!std::is_constant_evaluated()) {
                if (index >= N) [[unlikely]]
                    throw std::out_of_range{"Matrix subscripts out of range"};
            }
            return m_ref[index];
        }
    };

public:

    constexpr Mat() : m_raw{} {}
    template <Number... Ts>
    constexpr Mat(Row<N, Ts>... rows);

    constexpr Mat(Mat&&) = default;
    constexpr Mat(Mat const&) = default;
    constexpr Mat& operator=(Mat&&) = default;
    constexpr Mat& operator=(Mat const&) = default;
    ~Mat() = default;

    constexpr Mat<N,T>  operator+(const Mat<N,T>& b) const;
    constexpr Mat<N,T>  operator-(const Mat<N,T>& b) const;
    constexpr Mat<N,T>  operator*(const Mat<N,T>& b) const;

    Mat<4,float> simd_mult(const Mat<4,float>& b) const;

    constexpr Mat<N,T>  operator*(T factor) const;
    constexpr Mat<N,T>  operator/(T factor) const;

    Mat<N,T>& operator+=(const Mat<N,T>& b) { *this = *this + b; return *this; }
    Mat<N,T>& operator-=(const Mat<N,T>& b) { *this = *this - b; return *this; }
    Mat<N,T>& operator*=(const Mat<N,T>& b) { *this = *this * b; return *this; }

    Mat<N,T>& operator*=(T factor) { *this = *this * factor; return *this; }
    Mat<N,T>& operator/=(T factor) { *this = *this / factor; return *this; }

    constexpr Vec<N,T> operator*(const Vec<N,T>& b) const;

    constexpr MatRowRef operator[](std::size_t index);
    constexpr ConstMatRowRef operator[](std::size_t index) const;

    template <std::size_t M = N> requires (M == 1)
    constexpr auto Determinant() const;
    template <std::size_t M = N> requires (M == 2)
    constexpr auto Determinant() const;
    template <std::size_t M = N> requires (M >= 3)
    constexpr auto Determinant() const;

    constexpr std::size_t Order() const { return N; }
    constexpr std::size_t Size() const { return N * N; };
    constexpr Mat<N,T>    Inverse() const;
    constexpr Mat<N,T>    Transpose() const;
    constexpr Mat<N,T>    Adjoint() const;
    std::string           PrettyString() const;

    template<std::size_t M = N> requires (N > 1)
    Mat<M-1,T> Minor(std::size_t r, std::size_t c) const;

    template<std::size_t R, std::size_t C>
    requires (N > R) && (N > C)
    constexpr Mat<N-1,T>  Minor() const;

    template<std::size_t M = N> requires (N > 1)
    T Cofactor(std::size_t r, std::size_t c) const;

    template<std::size_t M = N, std::size_t R, std::size_t C>
    requires (N > 1) && (N > R) && (N > C)
    constexpr T Cofactor() const;

    template<std::size_t M = N> requires (N > 1)
    constexpr Mat<N, T> CofactorMatrix() const;

    constexpr static inline Mat<N,T> Identity();
    constexpr static inline Mat<N,T> Zero();
    constexpr static inline Mat<N,T> Translate(Vec<N-1,T> delta);

    template <std::size_t M = N> requires (M == 4)
    constexpr static inline Mat<N,T> RotateX(T radians);

    template <std::size_t M = N> requires (M == 4)
    constexpr static inline Mat<N,T> RotateY(T radians);

    template <std::size_t M = N> requires (M == 4)
    constexpr static inline Mat<N,T> RotateZ(T radians);

    template <std::size_t M = N> requires (M == 4)
    constexpr static inline Mat<N,T> RotateXYZ(Vec<3,T> rotation);

    template <std::size_t M = N> requires (M == 4)
    constexpr static inline Mat<N,T> LookAt(Vec<3,T> eye, Vec<3,T> origin, Vec<3,T> up);

    template <std::size_t M = N> requires (M == 4)
    constexpr static inline Mat<N,T> Perspective(float fov, float aspect_ratio,
        float znear, float zfar);

    template <std::size_t M = N> requires (M == 4)
    constexpr static inline Mat<N,T> Orthographic(Vec<2, T> xextent, Vec<2, T> yextent,
        Vec<2, T> zextent);

    constexpr iterator begin() { return m_raw; }
    constexpr iterator end() { return begin() + Size(); }
    constexpr const_iterator cbegin() const { return m_raw; }
    constexpr const_iterator cend() const { return cbegin() + Size(); }

    constexpr bool operator==(const Mat<N, T>& rhs) const;
    constexpr bool operator!=(const Mat<N, T>& rhs) const;

    template <std::size_t M, typename U>
    friend std::ostream& operator<<(std::ostream&, const Mat<M,U>&);
};

export using Mat2i = Mat<2, int32_t>;
export using Mat2u = Mat<2, uint32_t>;
export using Mat2f = Mat<2, float>;
export using Mat2d = Mat<2, double>;
export using Mat3i = Mat<3, int32_t>;
export using Mat3u = Mat<3, uint32_t>;
export using Mat3f = Mat<3, float>;
export using Mat3d = Mat<3, double>;
export using Mat4i = Mat<4, int32_t>;
export using Mat4u = Mat<4, uint32_t>;
export using Mat4f = Mat<4, float>;
export using Mat4d = Mat<4, double>;

template <std::size_t N, Number T> requires (N > 0)
template <Number... Ts>
constexpr Mat<N,T>::Mat(Row<N, Ts>... rows)
{
    auto curr = begin();
    ((curr = std::copy(std::begin(rows), std::end(rows), curr)), ...);
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::operator+(const Mat<N,T>& b) const
{
    Mat<N,T> ret;
    for(std::size_t i = 0; i < N; i++) {
    for(std::size_t j = 0; j < N; j++) {
        ret[i][j] = m_raw[i * N + j] + b[i][j];
    }}
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::operator-(const Mat<N,T>& b) const
{
    Mat<N,T> ret;
    for(std::size_t i = 0; i < N; i++) {
    for(std::size_t j = 0; j < N; j++) {
        ret[i][j] = m_raw[i * N + j] - b[i][j];
    }}
    return ret;
}

template <>
inline Mat<4,float> Mat<4,float>::simd_mult(const Mat<4,float>& b) const
{
    Mat<4,float> ret;
    for(std::size_t i = 0; i < 4; i++) {
    for(std::size_t k = 0; k < 4; k++) {

        /* The inner loop computes one row of the 
         * matrix at a time using vector instructions
         */
        float cell = m_raw[i * 4 + k];
        const float *in_row = &b.m_raw[k * 4];
        float *out_row = &ret.m_raw[i * 4];

        __m128 vcell = _mm_set1_ps(cell);      // broadcast 'cell' to all 4 values
        __m128 vout = _mm_loadu_ps(out_row);   // load 4 float from 'out_row'
        __m128 vin = _mm_loadu_ps(in_row);     // load 4 float from 'in_row'
        __m128 vtmp = _mm_mul_ps(vcell, vin);  // vcell * vin
        vout = _mm_add_ps(vtmp, vout);         // vout = vcell * vin + vout
        _mm_storeu_ps(out_row, vout);          // write vout to c array
    }}
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::operator*(const Mat<N,T>& b) const
{
    if(!std::is_constant_evaluated()) {
        /* Accelerate floating-point 4x4 matrix multiplication with vector instructions 
         */
        if constexpr((N == 4) && std::is_same_v<float, T>) {
            return simd_mult(b);
        }
    }

    Mat<N,T> ret = Mat<N,T>::Zero();
    for(std::size_t i = 0; i < N; i++) {
    for(std::size_t j = 0; j < N; j++) {
        for(std::size_t k = 0; k < N; k++)
            ret[i][j] += m_raw[i * N + k] * b[k][j];
    }}
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::operator*(T factor) const
{
    Mat<N,T> ret;
    for(std::size_t i = 0; i < N; i++) {
    for(std::size_t j = 0; j < N; j++) {
        ret[i][j] = m_raw[i * N + j] * factor;
    }}
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::operator/(T factor) const
{
    Mat<N,T> ret;
    for(std::size_t i = 0; i < N; i++) {
    for(std::size_t j = 0; j < N; j++) {
        ret[i][j] = m_raw[i * N + j] / factor;
    }}
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Vec<N,T> Mat<N,T>::operator*(const Vec<N,T>& b) const
{
    auto ret = Vec<N,T>::Zero();
    for(std::size_t i = 0; i < N; i++) {
    for(std::size_t j = 0; j < N; j++) {
        ret[i] += m_raw[i * N + j] * b[j];
    }}
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T>::MatRowRef Mat<N,T>::operator[](std::size_t index)
{
    if(!std::is_constant_evaluated()) {
        if (index >= N) [[unlikely]] {
            throw std::out_of_range{"Matrix subscripts out of range"};
        }
    }
    return MatRowRef{m_raw + (index * N)};
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T>::ConstMatRowRef Mat<N,T>::operator[](std::size_t index) const
{
    if(!std::is_constant_evaluated()) {
        if (index >= N) [[unlikely]] {
            throw std::out_of_range{"Matrix subscripts out of range"};
        }
    }
    return ConstMatRowRef{m_raw + (index * N)};
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::Identity()
{
    Mat<N,T> ret{};
    for(std::size_t i = 0; i < N; i++) {
        ret[i][i] = T{1};
    }
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::Zero()
{
    Mat<N,T> ret;
    for(std::size_t i = 0; i < N; i++) {
        ret[i][i] = T{};
    }
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::Inverse() const
{
    T det = Determinant();
    if(!std::is_constant_evaluated()) {
        if(equal(det, T{})) {
            throw std::domain_error{"No inverse exists for this matrix"};
        }
    }
    return Adjoint() / Determinant();
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::Transpose() const
{
    Mat<N,T> ret{};
    constexpr_for<0, N, 1>([&]<std::size_t I>{
    constexpr_for<0, N, 1>([&]<std::size_t J>{
        ret[I][J] = m_raw[J * N + I];
    }); });
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr Mat<N,T> Mat<N,T>::Adjoint() const
{
    return CofactorMatrix().Transpose();
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M == 1)
constexpr auto Mat<N,T>::Determinant() const
{
    return m_raw[0];
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M == 2)
constexpr auto Mat<N,T>::Determinant() const
{
    return m_raw[0 * N + 0] * m_raw[1 * N + 1]
         - m_raw[0 * N + 1] * m_raw[1 * N + 0];
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M >= 3)
constexpr auto Mat<N,T>::Determinant() const
{
    T ret = 0;
    constexpr_for<0, N, 1>([&]<std::size_t I>{
        auto val = m_raw[0 * N + I] * Minor<0, I>().Determinant(); 
        if constexpr (I % 2 == 0) {
            ret += val;
        }else {
            ret -= val;
        }
    });
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (N > 1)
Mat<M-1,T> Mat<N,T>::Minor(std::size_t r, std::size_t c) const
{
    if (r>= N || c >= N) [[unlikely]] {
        throw std::out_of_range{"Matrix subscripts out of range"};
    }

    Mat<M-1, T> ret;
    auto it = ret.begin();
    for(std::size_t i = 0; i < N; i++) {
        if(i == r)
            continue;
        for(std::size_t j = 0; j < N; j++) {
            if(j == c)
                continue;
            *it++ = m_raw[i * N + j];
        }
    }
    assert(it == ret.end());
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t R, std::size_t C>
requires (N > R) && (N > C)
constexpr Mat<N-1,T> Mat<N,T>::Minor() const
{
    Mat<N-1, T> ret;
    std::size_t curr = 0; 
    constexpr_for<0, N, 1>([&]<std::size_t I>{
        if constexpr (I == R)
            return;
        constexpr_for<0, N, 1>([&]<std::size_t J>{
            if constexpr (J == C)
                return;
            ret[curr / (N-1)][curr % (N-1)] = m_raw[I * N + J];
            curr++;
        });
    });
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (N > 1)
T Mat<N,T>::Cofactor(std::size_t r, std::size_t c) const
{
    auto minor = Minor(r, c);
    auto coefficient = std::pow(-1, (r+1) + (c+1));
    return coefficient * minor.Determinant();
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M, std::size_t R, std::size_t C>
requires (N > 1) && (N > R) && (N > C)
constexpr T Mat<N,T>::Cofactor() const
{
    auto minor = Minor<R, C>();
    auto coefficient = pow<T, -1, (R+1) + (C+1)>();
    return coefficient * minor.Determinant();
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (N > 1)
constexpr Mat<N, T> Mat<N,T>::CofactorMatrix() const
{
    Mat<N, T> ret{};
    constexpr_for<0, N, 1>([&]<std::size_t I>{
    constexpr_for<0, N, 1>([&]<std::size_t J>{
        ret[I][J] = Cofactor<M, I, J>();
    }); });
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr bool Mat<N, T>::operator==(const Mat<N, T>& rhs) const
{
    bool ret = true;
    constexpr_for<0, N, 1>([&]<std::size_t I>{
    constexpr_for<0, N, 1>([&]<std::size_t J>{
        if(!equal(this->m_raw[I * N + J], rhs[I][J])) {
            ret = false;
            return;
        }
    }); });
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
constexpr bool Mat<N, T>::operator!=(const Mat<N, T>& rhs) const
{
    return !(*this == rhs);
}

template <std::size_t N, Number T> requires (N > 0)
constexpr inline Mat<N,T> Mat<N, T>::Translate(Vec<N-1,T> delta)
{
    auto ret = Mat<N,T>::Identity();
    constexpr_for<0, N-1, 1>([&]<std::size_t I>{
        ret[I][N-1] = delta[I];
    });
    return ret;
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M == 4)
constexpr inline Mat<N,T> Mat<N,T>::RotateX(T radians)
{
    return {
        {T{1},  T{0},              T{0},              T{0}},
        {T{0},  std::cos(radians), std::sin(radians), T{0}},
        {T{0}, -std::sin(radians), std::cos(radians), T{0}},
        {T{0},  T{0},              T{0},              T{1}}
    };
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M == 4)
constexpr inline Mat<N,T> Mat<N,T>::RotateY(T radians)
{
    return {
        { std::cos(radians), std::sin(radians), T{0}, T{0}},
        {-std::sin(radians), std::cos(radians), T{0}, T{0}},
        { T{0},              T{0},              T{1}, T{0}},
        { T{0},              T{0},              T{0}, T{1}}
    };
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M == 4)
constexpr inline Mat<N,T> Mat<N,T>::RotateZ(T radians)
{
    return {
        { std::cos(radians), T{0}, -std::sin(radians), T{0}},
        { T{0},              T{1},  T{0},              T{0}},
        { std::sin(radians), T{0},  std::cos(radians), T{0}},
        { T{0},              T{0},  T{0},              T{1}}
    };
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M == 4)
constexpr inline Mat<N,T> Mat<N,T>::RotateXYZ(Vec<3,T> rotation)
{
    return RotateX(rotation.x()) * RotateY(rotation.y()) * RotateZ(rotation.z());
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M == 4)
constexpr inline Mat<N,T> Mat<N,T>::LookAt(Vec<3,T> eye, Vec<3,T> origin, Vec<3,T> up)
{
    Vec<3,T> delta = origin - eye;
    if(!std::is_constant_evaluated()) {
        if(delta == Vec<3,T>::Zero()) [[unlikely]]
            throw std::domain_error{"The origin and eye position cannot be congruent!"};
    }

    auto forward = (origin - eye).Normalize();
    auto cross = forward.Cross(up);
    if(!std::is_constant_evaluated()) {
        if(cross == Vec<3,T>::Zero()) [[unlikely]]
            throw std::domain_error{"The up vector must not be colinear with the view direction!"};
    }

    auto right = cross.Normalize();
    auto u = right.Cross(forward);

    return Mat<N,T>{
        { right.x(),     right.y(),    right.z(),   -right.Dot(eye) },
        { u.x(),         u.y(),        u.z(),       -u.Dot(eye)     },
        {-forward.x(),  -forward.y(), -forward.z(), forward.Dot(eye)},
        { T{0},          T{0},         T{0},        T{1}            }
    };
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M == 4)
constexpr inline Mat<N,T> Mat<N,T>::Perspective(float fov, float aspect_ratio,
    float znear, float zfar)
{
    if(!std::is_constant_evaluated()) {
        if(equal(T{aspect_ratio}, T{})) [[unlikely]]
            throw std::domain_error{"Aspect ratio must not be zero."};
    }

    const T tan_half_fov = std::tan(fov / static_cast<T>(2));
    return {
        {static_cast<T>(1) / (aspect_ratio * tan_half_fov), T{0}, T{0}, T{0}      },
        {T{0}, static_cast<T>(1) / tan_half_fov, T{0}, T{0}                       },
        {T{0}, T{0}, T{zfar / (znear - zfar)}, T{-(zfar * znear) / (zfar - znear)}},
        {T{0}, T{0}, T{-1.0}, T{0}                                                }
    };
}

template <std::size_t N, Number T> requires (N > 0)
template <std::size_t M> requires (M == 4)
constexpr inline Mat<N,T> Mat<N,T>::Orthographic(Vec<2, T> xextent, Vec<2, T> yextent,
    Vec<2, T> zextent)
{
    if(!std::is_constant_evaluated()) {
        if(equal(xextent.y() - xextent.x(), T{})
        || equal(yextent.y() - yextent.x(), T{})
        || equal(zextent.y() - zextent.x(), T{})) [[unlikely]]
            throw std::domain_error{"Cannot have zero-length extents!"};
    }

    return Mat<N,T>{
        { static_cast<T>(2) / (xextent.y() - xextent.x()), 
          T{0}, 
          T{0}, 
         -(xextent.y() + xextent.x()) / (xextent.y() - xextent.x())
        },
        {  T{0}, 
           static_cast<T>(2) / (yextent.y() - yextent.x()),
           T{0}, 
          -(yextent.y() + yextent.x()) / (yextent.y() - yextent.x())
        },
        {  T{0}, 
           T{0}, 
          -static_cast<T>(1) / (zextent.y() - zextent.x()), 
          -zextent.x() / (zextent.y() - zextent.x())
        },
        {T{0}, T{0}, T{0}, T{1}}
    };
}

template <std::size_t N, Number T> requires (N > 0)
std::string Mat<N,T>::PrettyString() const
{
    std::stringstream stream{};
    stream << "{" << std::endl;
    for(std::size_t i = 0; i < N; i++) {
        stream << "  " << "{";
        for(std::size_t j = 0; j < N; j++) {
            stream << m_raw[i * N + j];
            if(j + 1 != N)
                stream << ", ";
        }
        stream << "}";
        if(i + 1 != N)
            stream << ", " << std::endl;
    }
    stream << std::endl << "}";
    return stream.str();
}

export
template <std::size_t N, Number T> requires (N > 0)
std::ostream& operator<<(std::ostream& stream, const pe::Mat<N,T>& mat)
{
    stream << mat.PrettyString();
    return stream;
}

} //namespace pe

