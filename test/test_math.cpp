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

import nvector;
import nmatrix;
import logger;
import assert;
import platform;

import <cstdlib>;
import <exception>;
import <array>;
import <memory>;


constexpr std::size_t kNumMatrices = 1'000'000;
constexpr std::size_t kNumLoops = 1'000;

void test_matrix()
{
    /* Test multiply by scalar */
    constexpr pe::Mat4d mmat = pe::Mat4d::Identity();
    constexpr auto doubled = mmat * 2.0;
    constexpr pe::Mat4d danswer{
        {2.0, 0.0, 0.0, 0.0},
        {0.0, 2.0, 0.0, 0.0},
        {0.0, 0.0, 2.0, 0.0},
        {0.0, 0.0, 0.0, 2.0},
    };
    static_assert(doubled == danswer);
    pe::assert(doubled == danswer);

    auto mmat2 = mmat;
    mmat2 *= 2.0;
    pe::assert(doubled == mmat2);

    /* Test determinant */
    constexpr pe::Mat4i dmat{
        { 4,  3,  2,  2},
        { 0,  1, -3,  3},
        { 0, -1,  3,  3},
        { 0,  3,  1,  1}
    };
    constexpr auto det = dmat.Determinant();
    static_assert(det == -240);
    pe::assert(det == -240);

    /* Test cofactor */
    constexpr pe::Mat3i cmat{
        {1, 9, 3},
        {2, 5, 4},
        {3, 7, 8}
    };
    constexpr auto static_cofactor_mat = cmat.CofactorMatrix();
    constexpr pe::Mat3i answer{
        { 12, -4,  -1},
        {-51, -1,  20},
        { 21,  2, -13}
    };
    static_assert(static_cofactor_mat == answer);

    auto dynamic_cofactor_mat = cmat.CofactorMatrix();
    pe::assert(static_cofactor_mat == dynamic_cofactor_mat);
    pe::assert(static_cofactor_mat == answer);

    /* Test adjoint */
    constexpr pe::Mat3i amat{
        { 1, -1,  2},
        { 2,  3,  5},
        { 1,  0,  3}
    };
    constexpr pe::Mat3i answer2{
        { 9,  3, -11},
        {-1,  1,  -1},
        {-3, -1,   5}
    };
    constexpr auto adjoint = amat.Adjoint();
    static_assert(adjoint == answer2);
    pe::assert(adjoint == answer2);

    /* Test inverse */
    constexpr pe::Mat3f imat{
        {1, 2, 3},
        {4, 5, 6},
        {7, 2, 9}
    };
    constexpr pe::Mat3f answer3{
        {-11.0/12, 1.0/3, 1.0/12},
        {-1.0/6, 1.0/3, -1.0/6},
        {3.0/4, -1.0/3, 1.0/12}
    };
    constexpr auto inverse = imat.Inverse();
    static_assert(inverse == answer3);
    pe::assert(inverse == answer3);

    auto product = inverse * imat;
    pe::assert(product == pe::Mat3f::Identity());

    /* matrix-matrix multiplication */
    constexpr pe::Mat4f a{
        { 1, 2, 3 ,4},
        { 0, 0, 5, 6},
        { 0, 2, 1, 3},
        { 0, 0, 0, 0}
    };
    constexpr pe::Mat4f b{
        { 0, 1, 1, 1},
        { 0, 0, 2, 2},
        { 0, 0, 0, 3},
        { 0, 0, 0, 0}
    };
    constexpr pe::Mat4f mult_answer{
        { 0, 1, 5, 14},
        { 0, 0, 0, 15},
        { 0, 0, 4,  7},
        { 0, 0, 0,  0}
    };
    constexpr auto mult_result = a * b;
    static_assert(mult_answer == mult_result);
    pe::assert(mult_answer == mult_result);

    auto dynamic_mult_result = a * b;
    pe::assert(mult_answer == dynamic_mult_result);

    /* matrix-vector multiplication */
    constexpr pe::Mat3i mult_mat{
        {1, 2, 3},
        {4, 5, 6},
        {7, 8, 9}
    };
    constexpr pe::Vec3i mult_vec{2, 1, 3};
    constexpr pe::Vec3i vec_mult_answer{13, 31, 49};
    constexpr auto vec_mult_result = mult_mat * mult_vec;
    static_assert(vec_mult_answer == vec_mult_result);
    pe::assert(vec_mult_result == (mult_mat * mult_vec));

    /* Ensure all methods are constexpr-capable */
    [[maybe_unused]] constexpr auto look_at = 
        pe::Mat4d::LookAt({1, 1, 0}, {0, 0, 0}, {0, 1, 0});
    [[maybe_unused]] constexpr auto ortho = 
        pe::Mat4d::Orthographic({-100, 100}, {-100, 100}, {-100, 100});
    [[maybe_unused]] constexpr auto translate =
        pe::Mat4d::Translate({1.0, 2.0, 3.0});

    /* would require a constexpr sin/cos/tan implementation */
    [[maybe_unused]] auto perspective = 
        pe::Mat4d::Perspective(M_PI/4.0f, 4.0/3.0, 0.1f, 10.0f);
    [[maybe_unused]] auto rot =
        pe::Mat4d::RotateXYZ({0.5, 1.5, 3.0});
}

__attribute__((optnone))
void matrix_benchmark()
{
    auto a = std::make_unique_for_overwrite<pe::Mat4f[]>(kNumMatrices);
    auto b = std::make_unique_for_overwrite<pe::Mat4f[]>(kNumMatrices);
    auto c = std::make_unique_for_overwrite<pe::Mat4f[]>(kNumMatrices);

    for(int i = 0; i < kNumLoops; i++) {
    for(int j = 0; j < kNumMatrices; j++) {
        c[j] = a[j] * b[j];
    }}
}

int main()
{
    int ret = EXIT_SUCCESS;
    try{

        pe::ioprint(pe::TextColor::eGreen, "Starting Matrix test.");
        test_matrix();
        pe::dbgtime<true>([&](){
            matrix_benchmark();
        }, [&](uint64_t delta) {
            float seconds = delta / 1'000'000.0f;
            std::size_t total_mults = kNumMatrices * kNumLoops;
            pe::dbgprint("Matrix multiplication test with", total_mults, "multiplications",
                "took", pe::rdtsc_usec(delta), "microseconds. (", pe::fmt::cat{},
                static_cast<int>(total_mults / seconds), "multiplications per second)");
        });
        pe::ioprint(pe::TextColor::eGreen, "Finished Matrix test.");

    }catch(std::exception &e){

        pe::ioprint(pe::LogLevel::eError, "Unhandled std::exception:", e.what());
        ret = EXIT_FAILURE;

    }catch(...){

        pe::ioprint(pe::LogLevel::eError, "Unknown unhandled exception.");
        ret = EXIT_FAILURE;
    }
    return ret;
}

