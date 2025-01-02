/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#include "vector/vector.h"
#include <iostream>
#include <thread>
#include <cassert>

void test_lock_free_vector() {
    LockFreeVector<int> lf_vector;

    // 基本功能测试
    for (int i = 0; i < 10; ++i) {
        lf_vector.push_back(i);
    }
    for (int i = 0; i < 10; ++i) {
        assert(lf_vector[i] == i);
    }
    for (int i = 9; i >= 0; --i) {
        assert(lf_vector.pop_back() == i);
    }

    // 多线程并发测试
    const int num_threads = 4;
    const int num_elements = 1000;

    auto push_task = [&lf_vector](int start) {
        for (int i = start; i < start + num_elements; ++i) {
            lf_vector.push_back(i);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(push_task, i * num_elements);
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "Final size: " << lf_vector.size() << "\n";
}

int main() {
    test_lock_free_vector();
    std::cout << "All tests passed!\n";
    return 0;
}