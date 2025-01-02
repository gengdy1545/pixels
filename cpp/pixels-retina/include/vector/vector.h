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
#ifndef PIXELS_RETINA_VECTOR_H
#define PIXELS_RETINA_VECTOR_H
#include <atomic>
#include <vector>
#include <memory>
#include <stdexcept>
#include <cstddef>

// 描述符类定义
template <typename T>
struct Descriptor {
    size_t size;                // 当前数组大小
    size_t capacity;            // 当前容量
    std::atomic<T*> pending;    // 当前写操作指针（可选）
    Descriptor(size_t s, size_t c) : size(s), capacity(c), pending(nullptr) {}
};

// 锁无关向量类定义
template <typename T>
class LockFreeVector {
private:
    std::vector<std::atomic<T*>> storage;       // 两级存储
    std::atomic<Descriptor<T>*> descriptor;    // 描述符
    static const size_t INITIAL_BUCKET_SIZE = 8; // 初始桶大小

    void allocBucket(size_t bucket);          // 分配新存储桶

public:
    LockFreeVector();                         // 构造函数
    ~LockFreeVector();                        // 析构函数

    void push_back(const T& elem);            // 添加元素到末尾
    T pop_back();                             // 移除并返回末尾元素
    T operator[](size_t index) const;         // 随机访问
    size_t size() const;                      // 返回当前大小
};
#endif //PIXELS_RETINA_VECTOR_H
