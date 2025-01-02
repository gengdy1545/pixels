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

// 构造函数
template <typename T>
LockFreeVector<T>::LockFreeVector() {
    storage.resize(32); // 初始化支持 32 层存储桶
    descriptor.store(new Descriptor<T>(0, INITIAL_BUCKET_SIZE));
}

// 析构函数
template <typename T>
LockFreeVector<T>::~LockFreeVector() {
    for (auto& bucket : storage) {
        if (bucket.load()) {
            delete[] bucket.load();
        }
    }
    delete descriptor.load();
}

// 分配新存储桶
template <typename T>
void LockFreeVector<T>::allocBucket(size_t bucket) {
    size_t bucketSize = INITIAL_BUCKET_SIZE * (1 << bucket);
    T* newBucket = new T[bucketSize];
    T* expected = nullptr;
    if (!storage[bucket].compare_exchange_strong(expected, newBucket)) {
        delete[] newBucket; // 如果其他线程已经分配，则释放
    }
}

// 添加元素到末尾
template <typename T>
void LockFreeVector<T>::push_back(const T& elem) {
    while (true) {
        Descriptor<T>* current = descriptor.load();
        size_t index = current->size;
        size_t bucket = __builtin_clz(index + INITIAL_BUCKET_SIZE) - __builtin_clz(INITIAL_BUCKET_SIZE);

        if (!storage[bucket].load()) {
            allocBucket(bucket);
        }

        Descriptor<T>* newDescriptor = new Descriptor<T>(index + 1, current->capacity);
        if (descriptor.compare_exchange_strong(current, newDescriptor)) {
            storage[bucket].load()[index % (INITIAL_BUCKET_SIZE * (1 << bucket))] = elem;
            delete current;
            return;
        }
        delete newDescriptor; // CAS失败时释放新描述符
    }
}

// 移除并返回末尾元素
template <typename T>
T LockFreeVector<T>::pop_back() {
    while (true) {
        Descriptor<T>* current = descriptor.load();
        if (current->size == 0) {
            throw std::underflow_error("Vector is empty");
        }

        Descriptor<T>* newDescriptor = new Descriptor<T>(current->size - 1, current->capacity);
        if (descriptor.compare_exchange_strong(current, newDescriptor)) {
            size_t index = current->size - 1;
            size_t bucket = __builtin_clz(index + INITIAL_BUCKET_SIZE) - __builtin_clz(INITIAL_BUCKET_SIZE);
            T value = storage[bucket].load()[index % (INITIAL_BUCKET_SIZE * (1 << bucket))];
            delete current;
            return value;
        }
        delete newDescriptor; // CAS失败时释放新描述符
    }
}

// 随机访问
template <typename T>
T LockFreeVector<T>::operator[](size_t index) const {
    Descriptor<T>* current = descriptor.load();
    if (index >= current->size) {
        throw std::out_of_range("Index out of range");
    }
    size_t bucket = __builtin_clz(index + INITIAL_BUCKET_SIZE) - __builtin_clz(INITIAL_BUCKET_SIZE);
    return storage[bucket].load()[index % (INITIAL_BUCKET_SIZE * (1 << bucket))];
}

// 返回当前大小
template <typename T>
size_t LockFreeVector<T>::size() const {
    Descriptor<T>* current = descriptor.load();
    return current->size;
}