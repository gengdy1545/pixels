#include <sstream>
#include "encoding/HashTableDictionary.h"

// ====== KeyBuffer Implementation ======

KeyBuffer::KeyBuffer(const uint8_t* bytes, int offset, int length)
    : bytes(bytes), offset(offset), length(length) {}
KeyBuffer::KeyBuffer() : bytes(nullptr), offset(0), length(0), hashCodeCache(0) {}
KeyBuffer KeyBuffer::wrap(const uint8_t* keyContent, int offset, int length) {
    return KeyBuffer(keyContent, offset, length);
}

bool KeyBuffer::operator==(const KeyBuffer& other) const {
    if (this->length != other.length) return false;
    return std::memcmp(this->bytes + this->offset, other.bytes + other.offset, this->length) == 0;
}

bool KeyBuffer::operator<(const KeyBuffer& other) const {
    return compareTo(other) < 0;
}

int KeyBuffer::compareTo(const KeyBuffer& other) const {
    int compareLen = std::min(this->length, other.length);
    int cmp = std::memcmp(this->bytes + this->offset, other.bytes + other.offset, compareLen);
    if (cmp != 0) return cmp;
    return this->length - other.length;
}

size_t KeyBuffer::hashCode() const {
    if (hashCodeCache == 0) {
        size_t hash = 31 + std::hash<int>()(length);
        for (int i = 0; i < length; ++i) {
            hash = 31 * hash + bytes[offset + i];
        }
        hashCodeCache = hash;
    }
    return hashCodeCache;
}

const uint8_t* KeyBuffer::getBytes() const {
    return bytes;
}

int KeyBuffer::getOffset() const {
    return offset;
}

int KeyBuffer::getLength() const {
    return length;
}

// ====== HashTableDictionary Implementation ======

HashTableDictionary::HashTableDictionary(int initialCapacity) {
    int capacity = initialCapacity / NUM_DICTIONARIES;
    if (initialCapacity % NUM_DICTIONARIES > 0) {
        capacity++;
    }
    dictionaries.resize(NUM_DICTIONARIES);
}

int HashTableDictionary::add(std::string key) {
    const uint8_t* byteArray = reinterpret_cast<const uint8_t*>(key.data());
    return add(byteArray, 0, key.size());
}

int HashTableDictionary::add(const uint8_t* key, int offset, int length) {
    KeyBuffer keyBuffer = KeyBuffer::wrap(key, offset, length);
    int dictId = keyBuffer.hashCode() % NUM_DICTIONARIES;
    if (dictId < 0) dictId = -dictId;
    auto& dict = dictionaries[dictId];
    auto it = dict.find(keyBuffer);
    if (it != dict.end()) {
        return it->second;
    }
    dict[keyBuffer] = originalPosition;
    return originalPosition++;
}

int HashTableDictionary::size() {
    return originalPosition;
}

void HashTableDictionary::clear() {
    for (auto& dict : dictionaries) {
        dict.clear();
    }
    originalPosition = 0;
}

void HashTableDictionary::visit(std::shared_ptr<Visitor> visitor) {
    std::vector<std::unordered_map<KeyBuffer, int>::iterator> iterators;
    for (auto& dict : dictionaries) {
        iterators.push_back(dict.begin());
    }

    for (int position = 0; position < originalPosition; ++position) {
        bool keyIsFound = false;
        KeyBuffer keyBuffer;
        for (size_t i = 0; i < dictionaries.size(); ++i) {
            auto& dict = dictionaries[i];
            auto& it = iterators[i];
            if (it != dict.end() && it->second == position) {
                keyBuffer = it->first;
                ++it;
                keyIsFound = true;
                break;
            }
        }
        if (!keyIsFound) {
            std::ostringstream oss;
            oss << "Key position " << position << " not found, dictionary is corrupt";
            throw std::runtime_error(oss.str());
        }
        auto context = std::make_shared<VisitorContextImpl>();
        context->setKey(keyBuffer.getBytes(), keyBuffer.getOffset(), keyBuffer.getLength());
        visitor->visit(context);
    }
}

// ====== VisitorContextImpl Implementation ======

void HashTableDictionary::VisitorContextImpl::writeBytes(std::shared_ptr<ByteBuffer> out) {
    out->putBytes(const_cast<uint8_t*>(key + offset), length);
}

int HashTableDictionary::VisitorContextImpl::getLength() {
    return length;
}

void HashTableDictionary::VisitorContextImpl::setKey(const uint8_t* key, int offset, int length) {
    this->key = key;
    this->offset = offset;
    this->length = length;
}