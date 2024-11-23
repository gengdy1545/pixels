#include <list>
#include <unordered_map>
#include "encoding/Dictionary.h"

class KeyBuffer {
public:
    KeyBuffer();
    KeyBuffer(const uint8_t* bytes, int offset, int length);
    static KeyBuffer wrap(const uint8_t* keyContent, int offset, int length);

    bool operator==(const KeyBuffer& other) const;
    bool operator<(const KeyBuffer& other) const;
    int compareTo(const KeyBuffer& other) const;
    size_t hashCode() const;

    const uint8_t* getBytes() const;
    int getOffset() const;
    int getLength() const;

private:
    const uint8_t* bytes;
    int offset;
    int length;
    mutable size_t hashCodeCache = 0;

    static int compareBytes(const uint8_t* a, int aOffset, const uint8_t* b, int bOffset, int len);
};

struct KeyBufferHash {
    size_t operator()(const KeyBuffer& keyBuffer) const {
        return keyBuffer.hashCode();
    }
};

struct KeyBufferEqual {
    bool operator()(const KeyBuffer& lhs, const KeyBuffer& rhs) const {
        return lhs == rhs;
    }
};

class HashTableDictionary : public Dictionary {
public:
    explicit HashTableDictionary(int initialCapacity);

    int add(std::string key) override;
    int add(const uint8_t* key, int offset, int length) override;
    int size() override;
    void clear() override;
    void visit(std::shared_ptr<Visitor> visitor) override;

private:
    static const int NUM_DICTIONARIES = 41;
    std::vector<std::unordered_map<KeyBuffer, int>> dictionaries;
    int originalPosition = 0;

    class VisitorContextImpl : public VisitorContext {
    public:
        void writeBytes(std::shared_ptr<ByteBuffer> out) override;
        int getLength() override;
        void setKey(const uint8_t* key, int offset, int length);

    private:
        const uint8_t* key = nullptr;
        int offset = 0;
        int length = 0;
    };
};