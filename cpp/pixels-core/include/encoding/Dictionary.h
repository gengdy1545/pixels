#ifndef PIXELS_ENCODING_DICTIONARY_H
#define PIXELS_ENCODING_DICTIONARY_H
#include <string>
#include <memory>
#include "physical/natives/ByteBuffer.h"
class Dictionary {
public:
    class VisitorContext
    {
        public:
        /**
         * Write the key to the given output stream.
         */
        virtual void writeBytes(std::shared_ptr<ByteBuffer> out) = 0;

        /**
         * @return the key's length in bytes
         */
        virtual int getLength() = 0;
    };
    class Visitor {
        public:
        virtual void visit(std::shared_ptr<VisitorContext> context) = 0;
    };
    
public:
    virtual int add(std::string key) = 0;
    virtual int add(const uint8_t* key, int offset, int length) = 0;

    virtual int size() = 0;
    virtual void clear() = 0;
    virtual void visit(std::shared_ptr<Visitor> visitor) = 0;

};

#endif // PIXELS_ENCODING_DICTIONARY_H