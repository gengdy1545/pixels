#ifndef PIXELS_RUNLENBYTEENCODER_H
#define PIXELS_RUNLENBYTEENCODER_H

#include "encoding/Encoder.h"
#include "physical/natives/ByteBuffer.h"

class RunLenByteEncoder : public Encoder
{
private:
    std::shared_ptr<ByteBuffer> output;
    void write(uint8_t value);
    void writeValues();
    void flush();

public:
    RunLenByteEncoder();
    void encode(uint8_t *values, uint8_t *results, int length, int &resultLength);
    void close();

private:
    static const int MIN_REPEAT_SIZE = 3;
    static const int MAX_LITERAL_SIZE = 128;
    static const int MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;

    std::vector<uint8_t> literals{MAX_LITERAL_SIZE};
    int numLiterals{0};
    bool repeat{false};
    int tailRunLength{0};
};
#endif // PIXELS_RUNLENBYTEENCODER_H
