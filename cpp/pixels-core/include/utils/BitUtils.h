#ifndef PIXELS_BITUTILS_H
#define PIXELS_BITUTILS_H
#include <vector>
#include <cstdint>
#include "ByteOrder.h"
class BitUtils {
private:
    BitUtils() {};
    static std::vector<uint8_t> bitWiseCompactLE(bool* values, int length);
    static std::vector<uint8_t> bitWiseCompactBE(bool* values, int length); 
public:
    static std::vector<uint8_t> bitWiseCompact(bool* values, int length, ByteOrder byteOrder);

};

#endif // PIXELS_BITUTILS_H