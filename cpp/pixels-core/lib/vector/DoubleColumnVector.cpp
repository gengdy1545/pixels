#include "vector/DoubleColumnVector.h"

DoubleColumnVector::DoubleColumnVector(bool encoding = false) : DoubleColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {}

DoubleColumnVector::DoubleColumnVector(int len, bool encoding = false) 
    : ColumnVector(len, encoding), vector(len)
{
    std::fill(vector.begin(), vector.end(), std::bit_cast<long>(NULL_VALUE));
    memoryUsage += static_cast<long>(sizeof(long)) * len;
}

