#include "vector/DoubleColumnVector.h"

DoubleColumnVector::DoubleColumnVector(bool encoding) : DoubleColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {}

DoubleColumnVector::DoubleColumnVector(int len, bool encoding) 
    : ColumnVector(len, encoding), vector(len)
{
    std::fill(vector.begin(), vector.end(), *(reinterpret_cast<const long*>(&NULL_VALUE)));
    memoryUsage += static_cast<long>(sizeof(long)) * len;
}

