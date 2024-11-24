#ifndef PIXELS_DOUBLECOLUMNVECTOR_H
#define PIXELS_DOUBLECOLUMNVECTOR_H

#include <vector>
#include <string>
#include <memory>
#include <cmath>
#include <cstring>
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include <limits>

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

class DoubleColumnVector : public ColumnVector
{
public:
    std::vector<long> vector;
    static const double NULL_VALUE = std::numeric_limits<double>::quiet_NaN() ;
    // static constexpr int DEFAULT_SIZE = 1024; 

    DoubleColumnVector(bool encoding = false);       
    DoubleColumnVector(int len, bool encoding = false);
    // TODO
private:
    
    bool noNulls;
    bool isRepeating;
};

#endif // PIXELS_DECIMALCOLUMNVECTOR_H
