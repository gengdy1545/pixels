#include "vector/ColumnVector.h"

class LongDecimalColumnVector: public ColumnVector {
public:
    long* vector;
};