#include "vector/ColumnVector.h"
class VectorColumnVector: public ColumnVector {
public:
    // a vector of vectors. todo maybe worth try using long instead for better cpu performance because  some cpus don't handle float computations well
    std::vector<std::vector<double>> vector;
    // dimension of vectors in this column todo enforce this in schema
    int dimension;
};