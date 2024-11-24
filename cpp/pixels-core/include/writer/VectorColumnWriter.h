//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_VECTORCOLUMNWRITER_H
#define DUCKDB_VECTORCOLUMNWRITER_H
#include "writer/BaseColumnWriter.h"
#include "encoding/RunLenIntEncoder.h"
#include "vector/VectorColumnVector.h"
class VectorColumnWriter : public BaseColumnWriter {
    const std::unique_ptr<EncodingUtils> encodingUtils;
    public:
    VectorColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual bool decideNullsPadding(const PixelsWriterOption& writerOption);
    /**
     * Write a vector column vector to the output stream. The dimension of the vector should be enforced by the schema.
     */
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    private:
     void writeCurPartVec(std::shared_ptr<VectorColumnVector> columnVector, const std::vector<std::vector<double>>& values, 
        int curPartLength, int curPartOffset);
    std::unique_ptr<ByteBuffer> vecToBytes(const std::vector<double>& vec, int dimension);
};
#endif // DUCKDB_VECTORCOLUMNWRITER_H
