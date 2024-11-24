//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_DOUBLECOLUMNWRITER_H
#define DUCKDB_DOUBLECOLUMNWRITER_H
#include "writer/BaseColumnWriter.h"
#include "utils/EncodingUtils.h"
class DoubleColumnWriter : public BaseColumnWriter
{
private:
    const std::unique_ptr<EncodingUtils> encodingUtils;

public:
    DoubleColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);
};
#endif // DUCKDB_DOUBLECOLUMNWRITER_H
