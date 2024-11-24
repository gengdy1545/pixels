//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_FLOATCOLUMNWRITER_H
#define DUCKDB_FLOATCOLUMNWRITER_H

#include "writer/BaseColumnWriter.h"
#include "utils/EncodingUtils.h"
class FloatColumnWriter : public BaseColumnWriter
{
private:
    const std::unique_ptr<EncodingUtils> encodingUtils;

public:
    FloatColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
};
#endif // DUCKDB_FLOATCOLUMNWRITER_H
