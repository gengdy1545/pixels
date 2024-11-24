//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_DECIMALCOLUMNWRITER_H
#define DUCKDB_DECIMALCOLUMNWRITER_H

#include "writer/BaseColumnWriter.h"
#include "utils/EncodingUtils.h"

class DecimalColumnWriter : public BaseColumnWriter
{
private:
    const std::unique_ptr<EncodingUtils> encodingUtils;

public:
    DecimalColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);
};
#endif // DUCKDB_DECIMALCOLUMNWRITER_H
