//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_LONGDECIMALCOLUMNWRITER_H
#define DUCKDB_LONGDECIMALCOLUMNWRITER_H
#include "writer/BaseColumnWriter.h"
#include "utils/EncodingUtils.h"
class LongDecimalColumnWriter : public BaseColumnWriter {
    private:
    const std::unique_ptr<EncodingUtils> encodingUtils;
    public:
    LongDecimalColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);
};
#endif // DUCKDB_LONGDECIMALCOLUMNWRITER_H
