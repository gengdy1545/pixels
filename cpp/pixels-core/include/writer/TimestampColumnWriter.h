//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_TIMESTAMPCOLUMNWRITER_H
#define DUCKDB_TIMESTAMPCOLUMNWRITER_H
#include "writer/BaseColumnWriter.h"
#include "utils/EncodingUtils.h"
#include "vector/TimestampColumnVector.h"
#include "encoding/RunLenIntEncoder.h"
class TimestampColumnWriter : public BaseColumnWriter
{
private:
    std::vector<long> curPixelVector;
    const std::unique_ptr<EncodingUtils> encodingUtils;
    const bool runlengthEncoding;
    std::unique_ptr<RunLenIntEncoder> encoder;
public:
    TimestampColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual void newPixel();
    virtual pixels::proto::ColumnEncoding getColumnChunkEncoding() const;
    virtual void close();
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);
private:
    void writeCurPartTimestamp(std::shared_ptr<TimestampColumnVector> columnVector, long *values, int curPartLength, int curPartOffset);
};
#endif // DUCKDB_TIMESTAMPCOLUMNWRITER_H
