//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_TIMECOLUMNWRITER_H
#define DUCKDB_TIMECOLUMNWRITER_H

#include "writer/BaseColumnWriter.h"
#include "utils/EncodingUtils.h"
#include "encoding/RunLenIntEncoder.h"
#include "vector/TimeColumnVector.h"
class TimeColumnWriter : public BaseColumnWriter
{
private:
    const std::unique_ptr<EncodingUtils> encodingUtils;
    std::vector<int> curPixelVector;
    const bool runlengthEncoding;
    std::unique_ptr<RunLenIntEncoder> encoder;

public:
    TimeColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);
    virtual void close();
    virtual void newPixel();
    virtual pixels::proto::ColumnEncoding getColumnChunkEncoding() const;

private:
    void writeCurPartTime(std::shared_ptr<TimeColumnVector> columnVector, int *values, int curPartLength, int curPartOffset);
};
#endif // DUCKDB_TIMECOLUMNWRITER_H
