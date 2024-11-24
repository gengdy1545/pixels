#ifndef DUCKDB_DATECOLUMNWRITER_H
#define DUCKDB_DATECOLUMNWRITER_H

#include "writer/BaseColumnWriter.h"
#include "encoding/RunLenIntEncoder.h"

class DateColumnWriter : public BaseColumnWriter
{
public:
    DateColumnWriter(const TypeDescription& type, const PixelsWriterOption& writerOption);
    virtual void newPixel();
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual pixels::proto::ColumnEncoding getColumnChunkEncoding();
    virtual void close();
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);

private:
    std::unique_ptr<RunLenIntEncoder> encoder;
    std::vector<int32_t> curPixelVector;
    bool runlengthEncoding;
    void writeCurPartTime(std::shared_ptr<DateColumnVector> columnVector, const int32_t *values, int curPartLength, int curPartOffset);
};

#endif // DUCKDB_DATECOLUMNWRITER_H
