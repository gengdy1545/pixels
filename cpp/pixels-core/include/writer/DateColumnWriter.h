#ifndef DUCKDB_DATECOLUMNWRITER_H
#define DUCKDB_DATECOLUMNWRITER_H

#include "writer/BaseColumnWriter.h"


class DateColumnWriter : public BaseColumnWriter {
public:
    DateColumnWriter(TypeDescription type, const PixelsWriterOption &writerOption);
    virtual int write(ColumnVector *vector, int size);
    virtual void newPixel();

    
    // StatsRecorder getColumnChunkStatRecorder();
    virtual int write(std::shared_ptr<ColumnVector> vector, int length);
    virtual ByteBuffer getColumnChunkContent();
    virtual int getColumnChunkSize();

protected:
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);
    virtual std::shared_ptr<pixels::proto::ColumnEncoding> getColumnChunkEncoding();
    virtual void close();
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);

private:
    std::vector<int32_t> curPixelVector;
    bool runlengthEncoding;
    void writeCurPartTime(std::shared_ptr<DateColumnVector> columnVector, const int32_t *values, int curPartLength, int curPartOffset);
};

#endif // DUCKDB_DATECOLUMNWRITER_H

