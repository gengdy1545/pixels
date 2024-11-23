//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_BYTECOLUMNWRITER_H
#define DUCKDB_BYTECOLUMNWRITER_H
#include "writer/BaseColumnWriter.h"
#include "vector/ByteColumnVector.h"
#include "encoding/RunLenByteEncoder.h"
class ByteColumnWriter : public BaseColumnWriter
{
public:
    ByteColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual void newPixel();
    virtual pixels::proto::ColumnEncoding getColumnChunkEncoding() const;
    virtual void close();
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);

private:
    virtual void writeCurPartByte(std::shared_ptr<ByteColumnVector> ColumnVector, uint8_t *values, int curPartLength, int curPartOffset);
    std::vector<uint8_t> curPixelVector;
    bool runlengthEncoding;
    std::unique_ptr<RunLenByteEncoder> encoder;
};

#endif // DUCKDB_BYTECOLUMNWRITER_H
