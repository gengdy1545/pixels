#ifndef DUCKDB_INTEGERCOLUMNWRITER_H
#define DUCKDB_INTEGERCOLUMNWRITER_H


#include "writer/BaseColumnWriter.h"
#include "encoding/RunLenIntEncoder.h"
class IntegerColumnWriter : public BaseColumnWriter {
public:
    IntegerColumnWriter(const TypeDescription& type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int length);
    virtual void close();
    virtual void newPixel();
    virtual pixels::proto::ColumnEncoding getColumnChunkEncoding() const;
private:
    bool isLong; //current column type is long or int, used for the first pixel
    bool runlengthEncoding;
    std::unique_ptr<RunLenIntEncoder> encoder;
    std::vector<long> curPixelVector; // current pixel value vector haven't written out yet
    void writeCurPartLong(std::shared_ptr<ColumnVector> columnVector, long* values, int curPartLength, int curPartOffset);
    virtual bool decideNullsPadding(const PixelsWriterOption& writerOption);
};

#endif // PIXELS_INTEGER_COLUMNWRITER_H
