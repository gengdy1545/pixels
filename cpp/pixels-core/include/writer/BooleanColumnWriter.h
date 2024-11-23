//
// Created by whz on 11/19/24.
//

#ifndef PIXELS_BOOLEANCOLUMNWRITER_H
#define PIXELS_BOOLEANCOLUMNWRITER_H
#include "writer/BaseColumnWriter.h"
#include "vector/BinaryColumnVector.h"

class BooleanColumnWriter : public BaseColumnWriter
{
public:
    BooleanColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual void newPixel();
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);

private:
    void writeCurBoolean(std::shared_ptr<BinaryColumnVector> columnVector, duckdb::string_t *values, int curPartLength, int curPartOffset);
    std::vector<uint8_t> curPixelVector;
};
#endif // PIXELS_BOOLEANCOLUMNWRITER_H
