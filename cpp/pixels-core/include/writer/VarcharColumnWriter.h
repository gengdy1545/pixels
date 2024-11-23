//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_VARCHARCOLUMNWRITER_H
#define DUCKDB_VARCHARCOLUMNWRITER_H
#include "writer/StringColumnWriter.h"
class VarcharColumnWriter : StringColumnWriter
{
public:
    VarcharColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int length);
    virtual void reset();
    int getNumTruncated();

private:
    /**
     * Max length of varchar. It is recorded in the file footer's schema.
     */
    const int maxLength;
    int numTruncated;
};
#endif // DUCKDB_VARCHARCOLUMNWRITER_H
