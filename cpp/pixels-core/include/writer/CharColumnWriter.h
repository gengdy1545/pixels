//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_CHARCOLUMNWRITER_H
#define DUCKDB_CHARCOLUMNWRITER_H

#include "writer/VarcharColumnWriter.h"
class CharColumnWriter : public VarcharColumnWriter
{
public:
    CharColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual ~CharColumnWriter() = default;
    virtual bool decideNullsPadding(const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
};
#endif // DUCKDB_CHARCOLUMNWRITER_H
