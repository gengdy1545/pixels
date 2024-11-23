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
};
#endif // DUCKDB_CHARCOLUMNWRITER_H
