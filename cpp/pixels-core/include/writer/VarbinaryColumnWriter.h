//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_VARBINARYCOLUMNWRITER_H
#define DUCKDB_VARBINARYCOLUMNWRITER_H
#include "writer/BinaryColumnWriter.h"

class VarbinaryColumnWriter: public BinaryColumnWriter {
public:
    VarbinaryColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
};
#endif // DUCKDB_VARBINARYCOLUMNWRITER_H
