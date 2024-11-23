//
// Created by whz on 11/19/24.
//

#include "writer/CharColumnWriter.h"

CharColumnWriter::CharColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : VarcharColumnWriter(type, writerOption) {};