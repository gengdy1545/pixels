//
// Created by whz on 11/19/24.
//
#include "writer/VarbinaryColumnWriter.h"
VarbinaryColumnWriter::VarbinaryColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : BinaryColumnWriter(type, writerOption) {}