//
// Created by whz on 11/19/24.
//

#include "writer/CharColumnWriter.h"

CharColumnWriter::CharColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : VarcharColumnWriter(type, writerOption) {};
bool CharColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    return writerOption.isNullsPadding();
}
int CharColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    return VarcharColumnWriter::write(vector, size);
}