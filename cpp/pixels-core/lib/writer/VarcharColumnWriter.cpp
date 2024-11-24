//
// Created by whz on 11/19/24.
//
#include "writer/VarcharColumnWriter.h"
#include "vector/BinaryColumnVector.h"

VarcharColumnWriter::VarcharColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : StringColumnWriter(type, writerOption), maxLength{type.getMaxLength()}, numTruncated{0} {}

int VarcharColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length)
{
    std::shared_ptr<BinaryColumnVector> columnVector = std::static_pointer_cast<BinaryColumnVector>(vector);
    auto vLens = columnVector->lens;
    for (auto vLen: vLens)
    {
        if (vLen > maxLength)
        {
            vLen = maxLength;
            ++numTruncated;
        }
    }

    return StringColumnWriter::write(vector, length);
}

void VarcharColumnWriter::reset()
{
    StringColumnWriter::reset();
    numTruncated = 0;
}

int VarcharColumnWriter::getNumTruncated()
{
    return numTruncated;
}

bool VarcharColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    return writerOption.isNullsPadding();
}
