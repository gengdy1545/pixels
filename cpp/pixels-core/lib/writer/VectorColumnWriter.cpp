
//
// Created by whz on 11/19/24.
//
#include <cassert>
#include "writer/VectorColumnWriter.h"
VectorColumnWriter::VectorColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : BaseColumnWriter(type, writerOption), encodingUtils{std::make_unique<EncodingUtils>()}
{
}

bool VectorColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    return writerOption.isNullsPadding();
}

int VectorColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<VectorColumnVector>(vector);
    auto values = columnVector->vector;
    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = size;

    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartVec(columnVector, values, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartVec(columnVector, values, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}

void VectorColumnWriter::writeCurPartVec(std::shared_ptr<VectorColumnVector> columnVector, const std::vector<std::vector<double>> &values, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset])
        {
            hasNull = true;
            pixelStatRecorder.increment();
        }
        else
        {
            auto bytesOfOneVec = vecToBytes(values[curPartOffset + i], columnVector->dimension);
            outputStream->putBytes(bytesOfOneVec->getPointer(), bytesOfOneVec->getWritePos());
            pixelStatRecorder.updateVector();
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

std::unique_ptr<ByteBuffer> VectorColumnWriter::vecToBytes(const std::vector<double> &vec, int dimension)
{
    assert(vec.size() == dimension);
    auto buffer = std::make_unique<ByteBuffer>(sizeof(double) * vec.size());
    for (auto value : vec)
    {
        buffer->putDouble(value);
    }
    return buffer;
}
