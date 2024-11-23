//
// Created by whz on 11/19/24.
//
#include "writer/BooleanColumnWriter.h"
#include "utils/BitUtils.h"
BooleanColumnWriter::BooleanColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : BaseColumnWriter(type, writerOption), curPixelVector(pixelStride, false) {}

int BooleanColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<BinaryColumnVector>(vector);
    duckdb::string_t *values = columnVector->vector;
    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = size;

    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurBoolean(columnVector, values, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurBoolean(columnVector, values, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}

void BooleanColumnWriter::newPixel()
{
    for (int i = 0; i < curPixelVectorIndex; i++)
    {
        pixelStatRecorder.updateBoolean(curPixelVector[i], 1);
    }
    auto compacted = BitUtils::bitWiseCompact(curPixelVector, curPixelVectorIndex, byteOrder);
    outputStream->putBytes(compacted.data(), compacted.size());
    BaseColumnWriter::newPixel();
}

bool BooleanColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    return writerOption.isNullsPadding();
}

void BooleanColumnWriter::writeCurBoolean(std::shared_ptr<BinaryColumnVector> columnVector, duckdb::string_t *values, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset])
        {
            hasNull = true;
            pixelStatRecorder.increment();
            if (nullsPadding)
            {
                // padding 0 for nulls
                curPixelVector[curPixelVectorIndex++] = 0x00;
            }
        }
        else
        {
            curPixelVector[curPixelVectorIndex++] = values->GetData()[i + curPartOffset];
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}
