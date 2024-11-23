//
// Created by whz on 11/19/24.
//
#include "writer/BinaryColumnWriter.h"

BinaryColumnWriter::BinaryColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : BaseColumnWriter(type, writerOption), maxLength{type.getMaxLength()}, numTruncated{0} {}
int BinaryColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<BinaryColumnVector>(vector);
    auto values = columnVector->vector;

    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = size;

    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartBinary(columnVector, values, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartBinary(columnVector, values, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}

bool BinaryColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    return writerOption.isNullsPadding();
}

void BinaryColumnWriter::writeCurPartBinary(std::shared_ptr<BinaryColumnVector> columnVector, duckdb::string_t *values, int curPartLength, int curPartOffset)
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
            auto &bytes = values[curPartOffset + i];
            int32_t length = int32_t(bytes.GetSize());
            if (length <= maxLength)
            {
                outputStream->putInt(length);
                outputStream->putBytes((uint8_t *)bytes.GetData(), length);
            }
            else
            {
                outputStream->putInt(maxLength);                               // 写入最大长度
                outputStream->putBytes((uint8_t *)bytes.GetData(), maxLength); // 写入部分字节
                numTruncated++;
            }
            pixelStatRecorder.updateBinary(bytes.GetString(), 1);
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}
