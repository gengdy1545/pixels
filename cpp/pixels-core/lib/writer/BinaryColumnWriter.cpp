//
// Created by whz on 11/19/24.
//
#include "writer/BinaryColumnWriter.h"

BinaryColumnWriter::BinaryColumnWriter(const TypeDescription &type, const PixelsWriterOption& writerOption) 
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
    for (int i = 0; i < curPartLength; i++) {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset]) {
            hasNull = true;
            pixelStatRecorder.increment();
        } else {
            auto& bytes = values[curPartOffset + i];
            int32_t length = int32_t(bytes.GetSize());
            if (length <= maxLength) {
                outputStream ->
                outputStream->(static_cast<char>(length)); // 写入长度
                outputStream.write(bytes.data(), length); // 写入字节
            } else {
                outputStream.put(static_cast<char>(maxLength)); // 写入最大长度
                outputStream.write(bytes.data(), maxLength); // 写入部分字节
                numTruncated++;
            }
            pixelStatRecorder.updateBinary(bytes, 0, length, 1);
        }
    }

    // 将 isNull 信息复制到当前状态
    std::memcpy(isNull.data() + curPixelIsNullIndex, 
                columnVector->isNull.data() + curPartOffset, 
                curPartLength * sizeof(bool));
    curPixelIsNullIndex += curPartLength; 
}
