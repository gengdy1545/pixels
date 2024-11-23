#include "ByteColumnWriter.h"
//
// Created by whz on 11/19/24.
//
#include "writer/ByteColumnWriter.h"
ByteColumnWriter::ByteColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : BaseColumnWriter(type, writerOption), curPixelVector(pixelStride, 0)
{
    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
    if (runlengthEncoding)
    {
        encoder = std::make_unique<RunLenByteEncoder>();
    }
}

int ByteColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<ByteColumnVector>(vector);
    auto values = columnVector->vector;
    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = size;

    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartByte(columnVector, values, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartByte(columnVector, values, curPartLength, curPartOffset);
    return outputStream->getWritePos();
}

void ByteColumnWriter::newPixel()
{
    for (int i = 0; i < curPixelVectorIndex; i++)
    {
        pixelStatRecorder.updateInteger(curPixelVector[i], 1);
    }

    if (runlengthEncoding)
    {
        int resLen;
        std::vector<uint8_t> buffer(curPixelVectorIndex * 2); // PENDING
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    }
    else
    {
        outputStream->putBytes(curPixelVector.data(), curPixelVectorIndex);
    }

    BaseColumnWriter::newPixel();
}

pixels::proto::ColumnEncoding ByteColumnWriter::getColumnChunkEncoding() const
{
    pixels::proto::ColumnEncoding encoding;
    if (runlengthEncoding)
    {
        encoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    }
    else
    {
        encoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    }
    return encoding;
}

void ByteColumnWriter::close()
{
    if (runlengthEncoding)
    {
        encoder->close();
    }
    BaseColumnWriter::close();
}

bool ByteColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    if (writerOption.getEncodingLevel().ge(EncodingLevel::Level::EL2))
    {
        return false;
    }
    return writerOption.isNullsPadding();
}

void ByteColumnWriter::writeCurPartByte(std::shared_ptr<ByteColumnVector> columnVector, uint8_t *values, int curPartLength, int curPartOffset)
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
            curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset];
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}
