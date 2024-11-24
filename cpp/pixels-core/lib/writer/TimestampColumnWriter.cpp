//
// Created by whz on 11/19/24.
//

#include "writer/TimestampColumnWriter.h"

TimestampColumnWriter::TimestampColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : BaseColumnWriter(type, writerOption), encodingUtils{std::make_unique<EncodingUtils>()},
      curPixelVector(pixelStride), runlengthEncoding{encodingLevel.ge(EncodingLevel::Level::EL2)}
{
    if (runlengthEncoding)
    {
        // time is likely to be negative according to different time zone.
        encoder = std::make_unique<RunLenIntEncoder>(true, true);
    }
}

int TimestampColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<TimestampColumnVector>(vector);
    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }
    auto times = columnVector->times;
    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = size;

    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartTimestamp(columnVector, times, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartTimestamp(columnVector, times, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}
void TimestampColumnWriter::newPixel()
{
    if (runlengthEncoding)
    {
        for (int i = 0; i < curPixelVectorIndex; i++)
        {
            pixelStatRecorder.updateTimestamp(curPixelVector[i]);
        }
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(long));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    }
    else
    {
        bool littleEndian = byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN;
        for (int i = 0; i < curPixelVectorIndex; i++)
        {
            if (littleEndian)
            {
                encodingUtils->writeLongLE(outputStream, curPixelVector[i]);
            }
            else
            {
                encodingUtils->writeLongBE(outputStream, curPixelVector[i]);
            }
            pixelStatRecorder.updateTimestamp(curPixelVector[i]);
        }
    }
    BaseColumnWriter::newPixel();
}

pixels::proto::ColumnEncoding TimestampColumnWriter::getColumnChunkEncoding() const
{
    pixels::proto::ColumnEncoding columnEncoding;
    if (runlengthEncoding)
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    }
    else
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    }
    return columnEncoding;
}

void TimestampColumnWriter::close()
{
    if (runlengthEncoding)
    {
        encoder->clear();
        encoder = nullptr;
    }
    BaseColumnWriter::close();
}

bool TimestampColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    if (writerOption.getEncodingLevel().ge(EncodingLevel::Level::EL2))
    {
        return false;
    }
    return writerOption.isNullsPadding();
}

void TimestampColumnWriter::writeCurPartTimestamp(std::shared_ptr<TimestampColumnVector> columnVector, long *values, int curPartLength, int curPartOffset)
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
                curPixelVector[curPixelVectorIndex++] = 0L;
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
