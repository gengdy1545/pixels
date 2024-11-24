#include "writer/DateColumnWriter.h"
#include "encoding/RunLenIntEncoder.h"
DateColumnWriter::DateColumnWriter(const TypeDescription& type, const PixelsWriterOption& writerOption)
    : BaseColumnWriter(type, writerOption),
      curPixelVector(pixelStride),
      runlengthEncoding(encodingLevel.ge(EncodingLevel::Level::EL2)),
      encoder{nullptr}
{
    if (runlengthEncoding)
    {
        // Issue #94: Date.getTime() can be negative if the date is before 1970-1-1.
        encoder = std::make_unique<RunLenIntEncoder>(true, true);
    }
}

int DateColumnWriter::write(std::shared_ptr<ColumnVector> vector, int length)
{
    auto columnVector = std::dynamic_pointer_cast<DateColumnVector>(vector);
    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }

    const int32_t *dates = columnVector->dates;
    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = length;

    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartTime(columnVector, dates, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = length - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartTime(columnVector, dates, curPartLength, curPartOffset);

    return outputStream->getWritePos();
}

void DateColumnWriter::writeCurPartTime(std::shared_ptr<DateColumnVector> columnVector, const int32_t *values, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        curPixelVectorIndex++;
        if (columnVector->isNull[i + curPartOffset])
        {
            hasNull = true;
            pixelStatRecorder.increment();
            if (nullsPadding)
            {
                // padding 0 for nulls
                curPixelVector[curPixelVectorIndex++] = 0;
            }
        }
        else
        {
            curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset];
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength,
              isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

void DateColumnWriter::newPixel()
{
    if (runlengthEncoding)
    {
        std::vector<uint8_t> encodedData(2 * curPixelVectorIndex);
        int resLen = 0;
        encoder->encode(curPixelVector.data(), encodedData.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(encodedData.data(), resLen);
    }
    else
    {
        for (int i = 0; i < curPixelVectorIndex; i++)
        {
            outputStream->putInt(curPixelVector[i]);
            pixelStatRecorder.updateDate(curPixelVector[i]);
        }
    }
    BaseColumnWriter::newPixel();
}

pixels::proto::ColumnEncoding DateColumnWriter::getColumnChunkEncoding()
{
    pixels::proto::ColumnEncoding columnChunkEncodeing;
    if (runlengthEncoding)
    {
        columnChunkEncodeing.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    }
    else
    {
        columnChunkEncodeing.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    }
    return columnChunkEncodeing;
}

void DateColumnWriter::close()
{
    if (runlengthEncoding)
    {
        encoder->clear();
    }
    BaseColumnWriter::close();
}

bool DateColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    if (writerOption.getEncodingLevel().ge(EncodingLevel::Level::EL2))
    {
        return false;
    }
    return writerOption.isNullsPadding();
}
