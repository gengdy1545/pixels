#include "writer/DecimalColumnWriter.h"
//
// Created by whz on 11/19/24.
//

DecimalColumnWriter::DecimalColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : BaseColumnWriter(type, writerOption), encodingUtils{std::make_unique<EncodingUtils>()} {}

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }
    auto values = columnVector->vector;
    bool littleEndian = (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN);
    for (int i = 0; i < size; i++)
    {
        isNull[curPixelIsNullIndex++] = vector->isNull[i];
        curPixelEleIndex++;
        if (vector->isNull[i])
        {
            hasNull = true;
            pixelStatRecorder.increment();
            if (nullsPadding)
            {
                // padding 0 for nulls
                encodingUtils->writeLongLE(outputStream, 0L);
            }
        }
        else
        {
            if (littleEndian)
            {
                encodingUtils->writeLongLE(outputStream, values[i]);
            }
            else
            {
                encodingUtils->writeLongBE(outputStream, values[i]);
            }
            pixelStatRecorder.updateInteger(values[i], 1);
        }
        // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
        if (curPixelEleIndex >= pixelStride)
        {
            newPixel();
        }
    }
    return outputStream->getWritePos();
}

bool DecimalColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    return writerOption.isNullsPadding();
}
