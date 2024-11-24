//
// Created by whz on 11/19/24.
//
#include "LongDecimalColumnWriter.h"
#include "vector/LongDecimalColumnVector.h"
LongDecimalColumnWriter::LongDecimalColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
     : BaseColumnWriter(type, writerOption), encodingUtils{std::make_unique<EncodingUtils>()} {}

int LongDecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
        auto columnVector = std::static_pointer_cast<LongDecimalColumnVector>(vector);
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
                    encodingUtils->writeLongLE(outputStream, 0L);
                }
            }
            else
            {
                if (littleEndian)
                {
                    encodingUtils->writeLongLE(outputStream, values[i << 1]);
                    encodingUtils->writeLongLE(outputStream, values[(i << 1) + 1]);
                }
                else
                {
                    encodingUtils->writeLongBE(outputStream, values[i << 1]);
                    encodingUtils->writeLongBE(outputStream, values[(i << 1) + 1]);
                }
                pixelStatRecorder.updateInteger128(values[i << 1], values[(i << 1) + 1], 1);
            }
            // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
            if (curPixelEleIndex >= pixelStride)
            {
                newPixel();
            }
        }
        return outputStream->getWritePos();
}

bool LongDecimalColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    return writerOption.isNullsPadding();
}
