//
// Created by whz on 11/19/24.
//
#include "writer/FloatColumnWriter.h"
#include "vector/FloatColumnVector.h"
FloatColumnWriter::FloatColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : BaseColumnWriter(type, writerOption), encodingUtils{std::make_unique<EncodingUtils>()} {}

int FloatColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::dynamic_pointer_cast<FloatColumnVector>(vector);
    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }
    auto values = columnVector->vector;
    bool littleEndian = byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN;
    for (int i = 0; i < size; i++)
    {
        isNull[curPixelIsNullIndex++] = columnVector->isNull[i];
        curPixelEleIndex++;
        if (columnVector->isNull[i])
        {
            hasNull = true;
            pixelStatRecorder.increment();
            if (nullsPadding)
            {
                // padding 0 for nulls
                encodingUtils->writeIntLE(outputStream, 0);
            }
        }
        else
        {
            if (littleEndian)
            {
                encodingUtils->writeIntLE(outputStream, values[i]);
            }
            else
            {
                encodingUtils->writeIntBE(outputStream, values[i]);
            }
            pixelStatRecorder.updateFloat(*reinterpret_cast<float *>(values[i]));
        }
        // if current pixel size satisfies the pixel stride, end the current pixel and start a new one
        if (curPixelEleIndex >= pixelStride)
        {
            newPixel();
        }
    }
    return outputStream->getWritePos();
}
