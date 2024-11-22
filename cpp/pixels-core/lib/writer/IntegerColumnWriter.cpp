#include "writer/IntegerColumnWriter.h"

IntegerColumnWriter::IntegerColumnWriter(TypeDescription type, const PixelsWriterOption &writerOption) : BaseColumnWriter(type, writerOption), curPixelVector(pixelStride)
{
    isLong = type.getCategory() == TypeDescription::Category::LONG;
    runlengthEncoding = encodingLevel.ge(EncodingLevel::EL2);
    if (runlengthEncoding)
    {
        encoder = std::make_unique<RunLenIntEncoder>(false);
    }
}

int IntegerColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<LongColumnVector>(vector);
    auto values = columnVector->longVector;

    int curPartLength;         // size of the partition which belongs to current pixel
    int curPartOffset = 0;     // starting offset of the partition which belongs to current pixel
    int nextPartLength = size; // size of the partition which belongs to next pixel

    // do the calculation to partition the vector into current pixel and next one
    // doing this pre-calculation to eliminate branch prediction inside the for loop
    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartLong(columnVector, values, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }

    curPartLength = nextPartLength;
    writeCurPartLong(columnVector, values, curPartLength, curPartOffset);

    return outputStream.getWritePos();
}

void IntegerColumnWriter::writeCurPartLong(std::shared_ptr<ColumnVector> columnVector, long *values, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset])
        {
            hasNull = true;
            // pixelStatRecorder.increment();
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
    std::copy(columnVector->isNull + curPartOffset,
              columnVector->isNull + curPartOffset + curPartLength,
              isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

void IntegerColumnWriter::newPixel()
{
    if (hasNull)
    {
        // TODO
        // isNullStream.write(BitUtils.bitWiseCompact(isNull, curPixelIsNullIndex, byteOrder));
        // pixelStatRecorder.setHasNull();
    }
    // update position of current pixel
    curPixelPosition = outputStream.getWritePos();
    // set current pixel element count to 0 for the next batch pixel writing
    curPixelEleIndex = 0;
    curPixelVectorIndex = 0;
    curPixelIsNullIndex = 0;
    // update column chunk stat
    // columnChunkStatRecorder.merge(pixelStatRecorder);
    // add current pixel stat and position info to columnChunkIndex

    auto pixelStat = columnChunkIndex_->add_pixelstatistics();
    // *pixelStat->mutable_statistic() = pixelStatRecorder.serialize();

    columnChunkIndex_->add_pixelpositions(lastPixelPosition);
    
    // update lastPixelPosition to current one
    lastPixelPosition = curPixelPosition;
    // reset current pixel stat recorder
    // pixelStatRecorder.reset();
    // reset hasNull
    hasNull = false;
}//
// Created by whz on 11/19/24.
//
