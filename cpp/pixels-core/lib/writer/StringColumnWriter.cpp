#include "utils/Constants.h"
#include "encoding/HashTableDictionary.h"
#include "writer/StringColumnWriter.h"
#include "StringColumnWriter.h"

StringColumnWriter::StringColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption)
    : BaseColumnWriter(type, writerOption),
      curPixelVector(pixelStride),
      encodingUtils(std::make_unique<EncodingUtils>()),
      runlengthEncoding{encodingLevel.ge(EncodingLevel::Level::EL2)},
      dictionaryEncoding{encodingLevel.ge(EncodingLevel::Level::EL1)}
{
    if (runlengthEncoding)
    {
        encoder = std::make_unique<RunLenIntEncoder>(false, true);
    }
    if (dictionaryEncoding)
    {
        dictionary = std::make_unique<HashTableDictionary>(Constants::INIT_DICT_SIZE);
        startsArray = nullptr;
    }
    else
    {
        dictionary = nullptr;
        startsArray = std::make_unique<std::vector<int>>();
    }
}

int StringColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<BinaryColumnVector>(vector);
    auto values = columnVector->vector;
    auto vLens = columnVector->lens;
    auto vOffsets = columnVector->start;
    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = size;

    if (dictionaryEncoding)
    {
        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartWithDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartWithDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);
    }
    else
    {
        // directly add to outputStream if not using dictionary encoding
        while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
        {
            curPartLength = pixelStride - curPixelIsNullIndex;
            writeCurPartWithoutDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);
            newPixel();
            curPartOffset += curPartLength;
            nextPartLength = size - curPartOffset;
        }

        curPartLength = nextPartLength;
        writeCurPartWithoutDict(columnVector, values, vLens, vOffsets, curPartLength, curPartOffset);
    }
    return outputStream->getWritePos();
}

void StringColumnWriter::newPixel()
{
    if (runlengthEncoding)
    {
        // for encoding level 2 or higher, cascade run length encode on dictionary encoding
        std::vector<byte> buffer(curPixelVector.size() * sizeof(int));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    }
    else if (dictionaryEncoding)
    {
        if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
        {
            for (int i = 0; i < curPixelVectorIndex; ++i)
            {
                encodingUtils->writeIntLE(outputStream, curPixelVector[i]);
            }
        }
        else
        {
            for (int i = 0; i < curPixelVectorIndex; ++i)
            {
                encodingUtils->writeIntBE(outputStream, curPixelVector[i]);
            }
        }
    }
    // else write nothing to outputStream
    BaseColumnWriter::newPixel();
}
void StringColumnWriter::writeCurPartWithDict(std::shared_ptr<BinaryColumnVector> columnVector, duckdb::string_t *values, const std::vector<int> &vLens, const std::vector<int> &vOffsets, int curPartLength, int curPartOffset)
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
                curPixelVector[curPixelVectorIndex++] = 0;
            }
        }
        else
        {
            curPixelVector[curPixelVectorIndex++] = dictionary
                                                        ->add((uint8_t *)values[curPartOffset + i].GetData(), vOffsets[curPartOffset + i], vLens[curPartOffset + i]);
            pixelStatRecorder.updateString(
                std::string(values[curPartOffset + i].GetData() + vOffsets[curPartOffset + i], vLens[curPartOffset + i]), 1);
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

void StringColumnWriter::writeCurPartWithoutDict(std::shared_ptr<BinaryColumnVector> columnVector, duckdb::string_t *values, const std::vector<int> &vLens, const std::vector<int> &vOffsets, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        curPixelEleIndex++;
        if (columnVector->isNull[curPartOffset + i])
        {
            hasNull = true;
            pixelStatRecorder.increment();
            if (nullsPadding)
            {
                // add starts even if the current value is null, this is for random access
                startsArray->emplace_back(startOffset);
            }
        }
        else
        {
            outputStream->putBytes((uint8_t *)values[curPartOffset].GetData() + vOffsets[curPartOffset + i], vLens[curPartOffset + i]);
            startsArray->emplace_back(startOffset);
            startOffset += vLens[curPartOffset + i];
            pixelStatRecorder.updateString(
                std::string(values[curPartOffset + i].GetData() + vOffsets[curPartOffset + i], vLens[curPartOffset + i]), 1);
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}
