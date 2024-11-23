#include "utils/Constants.h"
#include "encoding/HashTableDictionary.h"
#include "writer/StringColumnWriter.h"

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
void StringColumnWriter::flush()
{
    // flush out pixels field
    BaseColumnWriter::flush();
    // flush out other fields
    if (dictionaryEncoding)
    {
        flushDictionary();
    }
    else
    {
        flushStarts();
    }
}
pixels::proto::ColumnEncoding StringColumnWriter::getColumnChunkEncoding() const
{
    pixels::proto::ColumnEncoding columnEncoding;
    if (dictionaryEncoding)
    {

        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_DICTIONARY);
        columnEncoding.set_dictionarysize(dictionary->size());
        if (runlengthEncoding)
        {
            auto cascadeColumnEncoding = columnEncoding.mutable_cascadeencoding();
            cascadeColumnEncoding->set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
        }
        return columnEncoding;
    }
    columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    return columnEncoding;
}

void StringColumnWriter::close()
{
    if (dictionaryEncoding)
    {
        dictionary->clear();
    }
    else
    {
        startsArray = nullptr;
    }
    if (runlengthEncoding)
    {
        encoder->clear();
        encoder = nullptr;
    }
    BaseColumnWriter::close();
}

bool StringColumnWriter::decideNullsPadding(const PixelsWriterOption &writerOption)
{
    if (writerOption.getEncodingLevel().ge(EncodingLevel::EncodingLevel::Level::EL2))
    {
        return false;
    }
    return writerOption.isNullsPadding();
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

void StringColumnWriter::flushStarts()
{
    int startsFieldOffset = outputStream->getWritePos();
    startsArray->emplace_back(startOffset); // add the last start offset
    if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
    {
        for (int i = 0; i < startsArray->size(); i++)
        {
            encodingUtils->writeIntLE(outputStream, startsArray->at(i));
        }
    }
    else
    {
        for (int i = 0; i < startsArray->size(); i++)
        {
            encodingUtils->writeIntBE(outputStream, startsArray->at(i));
        }
    }
    startsArray->clear();
    auto byteBuffer = std::make_shared<ByteBuffer>(sizeof(startsFieldOffset));
    if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
    {
        encodingUtils->writeIntLE(byteBuffer, startsFieldOffset);
    }
    else
    {
        encodingUtils->writeIntBE(byteBuffer, startsFieldOffset);
    }
    outputStream->putBytes(byteBuffer->getPointer(), sizeof(startsFieldOffset));
}

void StringColumnWriter::flushDictionary()
{
    int dictContentOffset;
    int dictStartsOffset;
    int size = dictionary->size();
    std::vector<int> starts(size);

    dictContentOffset = outputStream->getWritePos();

    int initStart = 0;
    int currentId = 0;
    // recursively visit the red black tree, and fill origins field, get starts array and orders array
    dictionary->visit([this, &starts, &initStart, &currentId](std::shared_ptr<Dictionary::VisitorContext> context)
                      {
        context->writeBytes(this->outputStream);
        starts[currentId++] = initStart;
        initStart += context->getLength(); });

    dictStartsOffset = outputStream->getWritePos();
    starts[size] = dictStartsOffset - dictContentOffset;

    // write out run length starts array
    if (runlengthEncoding)
    {
        std::vector<byte> res(starts.size() * sizeof(int));
        int resLen;
        encoder->encode(starts.data(), res.data(), starts.size(), resLen);
        outputStream->putBytes(res.data(), resLen);
    }
    else
    {
        if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
        {
            for (int start : starts)
            {
                encodingUtils->writeIntLE(outputStream, start);
            }
        }
        else
        {
            for (int start : starts)
            {
                encodingUtils->writeIntBE(outputStream, start);
            }
        }
    }

    /*
     * Issue #498:
     * We no longer write the orders array (encoded-id to key-index mapping) to files.
     * Encoded id is exactly the index of the key in the dictionary.
     */

    // ByteBuffer offsetsBuf = ByteBuffer.allocate(2 * Integer.BYTES);
    // offsetsBuf.order(byteOrder);
    // offsetsBuf.putInt(dictContentOffset);
    // offsetsBuf.putInt(dictStartsOffset);
    // outputStream.write(offsetsBuf.array());

    auto byteBuffer = std::make_shared<ByteBuffer>(2 * sizeof(int));
    if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
    {
        encodingUtils->writeIntLE(byteBuffer, dictContentOffset);
        encodingUtils->writeIntLE(byteBuffer, dictStartsOffset);
    }
    else
    {
        encodingUtils->writeIntBE(byteBuffer, dictContentOffset);
        encodingUtils->writeIntBE(byteBuffer, dictStartsOffset);
    }
    outputStream->putBytes(byteBuffer->getPointer(), 2 * sizeof(int));
}
