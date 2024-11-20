//
// Created by whz on 11/19/24.
//
#include "writer/BaseColumnWriter.h"
#include "utils/ConfigFactory.h"

const int BaseColumnWriter::ISNULL_ALIGNMENT = std::stoi(ConfigFactory::Instance().getProperty("isnull.bitmap.alignment"));
const std::vector<uint8_t> BaseColumnWriter::ISNULL_PADDING_BUFFER(BaseColumnWriter::ISNULL_ALIGNMENT, 0);

// 构造函数实现
BaseColumnWriter::BaseColumnWriter(const TypeDescription& type, PixelsWriterOption writerOption)
    : pixelStride(writerOption.getPixelsStride()),
      encodingLevel(writerOption.getEncodingLevel()),
      nullsPadding(decideNullsPadding(writerOption)),
      isNull(pixelStride, false),
      byteOrder(writerOption.getByteOrder())
//      pixelStatRecorder(StatsRecorder::create(type)),
//      columnChunkStatRecorder(StatsRecorder::create(type))
{
  columnChunkIndex.setLittleEndian(byteOrder == ByteOrder::LITTLE_ENDIAN);
  columnChunkIndex.setNullsPadding(nullsPadding);
  columnChunkIndex.setIsNullAlignment(ISNULL_ALIGNMENT);
}


std::vector<uint8_t> BaseColumnWriter::getColumnChunkContent() const {
  const std::string& str = outputStream.str();
  return std::vector<uint8_t>(str.begin(), str.end());
}

int BaseColumnWriter::getColumnChunkSize() const {
  return static_cast<int>(outputStream.str().size());
}

const pixels::proto::ColumnChunkIndex& BaseColumnWriter::getColumnChunkIndex() const {
  return columnChunkIndex;
}

pixels::proto::ColumnStatistic BaseColumnWriter::getColumnChunkStat() const {
  return columnChunkStatRecorder.serialize();
}

const StatsRecorder& BaseColumnWriter::getColumnChunkStatRecorder() const {
  return columnChunkStatRecorder;
}

pixels::proto::ColumnEncoding BaseColumnWriter::getColumnChunkEncoding() const {
  pixels::proto::ColumnEncoding encoding;
  encoding.setKind(pixels::proto::ColumnEncoding::Kind::NONE);
  return encoding;
}

void BaseColumnWriter::flush() {
  if (curPixelEleIndex > 0) {
    newPixel();
  }
  int isNullOffset = static_cast<int>(outputStream.tellp());
  if (ISNULL_ALIGNMENT != 0 && isNullOffset % ISNULL_ALIGNMENT != 0) {
    int alignBytes = ISNULL_ALIGNMENT - (isNullOffset % ISNULL_ALIGNMENT);
    outputStream.write(reinterpret_cast<const char*>(ISNULL_PADDING_BUFFER.data()), alignBytes);
    isNullOffset += alignBytes;
  }
  columnChunkIndex.setIsNullOffset(isNullOffset);
  isNullStream.seekp(0, std::ios::end);
  outputStream << isNullStream.str();
}

void BaseColumnWriter::newPixel() {
  if (hasNull) {
    auto compacted = BitUtils::bitWiseCompact(isNull, curPixelIsNullIndex, byteOrder);
    isNullStream.write(reinterpret_cast<const char*>(compacted.data()), compacted.size());
    pixelStatRecorder.setHasNull();
  }
  curPixelPosition = static_cast<int>(outputStream.tellp());
  curPixelEleIndex = 0;
  curPixelVectorIndex = 0;
  curPixelIsNullIndex = 0;

  columnChunkStatRecorder.merge(pixelStatRecorder);

  pixels::proto::PixelStatistic pixelStat;
  pixelStat.setStatistic(pixelStatRecorder.serialize());
  columnChunkIndex.addPixelPositions(lastPixelPosition);
  columnChunkIndex.addPixelStatistics(pixelStat);

  lastPixelPosition = curPixelPosition;
  pixelStatRecorder.reset();
  hasNull = false;
}

void BaseColumnWriter::reset() {
  lastPixelPosition = 0;
  curPixelPosition = 0;
  columnChunkIndex.Clear();
  columnChunkStat.Clear();
  pixelStatRecorder.reset();
  columnChunkStatRecorder.reset();
  outputStream.str("");
  outputStream.clear();
  isNullStream.str("");
  isNullStream.clear();
}

void BaseColumnWriter::close() {
  outputStream.clear();
  isNullStream.clear();
}