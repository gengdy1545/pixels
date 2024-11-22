//
// Created by whz on 11/19/24.
//
#include "writer/BaseColumnWriter.h"
#include "utils/ConfigFactory.h"
#include "utils/BitUtils.h"

const int BaseColumnWriter::ISNULL_ALIGNMENT = std::stoi(ConfigFactory::Instance().getProperty("isnull.bitmap.alignment"));
const std::vector<uint8_t> BaseColumnWriter::ISNULL_PADDING_BUFFER(BaseColumnWriter::ISNULL_ALIGNMENT, 0);

// 构造函数实现
BaseColumnWriter::BaseColumnWriter(const TypeDescription& type, const PixelsWriterOption& writerOption)
    : pixelStride(writerOption.getPixelsStride()),
      encodingLevel(writerOption.getEncodingLevel()),
      nullsPadding(decideNullsPadding(writerOption)),
      isNull(pixelStride, false),
      byteOrder(writerOption.getByteOrder())
//      pixelStatRecorder(StatsRecorder::create(type)),
//      columnChunkStatRecorder(StatsRecorder::create(type))
{
  columnChunkIndex->set_littleendian(byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN);
  columnChunkIndex->set_nullspadding(nullsPadding);
  columnChunkIndex->set_isnullalignment(ISNULL_ALIGNMENT);
}


std::vector<uint8_t> BaseColumnWriter::getColumnChunkContent() const {
  auto begin = outputStream->getPointer() + outputStream->getReadPos();
  auto end = outputStream->getPointer() + outputStream->getReadPos();
  return std::vector<uint8_t>(begin, end);
}

int BaseColumnWriter::getColumnChunkSize() const {
  return static_cast<int>(outputStream->getWritePos() - outputStream->getReadPos());
}

const std::shared_ptr<pixels::proto::ColumnChunkIndex> BaseColumnWriter::getColumnChunkIndex() const {
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
  encoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
  return encoding;
}

void BaseColumnWriter::flush() {
  if (curPixelEleIndex > 0) {
    newPixel();
  }
  int isNullOffset = static_cast<int>(outputStream->getWritePos());
  if (ISNULL_ALIGNMENT != 0 && isNullOffset % ISNULL_ALIGNMENT != 0) {
    int alignBytes = ISNULL_ALIGNMENT - (isNullOffset % ISNULL_ALIGNMENT);
    outputStream->putBytes(const_cast<uint8_t*>(ISNULL_PADDING_BUFFER.data()), alignBytes);
    isNullOffset += alignBytes;
  }
  columnChunkIndex->set_isnulloffset(isNullOffset);
  outputStream->putBytes(isNullStream->getPointer() + isNullStream->getReadPos(), isNullStream->getWritePos() - isNullStream->getReadPos());
}

void BaseColumnWriter::newPixel() {
  if (hasNull) {
    auto compacted = BitUtils::bitWiseCompact(isNull, curPixelIsNullIndex, byteOrder);
    isNullStream->putBytes(const_cast<uint8_t*>(compacted.data()), compacted.size());
    pixelStatRecorder.setHasNull();
  }
  curPixelPosition = static_cast<int>(outputStream->getWritePos());
  curPixelEleIndex = 0;
  curPixelVectorIndex = 0;
  curPixelIsNullIndex = 0;

  columnChunkStatRecorder.merge(pixelStatRecorder);

  pixels::proto::PixelStatistic pixelStat;
  *pixelStat.mutable_statistic() = pixelStatRecorder.serialize();
  columnChunkIndex->add_pixelpositions(lastPixelPosition);
  auto new_pixelstatistic = columnChunkIndex->add_pixelstatistics();
  *new_pixelstatistic = pixelStat;

  lastPixelPosition = curPixelPosition;
  pixelStatRecorder.reset();
  hasNull = false;
}

void BaseColumnWriter::reset() {
  lastPixelPosition = 0;
  curPixelPosition = 0;
  columnChunkIndex->Clear();
  columnChunkStat->Clear();
  pixelStatRecorder.reset();
  columnChunkStatRecorder.reset();
  outputStream->resetPosition();
  isNullStream->resetPosition();
}

void BaseColumnWriter::close() {
  outputStream->clear();
  isNullStream->clear();
}