//
// Created by whz on 11/19/24.
//

#ifndef CPP_BASECOLUMNWRITER_H
#define CPP_BASECOLUMNWRITER_H
#include <vector>
#include <string>
#include <sstream>
#include <memory>
#include "writer/ColumnWriter.h"
#include "encoding/EncodingLevel.h"
#include "encoding/Encoder.h"


class BaseColumnWriter : public ColumnWriter{
protected:
  static const int ISNULL_ALIGNMENT;
  static const std::vector<uint8_t> ISNULL_PADDING_BUFFER;

  const int pixelStride;
  const EncodingLevel encodingLevel;
//  const ByteOrder byteOrder;
  const bool nullsPadding;
  std::vector<bool> isNull;
  std::shared_ptr<pixels::proto::ColumnChunkIndex> columnChunkIndex;
  std::shared_ptr<pixels::proto::ColumnStatistic> columnChunkStat;

  StatsRecorder pixelStatRecorder;
  StatsRecorder columnChunkStatRecorder;

  int lastPixelPosition = 0;
  int curPixelPosition = 0;
  int curPixelEleIndex = 0;
  int curPixelVectorIndex = 0;
  int curPixelIsNullIndex = 0;

  //std::unique_ptr<Encoder> encoder;
  bool hasNull = false;

  std::shared_ptr<ByteBuffer> outputStream;
  std::shared_ptr<ByteBuffer> isNullStream;

public:
  BaseColumnWriter(const TypeDescription& type, const PixelsWriterOption& writerOption);
  virtual ~BaseColumnWriter() = default;

  virtual bool decideNullsPadding(const PixelsWriterOption& writerOption) = 0;
  virtual int write(std::shared_ptr<ColumnVector> vector, int size) = 0;

  std::vector<uint8_t> getColumnChunkContent() const;
  int getColumnChunkSize() const;



  const StatsRecorder& getColumnChunkStatRecorder() const;

  virtual pixels::proto::ColumnStatistic getColumnChunkStat() const;
  virtual pixels::proto::ColumnChunkIndex getColumnChunkIndex() const;
  virtual pixels::proto::ColumnEncoding getColumnChunkEncoding() const;

  void flush();
  void newPixel();
  void reset();
  void close();
  ByteOrder byteOrder;
};

#endif //CPP_BASECOLUMNWRITER_H
