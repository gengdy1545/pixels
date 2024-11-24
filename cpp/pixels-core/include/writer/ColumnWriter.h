//
// Created by whz on 11/19/24.
//

#ifndef PIXELS_COLUMNWRITER_H
#define PIXELS_COLUMNWRITER_H
#include "TypeDescription.h"
#include "physical/natives/ByteBuffer.h"
#include "pixels-common/pixels.pb.h"
#include "math.h"
#include "duckdb.h"
#include "duckdb/common/types/vector.hpp"
#include "PixelsFilter.h"
#include "PixelsWriterOption.h"
#include "stats/StatsRecoder.h"

class ColumnWriter
{
public:
  //    ColumnWriter(std::shared_ptr<TypeDescriotion> type);
  static std::unique_ptr<ColumnWriter> newColumnWriter(std::shared_ptr<TypeDescription> type, const PixelsWriterOption &writerOption);
  /**
   * Write values from input buffers
   *
   */

  virtual int write(std::shared_ptr<ColumnVector> columnVector, int length);

  std::vector<uint8_t> getColumnChunkContent() const;
  virtual int getColumnChunkSize() = 0;
  virtual bool decideNullsPadding(PixelsWriterOption writerOption) = 0;

  virtual pixels::proto::ColumnChunkIndex getColumnChunkIndex() = 0;

  virtual pixels::proto::ColumnStatistic getColumnChunkStat() = 0;

  virtual pixels::proto::ColumnEncoding getColumnChunkEncoding() = 0;

  StatsRecorder getColumnChunkStatRecorder();

  void reset();
  virtual void flush() = 0;
  virtual void close() = 0;
};

#endif // PIXELS_COLUMNWRITER_H
