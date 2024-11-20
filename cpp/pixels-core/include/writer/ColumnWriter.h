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

enum class ByteOrder {
  LITTLE_ENDIAN,
  BIG_ENDIAN
};


class ColumnWriter{
public:
//    ColumnWriter(std::shared_ptr<TypeDescriotion> type);
    static std::shared_ptr<ColumnWriter> newColumnWriter(std::shared_ptr<TypeDescription> type,const PixelsWriterOption& writerOption){
      switch (type->getCategory()) {
      case TypeDescription::TypeDescription::Category::BOOLEAN:
        return std::make_unique<BooleanColumnWriter>(type, writerOption);
      case TypeDescription::Category::BYTE:
        return std::make_unique<ByteColumnWriter>(type, writerOption);
      case TypeDescription::Category::SHORT:
      case TypeDescription::Category::INT:
      case TypeDescription::Category::LONG:
        return std::make_unique<IntegerColumnWriter>(type, writerOption);
      case TypeDescription::Category::FLOAT:
        return std::make_unique<FloatColumnWriter>(type, writerOption);
      case TypeDescription::Category::DOUBLE:
        return std::make_unique<DoubleColumnWriter>(type, writerOption);
      case TypeDescription::Category::DECIMAL:
        if (type->getPrecision() <= MAX_SHORT_DECIMAL_PRECISION) {
          return std::make_unique<DecimalColumnWriter>(type, writerOption);
        } else {
          return std::make_unique<LongDecimalColumnWriter>(type, writerOption);
        }
      case TypeDescription::Category::STRING:
        return std::make_unique<StringColumnWriter>(type, writerOption);
      case TypeDescription::Category::CHAR:
        return std::make_unique<CharColumnWriter>(type, writerOption);
      case TypeDescription::Category::VARCHAR:
        return std::make_unique<VarcharColumnWriter>(type, writerOption);
      case TypeDescription::Category::BINARY:
        return std::make_unique<BinaryColumnWriter>(type, writerOption);
      case TypeDescription::Category::VARBINARY:
        return std::make_unique<VarbinaryColumnWriter>(type, writerOption);
      case TypeDescription::Category::DATE:
        return std::make_unique<DateColumnWriter>(type, writerOption);
      case TypeDescription::Category::TIME:
        return std::make_unique<TimeColumnWriter>(type, writerOption);
      case TypeDescription::Category::TIMESTAMP:
        return std::make_unique<TimestampColumnWriter>(type, writerOption);
      case TypeDescription::Category::VECTOR:
        return std::make_unique<VectorColumnWriter>(type, writerOption);
      default:
        throw std::invalid_argument("Bad schema type: " + std::to_string(static_cast<int>(type.getTypeDescription::Category())));
      }
    }

  /**
   * Write values from input buffers
   *
   */

    virtual int write(std::shared_ptr<ColumnVector> columnVector,int length );

    std::vector<uint8_t> getColumnChunkContent() const;
    int getColumnChunkSize();
    bool decideNullsPadding(PixelsWriterOption writerOption);

    pixels::proto::ColumnChunkIndex getColumnChunkIndex();

    pixels::proto::ColumnStatistic getColumnChunkStat();

    pixels::proto::ColumnEncoding getColumnChunkEncoding();

    StatsRecorder getColumnChunkStatRecorder();

    void reset();

//    void flush() throws IOException;
//
//    void close() throws IOException;
    virtual void flush() = 0;
    virtual void close() = 0;
};




#endif //PIXELS_COLUMNWRITER_H
