//
// Created by whz on 11/19/24.
//
#include "writer/ColumnWriter.h"
#include "writer/BooleanColumnWriter.h"
#include "writer/ByteColumnWriter.h"
#include "writer/IntegerColumnWriter.h"
#include "writer/FloatColumnWriter.h"
#include "writer/DoubleColumnWriter.h"
#include "writer/FloatColumnWriter.h"
#include "writer/DoubleColumnWriter.h"
#include "writer/DecimalColumnWriter.h"
#include "writer/LongDecimalColumnWriter.h"
#include "writer/StringColumnWriter.h"
#include "writer/CharColumnWriter.h"
#include "writer/VarcharColumnWriter.h"
#include "writer/BinaryColumnWriter.h"
#include "writer/VarbinaryColumnWriter.h"
#include "writer/DateColumnWriter.h"
#include "writer/TimeColumnWriter.h"
#include "writer/TimestampColumnWriter.h"
#include "writer/VectorColumnWriter.h"

int write(std::shared_ptr<ColumnVector> columnVector, int length)
{
  return 1;
}

std::unique_ptr<ColumnWriter> ColumnWriter::newColumnWriter(const TypeDescription& type, const PixelsWriterOption &writerOption)
{
  switch (type.getCategory())
  {
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
    if (type.getPrecision() <= TypeDescription::SHORT_DECIMAL_MAX_PRECISION)
    {
      return std::make_unique<DecimalColumnWriter>(type, writerOption);
    }
    else
    {
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
    throw std::invalid_argument("Bad schema type: " + std::to_string(static_cast<int>(type.getCategory())));
  }
}