//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_BINARYCOLUMNWRITER_H
#define DUCKDB_BINARYCOLUMNWRITER_H
#include "writer/BaseColumnWriter.h"
class BinaryColumnWriter : public BaseColumnWriter {

public:
    BinaryColumnWriter(const TypeDescription& type, const PixelsWriterOption& writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual bool decideNullsPadding(const PixelsWriterOption& writerOption);

private:
    /**
     * Max length of binary. It is recorded in the file footer's schema.
    */
    const uint32_t maxLength;
    int32_t numTruncated;
    void writeCurPartBinary(std::shared_ptr<BinaryColumnVector> columnVector,  duckdb::string_t * values,
                                    int curPartLength, int curPartOffset) ;
};
#endif // DUCKDB_BINARYCOLUMNWRITER_H
