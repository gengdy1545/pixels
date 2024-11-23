//
// Created by whz on 11/19/24.
//

#ifndef DUCKDB_STRINGCOLUMNWRITER_H
#define DUCKDB_STRINGCOLUMNWRITER_H

#include "writer/BaseColumnWriter.h"
#include "vector/ColumnVector.h"
#include "encoding/RunLenIntEncoder.h"
#include "encoding/Dictionary.h"
#include "utils/EncodingUtils.h"
class StringColumnWriter: public BaseColumnWriter {

public:
    StringColumnWriter(const TypeDescription &type, const PixelsWriterOption &writerOption);
    virtual int write(std::shared_ptr<ColumnVector> vector, int size);
    virtual void newPixel();
    virtual void flush();
    virtual pixels::proto::ColumnEncoding getColumnChunkEncoding() const;
    virtual void close();
    virtual bool decideNullsPadding(const PixelsWriterOption& writerOption);
protected:
    std::unique_ptr<RunLenIntEncoder> encoder;
private:
    /**
     * current vector holding encoded values of string
     */
    std::vector<int> curPixelVector;
    std::unique_ptr<std::vector<int>> startsArray;  // lengths of each string when un-encoded
    std::unique_ptr<Dictionary> dictionary;
    std::unique_ptr<EncodingUtils> encodingUtils;
    const bool runlengthEncoding;
    const bool dictionaryEncoding;
    int startOffset = 0; // the start offset for the current string when un-encoded
    void writeCurPartWithDict(std::shared_ptr<BinaryColumnVector> columnVector, duckdb::string_t* values, const std::vector<int>& vLens, const std::vector<int>& vOffsets,
                                      int curPartLength, int curPartOffset);
    
    void writeCurPartWithoutDict(std::shared_ptr<BinaryColumnVector> columnVector, duckdb::string_t* values, const std::vector<int>& vLens, const std::vector<int>& vOffsets,
                                      int curPartLength, int curPartOffset);   

    void flushStarts();
    void flushDictionary();                           

};
#endif // DUCKDB_STRINGCOLUMNWRITER_H
