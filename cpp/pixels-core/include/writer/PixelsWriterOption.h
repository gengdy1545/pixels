//
// Created by whz on 11/19/24.
//

#ifndef CPP_PIXELSWRITEROPTION_H
#define CPP_PIXELSWRITEROPTION_H
#include <iostream>
#include <string>
#include <vector>
#include "duckdb/planner/table_filter.hpp"
#include "encoding/EncodingLevel.h"
#include "writer/ColumnWriter.h"
//#include ""


class PixelsWriterOption{
public:
    PixelsWriterOption();
    int getPixelsStride() {
        return pixelsStride;
    }

    EncodingLevel getEncodingLevel() {
        return  encodingLevel;
    }

    bool isNullsPadding() {
        return nullsPadding;
    }
    ByteOrder getByteOrder(){
      return byteOrder;
    }

private:
    int pixelsStride;
    EncodingLevel encodingLevel;
    bool nullsPadding;
    ByteOrder byteOrder;


};


#endif //CPP_PIXELSWRITEROPTION_H
