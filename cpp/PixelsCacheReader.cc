#include <iostream>
#include "PixelsCacheReader.h"
#include "MemoryMappedFile.h"
using namespace std;

JNIEXPORT jbyteArray JNICALL Java_io_pixelsdb_pixels_cache_NativePixelsCacheReader_get
  (JNIEnv *env, jclass cls, jlong blockId, jshort rowGroupId, jshort columnId)
{
    jbyteArray res;
    return res;
}


JNIEXPORT jbyteArray JNICALL Java_io_pixelsdb_pixels_cache_NativePixelsCacheReader_sch
  (JNIEnv *env, jclass cls, jlong blockId, jshort rowGroupId, jshort columnId)
{
    jbyteArray res;
    return res;
}