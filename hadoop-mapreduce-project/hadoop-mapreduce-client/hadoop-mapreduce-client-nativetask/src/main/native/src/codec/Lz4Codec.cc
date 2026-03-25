// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file Lz4Codec.cc
 * @brief LZ4压缩编解码器实现，提供MapReduce任务中数据块的LZ4压缩和解压缩功能
 */

#include "lib/commons.h"
#include "lz4.h"
#include "NativeTask.h"
#include "Lz4Codec.h"


namespace NativeTask {

/**
 * @brief 计算LZ4压缩后最大可能大小
 * @param orig 原始数据大小
 * @return LZ4压缩后最大可能字节数
 */
static int32_t LZ4_MaxCompressedSize(int32_t orig) {
  return LZ4_compressBound(orig);
}

/**
 * @brief LZ4压缩流构造函数
 * @param stream 底层输出流
 * @param bufferSizeHint 缓冲区大小提示
 */
Lz4CompressStream::Lz4CompressStream(OutputStream * stream, uint32_t bufferSizeHint)
    : BlockCompressStream(stream, bufferSizeHint) {
  init();
}

/**
 * @brief 压缩单个数据块
 * @param buff 原始数据缓冲区
 * @param length 原始数据长度
 */
void Lz4CompressStream::compressOneBlock(const void * buff, uint32_t length) {
  size_t compressedLength = _tempBufferSize - 8;
  // 调用LZ4默认压缩算法
  int ret = LZ4_compress_default((char*)buff, _tempBuffer + 8, length, LZ4_compressBound(length));
  if (ret > 0) {
    compressedLength = ret;
    // 写入原始长度，转换为大端字节序
    ((uint32_t*)_tempBuffer)[0] = bswap(length);
    // 写入压缩后长度，转换为大端字节序
    ((uint32_t*)_tempBuffer)[1] = bswap((uint32_t)compressedLength);
    // 将压缩块写入底层输出流
    _stream->write(_tempBuffer, compressedLength + 8);
    // 累计压缩字节数
    _compressedBytesWritten += (compressedLength + 8);
  } else {
    THROW_EXCEPTION(IOException, "compress LZ4 failed");
  }
}

/**
 * @brief 计算原始数据压缩后最大长度
 * @param origLength 原始数据长度
 * @return 最大压缩后字节数
 */
uint64_t Lz4CompressStream::maxCompressedLength(uint64_t origLength) {
  return LZ4_MaxCompressedSize(origLength);
}

//////////////////////////////////////////////////////////////

/**
 * @brief LZ4解压缩流构造函数
 * @param stream 底层输入流
 * @param bufferSizeHint 缓冲区大小提示
 */
Lz4DecompressStream::Lz4DecompressStream(InputStream * stream, uint32_t bufferSizeHint)
    : BlockDecompressStream(stream, bufferSizeHint) {
  init();
}

/**
 * @brief 解压缩单个数据块
 * @param compressedSize 压缩数据大小
 * @param buff 输出缓冲区
 * @param length 预期解压后长度
 * @return 实际解压后的原始数据长度
 */
uint32_t Lz4DecompressStream::decompressOneBlock(uint32_t compressedSize, void * buff,
    uint32_t length) {
  // 如果当前缓冲区不够，重新分配更大空间
  if (compressedSize > _tempBufferSize) {
    char * newBuffer = (char *)realloc(_tempBuffer, compressedSize);
    if (newBuffer == NULL) {
      THROW_EXCEPTION(OutOfMemoryException, "realloc failed");
    }
    _tempBuffer = newBuffer;
    _tempBufferSize = compressedSize;
  }
  // 从底层输入流读取完整的压缩块
  uint32_t rd = _stream->readFully(_tempBuffer, compressedSize);
  if (rd != compressedSize) {
    THROW_EXCEPTION(IOException, "readFully reach EOF");
  }
  // 累计读取压缩字节数
  _compressedBytesRead += rd;
  // 调用LZ4快速解压缩算法
  uint32_t ret = LZ4_decompress_fast(_tempBuffer, (char*)buff, length);
  if (ret == compressedSize) {
    return length;
  } else {
    THROW_EXCEPTION(IOException, "decompress LZ4 failed");
  }
}

/**
 * @brief 计算原始数据对应最大压缩长度
 * @param origLength 原始数据长度
 * @return 最大压缩后字节数
 */
uint64_t Lz4DecompressStream::maxCompressedLength(uint64_t origLength) {
  return LZ4_MaxCompressedSize(origLength);
}

} // namespace NativeTask