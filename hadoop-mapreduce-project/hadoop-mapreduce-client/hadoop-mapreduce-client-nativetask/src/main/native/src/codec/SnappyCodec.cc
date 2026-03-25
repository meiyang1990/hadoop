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
 * @file SnappyCodec.cc
 * @brief Snappy压缩编解码器本地实现，提供Snappy压缩和解压缩能力
 * 
 * 属于Hadoop MapReduce本地任务模块，为MapReduce中间数据提供Snappy压缩支持
 */

#include "config.h"

#if defined HADOOP_SNAPPY_LIBRARY
#include "lib/commons.h"
#include "NativeTask.h"
#include "SnappyCodec.h"

#include <snappy-c.h>

namespace NativeTask {

/**
 * @brief Snappy压缩流构造函数，初始化Snappy压缩器
 * @param stream 底层输出流，压缩后数据写入该流
 * @param bufferSizeHint 缓冲区大小提示
 */
SnappyCompressStream::SnappyCompressStream(OutputStream * stream, uint32_t bufferSizeHint)
    : BlockCompressStream(stream, bufferSizeHint) {
  init();
}

/**
 * @brief 压缩单个数据块
 * @param buff 待压缩原始数据缓冲区
 * @param length 待压缩原始数据长度
 */
void SnappyCompressStream::compressOneBlock(const void * buff, uint32_t length) {
  // 预留8字节存储原始长度和压缩长度，计算压缩后最大可用空间
  size_t compressedLength = _tempBufferSize - 8;
  // 调用Snappy库执行压缩
  snappy_status ret = snappy_compress((const char*)buff, length, _tempBuffer + 8,
      &compressedLength);
  if (ret == SNAPPY_OK) {
    // 写入原始数据长度（大端字节序）
    ((uint32_t*)_tempBuffer)[0] = bswap(length);
    // 写入压缩后数据长度（大端字节序）
    ((uint32_t*)_tempBuffer)[1] = bswap((uint32_t)compressedLength);
    // 将压缩块写入底层输出流
    _stream->write(_tempBuffer, compressedLength + 8);
    // 累计已压缩输出字节数
    _compressedBytesWritten += (compressedLength + 8);
  } else if (ret == SNAPPY_INVALID_INPUT) {
    THROW_EXCEPTION(IOException, "compress SNAPPY_INVALID_INPUT");
  } else if (ret == SNAPPY_BUFFER_TOO_SMALL) {
    THROW_EXCEPTION(IOException, "compress SNAPPY_BUFFER_TOO_SMALL");
  } else {
    THROW_EXCEPTION(IOException, "compress snappy failed");
  }
}

/**
 * @brief 计算给定原始数据最大压缩后长度
 * @param origLength 原始数据长度
 * @return 压缩后最大长度
 */
uint64_t SnappyCompressStream::maxCompressedLength(uint64_t origLength) {
  return snappy_max_compressed_length(origLength);
}

//////////////////////////////////////////////////////////////

/**
 * @brief Snappy解压缩流构造函数，初始化Snappy解压器
 * @param stream 底层输入流，从该流读取待解压缩数据
 * @param bufferSizeHint 缓冲区大小提示
 */
SnappyDecompressStream::SnappyDecompressStream(InputStream * stream, uint32_t bufferSizeHint)
    : BlockDecompressStream(stream, bufferSizeHint) {
  init();
}

/**
 * @brief 解压缩单个数据块
 * @param compressedSize 压缩块大小
 * @param buff 存储解压缩结果的缓冲区
 * @param length 解压缩缓冲区最大长度
 * @return 解压缩后原始数据长度
 */
uint32_t SnappyDecompressStream::decompressOneBlock(uint32_t compressedSize, void * buff,
    uint32_t length) {
  // 如果当前临时缓冲区不足，扩容
  if (compressedSize > _tempBufferSize) {
    char * newBuffer = (char *)realloc(_tempBuffer, compressedSize);
    if (newBuffer == NULL) {
      THROW_EXCEPTION(OutOfMemoryException, "realloc failed");
    }
    _tempBuffer = newBuffer;
    _tempBufferSize = compressedSize;
  }
  // 从输入流读取完整压缩块数据
  uint32_t rd = _stream->readFully(_tempBuffer, compressedSize);
  if (rd != compressedSize) {
    THROW_EXCEPTION(IOException, "readFully reach EOF");
  }
  // 累计已读取压缩字节数
  _compressedBytesRead += rd;
  size_t uncompressedLength = length;
  // 调用Snappy库执行解压缩
  snappy_status ret = snappy_uncompress(_tempBuffer, compressedSize, (char *)buff,
      &uncompressedLength);
  if (ret == SNAPPY_OK) {
    return uncompressedLength;
  } else if (ret == SNAPPY_INVALID_INPUT) {
    THROW_EXCEPTION(IOException, "decompress SNAPPY_INVALID_INPUT");
  } else if (ret == SNAPPY_BUFFER_TOO_SMALL) {
    THROW_EXCEPTION(IOException, "decompress SNAPPY_BUFFER_TOO_SMALL");
  } else {
    THROW_EXCEPTION(IOException, "decompress snappy failed");
  }
}

/**
 * @brief 计算给定原始数据压缩后最大长度
 * @param origLength 原始数据长度
 * @return 压缩后最大长度
 */
uint64_t SnappyDecompressStream::maxCompressedLength(uint64_t origLength) {
  return snappy_max_compressed_length(origLength);
}
} // namespace NativeTask

#endif // define HADOOP_SNAPPY_LIBRARY