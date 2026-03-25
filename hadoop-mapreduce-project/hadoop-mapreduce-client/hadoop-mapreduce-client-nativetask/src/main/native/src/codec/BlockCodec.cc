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
 * @file BlockCodec.cc
 * @brief MapReduce本地任务块压缩编解码器实现
 * @details 实现分块压缩/解压输出流，支持MapReduce shuffle阶段数据块压缩
 */

#include "lib/commons.h"
#include "NativeTask.h"
#include "BlockCodec.h"

namespace NativeTask {

/**
 * @class BlockCompressStream
 * @brief 分块压缩输出流，按块对数据进行压缩后写入底层输出流
 * 每个压缩块会记录原始长度和压缩后长度，支持分块流式压缩
 */

/**
 * @brief 构造函数，初始化分块压缩输出流
 * @param stream 底层输出流，压缩后数据写入此处
 * @param bufferSizeHint 缓冲区大小提示，用于控制单个压缩块的大小
 */
BlockCompressStream::BlockCompressStream(OutputStream * stream, uint32_t bufferSizeHint)
    : CompressStream(stream), _tempBuffer(NULL), _tempBufferSize(0), _compressedBytesWritten(0) {
  _hint = bufferSizeHint;
  _blockMax = bufferSizeHint / 2 * 3;
}

/**
 * @brief 初始化压缩流，分配临时压缩缓冲区
 */
void BlockCompressStream::init() {
  _tempBufferSize = maxCompressedLength(_blockMax) + 8;
  _tempBuffer = new char[_tempBufferSize];
}

/**
 * @brief 析构函数，释放临时压缩缓冲区资源
 */
BlockCompressStream::~BlockCompressStream() {
  delete[] _tempBuffer;
  _tempBuffer = NULL;
  _tempBufferSize = 0;
}

/**
 * @brief 写入待压缩数据，按分块方式处理输入
 * @param buff 待压缩数据缓冲区
 * @param length 待压缩数据长度
 */
void BlockCompressStream::write(const void * buff, uint32_t length) {
  while (length > 0) {
    uint32_t take = length < _blockMax ? length : _hint;
    compressOneBlock(buff, take);
    buff = ((const char *)buff) + take;
    length -= take;
  }
}

/**
 * @brief 刷新底层输出流
 */
void BlockCompressStream::flush() {
  _stream->flush();
}

/**
 * @brief 关闭压缩流，刷新数据
 */
void BlockCompressStream::close() {
  flush();
}

/**
 * @brief 直接写入数据到底层输出流，不进行压缩
 * @param buff 数据缓冲区
 * @param length 数据长度
 */
void BlockCompressStream::writeDirect(const void * buff, uint32_t length) {
  _stream->write(buff, length);
  _compressedBytesWritten += length;
}

/**
 * @brief 获取已压缩写出的字节总数
 * @return 压缩后总字节数
 */
uint64_t BlockCompressStream::compressedBytesWritten() {
  return _compressedBytesWritten;
}

//////////////////////////////////////////////////////////////

/**
 * @class BlockDecompressStream
 * @brief 分块解压输入流，从底层输入流读取分块压缩数据并解压
 */

/**
 * @brief 构造函数，初始化分块解压输入流
 * @param stream 底层输入流，从中读取压缩数据
 * @param bufferSizeHint 缓冲区大小提示
 */
BlockDecompressStream::BlockDecompressStream(InputStream * stream, uint32_t bufferSizeHint)
    : DecompressStream(stream), _tempBuffer(NULL), _tempBufferSize(0) {
  _hint = bufferSizeHint;
  _blockMax = bufferSizeHint / 2 * 3;
  _tempDecompressBuffer = NULL;
  _tempDecompressBufferSize = 0;
  _tempDecompressBufferUsed = 0;
  _tempDecompressBufferCapacity = 0;
  _compressedBytesRead = 0;
}

/**
 * @brief 初始化解压流，分配临时压缩数据缓冲区
 */
void BlockDecompressStream::init() {
  _tempBufferSize = maxCompressedLength(_blockMax) + 8;
  _tempBuffer = (char*)malloc(_tempBufferSize);
}

/**
 * @brief 析构函数，释放所有缓冲区资源
 */
BlockDecompressStream::~BlockDecompressStream() {
  close();
  if (NULL != _tempBuffer) {
    free(_tempBuffer);
    _tempBuffer = NULL;
  }
  _tempBufferSize = 0;
}

/**
 * @brief 读取解压后的数据到目标缓冲区
 * @param buff 目标缓冲区
 * @param length 需要读取的长度
 * @return 实际读取的解压后字节数，-1表示到达流末尾
 */
int32_t BlockDecompressStream::read(void * buff, uint32_t length) {
  // 临时解压缓冲区为空，读取新的压缩块
  if (_tempDecompressBufferSize == 0) {
    uint32_t sizes[2];
    // 读取原始长度和压缩长度两个字段
    int32_t rd = _stream->readFully(&sizes, sizeof(uint32_t) * 2);
    if (rd <= 0) {
      // EOF
      return -1;
    }
    if (rd != sizeof(uint32_t) * 2) {
      THROW_EXCEPTION(IOException, "readFully get incomplete data");
    }
    _compressedBytesRead += rd;
    // 字节序转换，Hadoop使用大端序存储
    sizes[0] = bswap(sizes[0]);
    sizes[1] = bswap(sizes[1]);
    // 解压后数据可以直接放入用户缓冲区，无需经过临时缓冲区
    if (sizes[0] <= length) {
      uint32_t len = decompressOneBlock(sizes[1], buff, sizes[0]);
      if (len != sizes[0]) {
        THROW_EXCEPTION(IOException, "Block decompress data error, length not match");
      }
      return len;
    } else {
      // 解压后数据大于用户请求长度，先放入临时缓冲区再分批返回
      if (sizes[0] > _tempDecompressBufferCapacity) {
        // 缓冲区容量不足，重新分配
        char * newBuffer = (char *)realloc(_tempDecompressBuffer, sizes[0]);
        if (newBuffer == NULL) {
          THROW_EXCEPTION(OutOfMemoryException, "realloc failed");
        }
        _tempDecompressBuffer = newBuffer;
        _tempDecompressBufferCapacity = sizes[0];
      }
      uint32_t len = decompressOneBlock(sizes[1], _tempDecompressBuffer, sizes[0]);
      if (len != sizes[0]) {
        THROW_EXCEPTION(IOException, "Block decompress data error, length not match");
      }
      _tempDecompressBufferSize = sizes[0];
      _tempDecompressBufferUsed = 0;
    }
  }
  // 从临时缓冲区返回数据
  if (_tempDecompressBufferSize > 0) {
    uint32_t left = _tempDecompressBufferSize - _tempDecompressBufferUsed;
    if (length < left) {
      // 只返回部分数据，剩余留到下次读取
      memcpy(buff, _tempDecompressBuffer + _tempDecompressBufferUsed, length);
      _tempDecompressBufferUsed += length;
      return length;
    } else {
      // 返回所有剩余数据，清空缓冲区
      memcpy(buff, _tempDecompressBuffer + _tempDecompressBufferUsed, left);
      _tempDecompressBufferSize = 0;
      _tempDecompressBufferUsed = 0;
      return left;
    }
  }
  // 逻辑错误，不应该执行到此处
  THROW_EXCEPTION(IOException, "Decompress logic error");
  return -1;
}

/**
 * @brief 关闭解压流，释放所有资源
 */
void BlockDecompressStream::close() {
  if (_tempDecompressBufferSize > 0) {
    LOG("[BlockDecompressStream] Some data left in the _tempDecompressBuffer when close()");
  }
  if (NULL != _tempDecompressBuffer) {
    free(_tempDecompressBuffer);
    _tempDecompressBuffer = NULL;
    _tempDecompressBufferCapacity = 0;
  }
  _tempDecompressBufferSize = 0;
  _tempDecompressBufferUsed = 0;
}

/**
 * @brief 直接从底层输入流读取数据，不进行解压
 * @param buff 目标缓冲区
 * @param length 需要读取的长度
 * @return 实际读取的字节数
 */
int32_t BlockDecompressStream::readDirect(void * buff, uint32_t length) {
  if (_tempDecompressBufferSize > 0) {
    THROW_EXCEPTION(IOException, "temp decompress data exists when call readDirect()");
  }
  int32_t ret = _stream->readFully(buff, length);
  if (ret > 0) {
    _compressedBytesRead += ret;
  }
  return ret;
}

/**
 * @brief 获取已读取的压缩字节总数
 * @return 已读取压缩字节数
 */
uint64_t BlockDecompressStream::compressedBytesRead() {
  return _compressedBytesRead;
}

} // namespace NativeTask