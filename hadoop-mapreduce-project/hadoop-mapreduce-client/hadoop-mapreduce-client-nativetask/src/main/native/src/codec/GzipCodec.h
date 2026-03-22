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
 * @file GzipCodec.h
 * @brief Gzip压缩解压缩编解码实现头文件，为MapReduce本地任务提供Gzip格式压缩流和解压缩流
 */

#ifndef GZIPCODEC_H_
#define GZIPCODEC_H_

#include "lib/Compressions.h"

namespace NativeTask {

/**
 * @class GzipCompressStream
 * @brief Gzip压缩输出流，继承自通用压缩流，实现基于zlib的Gzip格式数据压缩
 *        用于本地MapReduce任务输出数据压缩，处理Gzip格式压缩写入
 */
class GzipCompressStream : public CompressStream {
protected:
  // 已压缩输出的字节总数
  uint64_t _compressedBytesWritten;
  // 内部压缩缓冲区
  char * _buffer;
  // 缓冲区容量大小
  uint32_t _capacity;
  // zlib压缩流上下文指针
  void * _zstream;
  // 压缩是否已完成标记
  bool _finished;
public:
  /**
   * @brief 构造函数，初始化Gzip压缩流
   * @param stream 底层输出流，压缩后数据写入该流
   * @param bufferSizeHint 内部缓冲区大小提示
   */
  GzipCompressStream(OutputStream * stream, uint32_t bufferSizeHint);

  /**
   * @brief 析构函数，释放zlib上下文和缓冲区资源
   */
  virtual ~GzipCompressStream();

  /**
   * @brief 写入原始未压缩数据到压缩流
   * @param buff 原始数据缓冲区
   * @param length 要写入的数据长度
   */
  virtual void write(const void * buff, uint32_t length);

  /**
   * @brief 刷新缓冲区，将已压缩数据写入底层输出流
   */
  virtual void flush();

  /**
   * @brief 关闭压缩流，释放所有资源
   */
  virtual void close();

  /**
   * @brief 完成压缩，输出剩余压缩数据
   */
  virtual void finish() {
    flush();
  }

  /**
   * @brief 重置压缩器状态，用于重用压缩器
   */
  virtual void resetState();

  /**
   * @brief 直接写入原始数据进行压缩，不经过额外缓冲
   * @param buff 原始数据缓冲区
   * @param length 数据长度
   */
  virtual void writeDirect(const void * buff, uint32_t length);

  /**
   * @brief 获取压缩后输出的总字节数
   * @return 压缩输出字节数
   */
  virtual uint64_t compressedBytesWritten() {
    return _compressedBytesWritten;
  }
};

/**
 * @class GzipDecompressStream
 * @brief Gzip解压缩输入流，继承自通用解压缩流，实现基于zlib的Gzip格式数据解压缩
 *        用于本地MapReduce任务读取Gzip格式压缩输入数据
 */
class GzipDecompressStream : public DecompressStream {
protected:
  // 已读取解压缩的压缩字节总数
  uint64_t _compressedBytesRead;
  // 内部解压缩缓冲区
  char * _buffer;
  // 缓冲区容量大小
  uint32_t _capacity;
  // zlib解压缩流上下文指针
  void * _zstream;
  // 是否到达输入流结束标记
  bool _eof;
public:
  /**
   * @brief 构造函数，初始化Gzip解压缩流
   * @param stream 底层压缩输入流，从该流读取压缩数据
   * @param bufferSizeHint 内部缓冲区大小提示
   */
  GzipDecompressStream(InputStream * stream, uint32_t bufferSizeHint);

  /**
   * @brief 析构函数，释放zlib上下文和缓冲区资源
   */
  virtual ~GzipDecompressStream();

  /**
   * @brief 读取解压缩后数据到目标缓冲区
   * @param buff 目标缓冲区
   * @param length 最大读取长度
   * @return 实际读取的解压缩后字节数，-1表示流结束
   */
  virtual int32_t read(void * buff, uint32_t length);

  /**
   * @brief 关闭解压缩流，释放所有资源
   */
  virtual void close();

  /**
   * @brief 直接读取原始压缩数据，直接解压缩到目标缓冲区
   * @param buff 目标缓冲区
   * @param length 最大读取长度
   * @return 实际读取的解压缩后字节数，-1表示流结束
   */
  virtual int32_t readDirect(void * buff, uint32_t length);

  /**
   * @brief 获取已处理的压缩字节总数
   * @return 读取压缩字节数
   */
  virtual uint64_t compressedBytesRead() {
    return _compressedBytesRead;
  }
};

} // namespace NativeTask

#endif /* GZIPCODEC_H_ */