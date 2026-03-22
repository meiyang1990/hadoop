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
 * @file BlockCodec.h
 * @brief 块式压缩编解码器基类定义，为Hadoop原生任务提供分块压缩解压框架
 * 
 * 本文件定义了块式压缩流和解压流的抽象基类，所有具体压缩算法的块编解码器
 * 都继承自这些基类实现，支持对MapReduce输出数据进行分块压缩处理，提升压缩效率
 */

#ifndef BLOCKCODEC_H_
#define BLOCKCODEC_H_

#include "lib/Compressions.h"

namespace NativeTask {

/**
 * @class BlockCompressStream
 * @brief 块式压缩输出流抽象基类，提供分块压缩的核心框架逻辑
 * 
 * 核心职责：将输入数据按固定分块大小进行缓存，满块后触发压缩并写入输出流
 * 具体压缩逻辑由子类实现compressOneBlock方法完成，支持多种压缩算法扩展
 */
class BlockCompressStream : public CompressStream {
protected:
  // 缓冲区大小提示
  uint32_t _hint;
  // 单个压缩块最大大小
  uint32_t _blockMax;
  // 临时缓冲区，存储未压缩原始数据
  char * _tempBuffer;
  // 临时缓冲区已分配大小
  uint32_t _tempBufferSize;
  // 已写入压缩数据总字节数
  uint64_t _compressedBytesWritten;
public:
  /**
   * @brief 构造函数，初始化块压缩流
   * @param stream 底层输出流，压缩后数据写入此流
   * @param bufferSizeHint 缓冲区大小提示，用于确定分块大小
   */
  BlockCompressStream(OutputStream * stream, uint32_t bufferSizeHint);

  /**
   * @brief 析构函数，释放缓冲区资源
   */
  virtual ~BlockCompressStream();

  /**
   * @brief 写入原始数据，会先缓存，满块后压缩
   * @param buff 原始数据缓冲区
   * @param length 待写入数据长度
   */
  virtual void write(const void * buff, uint32_t length);

  /**
   * @brief 刷新缓冲区，将剩余未压缩数据压缩输出
   */
  virtual void flush();

  /**
   * @brief 关闭压缩流，刷新并释放资源
   */
  virtual void close();

  /**
   * @brief 直接写入已压缩数据，不经过本层压缩
   * @param buff 已压缩数据缓冲区
   * @param length 数据长度
   */
  virtual void writeDirect(const void * buff, uint32_t length);

  /**
   * @brief 获取已写入压缩数据总字节数
   * @return 压缩后总字节数
   */
  virtual uint64_t compressedBytesWritten();

  /**
   * @brief 初始化压缩流，分配缓冲区
   */
  void init();

protected:
  /**
   * @brief 计算原始数据压缩后的最大长度，供子类重写
   * @param origLength 原始数据长度
   * @return 压缩后最大可能长度
   */
  virtual uint64_t maxCompressedLength(uint64_t origLength) {
    return origLength;
  }

  /**
   * @brief 压缩单个块，抽象方法，由具体压缩算法子类实现
   * @param buff 原始数据缓冲区
   * @param length 原始数据长度
   */
  virtual void compressOneBlock(const void * buff, uint32_t length) {
  }
};

/**
 * @class BlockDecompressStream
 * @brief 块式解压输入流抽象基类，提供分块解压的核心框架逻辑
 * 
 * 核心职责：从输入流读取压缩块，逐块解压后提供给读取方，支持分块压缩数据的流式读取
 * 具体解压逻辑由子类实现decompressOneBlock方法完成
 */
class BlockDecompressStream : public DecompressStream {
protected:
  // 缓冲区大小提示
  uint32_t _hint;
  // 单个压缩块最大大小
  uint32_t _blockMax;
  // 临时压缩数据缓冲区，存储读取到的压缩块
  char * _tempBuffer;
  // 临时压缩缓冲区已分配大小
  uint32_t _tempBufferSize;
  // 临时解压缓冲区，存储解压后的数据
  char * _tempDecompressBuffer;
  // 临时解压缓冲区已分配大小
  uint32_t _tempDecompressBufferSize;
  // 临时解压缓冲区已使用字节数
  uint32_t _tempDecompressBufferUsed;
  // 临时解压缓冲区容量
  uint32_t _tempDecompressBufferCapacity;
  // 已读取压缩数据总字节数
  uint64_t _compressedBytesRead;
public:
  /**
   * @brief 构造函数，初始化块解压流
   * @param stream 底层输入流，从该流读取压缩数据
   * @param bufferSizeHint 缓冲区大小提示
   */
  BlockDecompressStream(InputStream * stream, uint32_t bufferSizeHint);

  /**
   * @brief 析构函数，释放缓冲区资源
   */
  virtual ~BlockDecompressStream();

  /**
   * @brief 读取解压后的数据
   * @param buff 目标缓冲区
   * @param length 最多读取长度
   * @return 实际读取到的字节数，-1表示流结束
   */
  virtual int32_t read(void * buff, uint32_t length);

  /**
   * @brief 关闭解压流，释放资源
   */
  virtual void close();

  /**
   * @brief 直接读取压缩数据，不经过本层解压
   * @param buff 目标缓冲区
   * @param length 最多读取长度
   * @return 实际读取到的字节数
   */
  virtual int32_t readDirect(void * buff, uint32_t length);

  /**
   * @brief 获取已读取压缩数据总字节数
   * @return 压缩数据总字节数
   */
  virtual uint64_t compressedBytesRead();

  /**
   * @brief 初始化解压流，分配缓冲区
   */
  void init();

protected:
  /**
   * @brief 获取原始数据压缩后的最大长度，供子类重写
   * @param origLength 原始数据长度
   * @return 压缩后最大可能长度
   */
  virtual uint64_t maxCompressedLength(uint64_t origLength) {
    return origLength;
  }

  /**
   * @brief 解压单个块，抽象方法，由具体解压算法子类实现
   * @param compressedSize 压缩块大小
   * @param buff 解压后数据目标缓冲区
   * @param length 目标缓冲区容量
   * @return 解压后原始数据长度
   */
  virtual uint32_t decompressOneBlock(uint32_t compressedSize, void * buff, uint32_t length) {
    //TODO: add implementation
    return 0;
  }
};

} // namespace NativeTask

#endif /* BLOCKCODEC_H_ */