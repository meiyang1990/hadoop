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
 * @file Lz4Codec.h
 * @brief  MapReduce本地任务LZ4压缩编解码器头文件，定义LZ4压缩和解压流实现类
 * 
 * 该文件属于Hadoop MapReduce本地任务的压缩模块，提供基于LZ4算法的块压缩实现
 */

#ifndef LZ4CODEC_H_
#define LZ4CODEC_H_

#include "lib/Compressions.h"
#include "BlockCodec.h"

namespace NativeTask {

/**
 * @class Lz4CompressStream
 * @brief  LZ4算法块压缩输出流实现类
 * 
 * 继承自BlockCompressStream，实现基于LZ4算法的单块压缩逻辑，支持按块压缩数据并输出
 */
class Lz4CompressStream : public BlockCompressStream {
public:
  Lz4CompressStream(OutputStream * stream, uint32_t bufferSizeHint);
protected:
  /**
   * @brief 计算原始数据压缩后最大需要的存储空间
   * @param origLength 原始数据长度
   * @return 压缩后最大所需字节数
   */
  virtual uint64_t maxCompressedLength(uint64_t origLength);
  /**
   * @brief 压缩单个数据块
   * @param buff 原始数据缓冲区地址
   * @param length 原始数据长度
   */
  virtual void compressOneBlock(const void * buff, uint32_t length);
};

/**
 * @class Lz4DecompressStream
 * @brief  LZ4算法块解压缩输入流实现类
 * 
 * 继承自BlockDecompressStream，实现基于LZ4算法的单块解压缩逻辑，支持按块读取并解压数据
 */
class Lz4DecompressStream : public BlockDecompressStream {
public:
  Lz4DecompressStream(InputStream * stream, uint32_t bufferSizeHint);
protected:
  /**
   * @brief 获取压缩块对应的原始解压后最大长度
   * @param origLength 压缩块在流中的存储长度
   * @return 解压后最大所需字节数
   */
  virtual uint64_t maxCompressedLength(uint64_t origLength);
  /**
   * @brief 解压单个压缩数据块到目标缓冲区
   * @param compressedSize 压缩块大小
   * @param buff 目标输出缓冲区地址
   * @param length 目标缓冲区最大容量
   * @return 解压后实际数据长度
   */
  virtual uint32_t decompressOneBlock(uint32_t compressedSize, void * buff, uint32_t length);
};

} // namespace NativeTask

#endif /* LZ4CODEC_H_ */