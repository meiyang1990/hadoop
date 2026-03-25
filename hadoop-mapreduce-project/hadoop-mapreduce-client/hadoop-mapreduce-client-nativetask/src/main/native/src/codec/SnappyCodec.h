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
 * @file SnappyCodec.h
 * Snappy压缩算法编解码器头文件，属于Hadoop MapReduce原生任务的压缩模块
 * 提供基于Snappy算法的分块压缩和解压缩流实现，用于MapReduce shuffle阶段的数据压缩
 */

#ifndef SNAPPYCODEC_H_
#define SNAPPYCODEC_H_

#include "lib/Compressions.h"
#include "BlockCodec.h"

namespace NativeTask {

/**
 * @class SnappyCompressStream
 * @brief Snappy算法分块压缩流，继承自通用分块压缩流基类
 * 实现Snappy压缩算法的分块压缩逻辑，为MapReduce任务提供高效的数据压缩能力
 */
class SnappyCompressStream : public BlockCompressStream {
public:
  SnappyCompressStream(OutputStream * stream, uint32_t bufferSizeHint);
protected:
  /**
   * 计算原始数据压缩后的最大可能长度，用于分配输出缓冲区
   * @param origLength 原始数据字节长度
   * @return 压缩后最大字节长度
   */
  virtual uint64_t maxCompressedLength(uint64_t origLength);
  /**
   * 压缩单个数据块
   * @param buff 原始数据缓冲区指针
   * @param length 原始数据长度
   */
  virtual void compressOneBlock(const void * buff, uint32_t length);
};

/**
 * @class SnappyDecompressStream
 * @brief Snappy算法分块解压缩流，继承自通用分块解压缩流基类
 * 实现Snappy压缩数据的分块解压缩逻辑，还原MapReduce shuffle阶段的压缩数据
 */
class SnappyDecompressStream : public BlockDecompressStream {
public:
  SnappyDecompressStream(InputStream * stream, uint32_t bufferSizeHint);

protected:
  /**
   * 获取压缩块解压缩所需的最大输出缓冲区长度
   * @param origLength 压缩数据块长度
   * @return 解压缩后最大可能长度
   */
  virtual uint64_t maxCompressedLength(uint64_t origLength);
  /**
   * 解压缩单个Snappy压缩块
   * @param compressedSize 压缩数据大小
   * @param buff 输出缓冲区指针
   * @param length 输出缓冲区可用长度
   * @return 解压缩后实际数据长度
   */
  virtual uint32_t decompressOneBlock(uint32_t compressedSize, void * buff, uint32_t length);
};

} // namespace NativeTask

#endif /* SNAPPYCODEC_H_ */