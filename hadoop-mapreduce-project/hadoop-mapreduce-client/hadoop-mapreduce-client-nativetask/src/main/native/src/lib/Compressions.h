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
 * @file Compressions.h
 * @brief 原生MapReduce任务压缩模块头文件，定义压缩/解压缩流接口和压缩编解码器工厂
 */

#ifndef COMPRESSIONS_H_
#define COMPRESSIONS_H_

#include <string>
#include <vector>
#include "lib/Streams.h"

namespace NativeTask {

using std::vector;
using std::string;

/**
 * @class CompressStream
 * @brief 压缩输出流抽象基类，继承自FilterOutputStream，提供数据压缩输出能力
 */
class CompressStream : public FilterOutputStream {
public:
  CompressStream(OutputStream * stream)
      : FilterOutputStream(stream) {
  }

  virtual ~CompressStream();

  virtual void writeDirect(const void * buff, uint32_t length);

  virtual void finish() {
    flush();
  }

  virtual void resetState() {

  }

  virtual uint64_t compressedBytesWritten() {
    return 0;
  }
};

/**
 * @class DecompressStream
 * @brief 解压缩输入流抽象基类，继承自FilterInputStream，提供压缩数据读取解压缩能力
 */
class DecompressStream : public FilterInputStream {
public:
  DecompressStream(InputStream * stream)
      : FilterInputStream(stream) {
  }

  virtual ~DecompressStream();

  virtual int32_t readDirect(void * buff, uint32_t length);

  virtual uint64_t compressedBytesRead() {
    return 0;
  }
};

/**
 * @class Compressions
 * @brief 压缩编解码器工厂类，统一管理支持的压缩算法，提供压缩/解压缩流创建能力
 */
class Compressions {
protected:
  /**
   * @class Codec
   * @brief 压缩编解码器描述类，存储编解码器名称和文件扩展名信息
   */
  class Codec {
  public:
    string name;
    string extension;

    Codec(const string & name, const string & extension)
        : name(name), extension(extension) {
    }
  };

  /** 存储所有支持的编解码器列表 */
  static vector<Codec> SupportedCodecs;

  /** 初始化支持的编解码器列表 */
  static void initCodecs();

public:
  /** Gzip压缩编Codec实例 */
  static const Codec GzipCodec;
  /** Snappy压缩编Codec实例 */
  static const Codec SnappyCodec;
  /** Lz4压缩编Codec实例 */
  static const Codec Lz4Codec;

public:
  /**
   * @brief 检查是否支持指定名称的编解码器
   * @param codec 编解码器名称
   * @return 是否支持
   */
  static bool support(const string & codec);

  /**
   * @brief 获取指定编解码器对应的文件扩展名
   * @param codec 编解码器名称
   * @return 文件扩展名
   */
  static const string getExtension(const string & codec);

  /**
   * @brief 根据文件扩展名获取对应编解码器名称
   * @param extension 文件扩展名
   * @return 编解码器名称
   */
  static const string getCodec(const string & extension);

  /**
   * @brief 根据文件名获取对应编解码器名称（从文件名扩展名解析）
   * @param file 文件名
   * @return 编解码器名称
   */
  static const string getCodecByFile(const string & file);

  /**
   * @brief 根据编解码器名称创建对应的压缩输出流
   * @param codec 编解码器名称
   * @param stream 底层输出流
   * @param bufferSizeHint 缓冲区大小提示
   * @return 压缩输出流实例
   */
  static CompressStream * getCompressionStream(const string & codec, OutputStream * stream,
      uint32_t bufferSizeHint);

  /**
   * @brief 根据编解码器名称创建对应的解压缩输入流
   * @param codec 编解码器名称
   * @param stream 底层输入流
   * @param bufferSizeHint 缓冲区大小提示
   * @return 解压缩输入流实例
   */
  static DecompressStream * getDecompressionStream(const string & codec, InputStream * stream,
      uint32_t bufferSizeHint);
};

} // namespace NativeTask

#endif /* COMPRESSIONS_H_ */