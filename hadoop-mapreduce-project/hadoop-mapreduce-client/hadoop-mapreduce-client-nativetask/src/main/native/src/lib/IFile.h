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
 * @file IFile.h
 * @brief  NativeTask模块中IFile格式读写器定义，处理Map输出溢写文件的读写
 *
 * IFile是Hadoop MapReduce中存储Map任务输出溢出数据的标准格式，本文件实现了
 * 原生C++版本的读写器，用于原生任务的shuffle过程，提供比Java版更高的性能。
 */

#ifndef IFILE_H_
#define IFILE_H_

#include "util/Checksum.h"
#include "lib/Buffers.h"
#include "util/WritableUtils.h"
#include "lib/SpillInfo.h"
#include "lib/MapOutputSpec.h"

namespace NativeTask {

/**
 * @class IFileReader
 * @brief IFile格式读取器，用于从溢写文件中读取Map输出的<key, value>对
 *
 * 支持多分区读取，按顺序遍历每个分区内的键值对，适配不同键值类型（文本、字节）
 * 和校验类型，支持压缩编解码。
 */
class IFileReader {
private:
  InputStream * _stream;
  ChecksumInputStream * _source;
  ReadBuffer _reader;
  ChecksumType _checksumType;
  KeyValueType _kType;
  KeyValueType _vType;
  string _codec;
  int32_t _segmentIndex;
  SingleSpillInfo * _spillInfo;
  const char * _valuePos;
  uint32_t _valueLen;
  bool _deleteSourceStream;

public:
  /**
   * @brief 构造IFileReader实例
   * @param stream 输入流，指向要读取的IFile文件
   * @param spill 溢写信息，包含分区信息
   * @param deleteSourceStream 是否在析构时删除输入流，默认false
   */
  IFileReader(InputStream * stream, SingleSpillInfo * spill, bool deleteSourceStream = false);

  /**
   * @brief 析构IFileReader，根据配置释放输入流资源
   */
  virtual ~IFileReader();

  /**
   * @brief 移动到下一个分区，准备读取该分区数据
   * @return true 存在下一个分区，false 已无更多分区
   */
  bool nextPartition();

  /**
   * @brief 获取下一个键，同时解析值的位置和长度
   * @param keyLen [输出] 存储键的长度
   * @return 键数据指针，NULL表示当前分区已无更多键值对
   *
   * 读取变长的键长度、值长度，根据键值类型（Text/Bytes）解析长度，
   * 解析完成后值信息保存在实例内部，可通过value()方法获取。
   * 在调用value()之前，返回的键指针保证有效。
   */
  const char * nextKey(uint32_t & keyLen) {
    // 读取键长度（变长编码）
    int64_t t1 = _reader.readVLong();
    // 读取值长度（变长编码）
    int64_t t2 = _reader.readVLong();
    // 键值对结束标记，返回NULL表示当前分区遍历完毕
    if (t1 == -1) {
      return NULL;
    }
    // 从读缓存中获取整个键值对的内存地址
    const char * kvbuff = _reader.get((uint32_t)(t1 + t2));
    uint32_t len;
    // 根据键类型解析键长度
    switch (_kType) {
    case TextType:
      // Text类型使用VInt编码长度
      keyLen = WritableUtils::ReadVInt(kvbuff, len);
      break;
    case BytesType:
      // Bytes类型使用大端4字节整数存储长度，需要交换字节序
      keyLen = bswap(*(uint32_t*)kvbuff);
      len = 4;
      break;
    default:
      // 原生类型直接使用读取到的总长度
      keyLen = t1;
      len = 0;
    }
    // 计算键数据和值数据的起始位置
    const char * kbuff = kvbuff + len;
    const char * vbuff = kvbuff + (uint32_t)t1;
    // 根据值类型解析值长度和位置
    switch (_vType) {
    case TextType:
      _valueLen = WritableUtils::ReadVInt(vbuff, len);
      _valuePos = vbuff + len;
      break;
    case BytesType:
      _valueLen = bswap(*(uint32_t*)vbuff);
      _valuePos = vbuff + 4;
      break;
    default:
      _valueLen = t2;
      _valuePos = vbuff;
    }
    // 返回键数据指针
    return kbuff;
  }

  /**
   * @brief 获取当前键对应的值的长度
   * @return 当前值长度
   */
  uint32_t valueLen() {
    return _valueLen;
  }

  /**
   * @brief 获取当前键对应的值数据
   * @param valueLen [输出] 存储值的长度
   * @return 值数据指针
   */
  const char * value(uint32_t & valueLen) {
    valueLen = _valueLen;
    return _valuePos;
  }
};

/**
 * @class IFileWriter
 * @brief IFile格式写入器，用于将Map输出的<key, value>对写入溢写文件
 *
 * 继承Collector接口，支持按分区写入，自动处理校验和键值类型编码，
 * 最终生成溢写信息供后续shuffle阶段读取。
 */
class IFileWriter : public Collector {
protected:
  OutputStream * _stream;
  ChecksumOutputStream * _dest;
  ChecksumType _checksumType;
  KeyValueType _kType;
  KeyValueType _vType;
  string _codec;
  AppendBuffer _appendBuffer;
  vector<IFileSegment> _spillFileSegments;
  Counter * _recordCounter;
  uint64_t _recordCount;

  bool _deleteTargetStream;

private:
  /**
   * @brief 将vector存储的分区段信息转换为数组
   * @param segments 分区段vector
   * @return 转换后的数组首地址
   */
  IFileSegment * toArray(std::vector<IFileSegment> *segments);

public:
  /**
   * @brief 静态工厂方法，根据路径和输出规格创建IFileWriter
   * @param filepath 输出文件路径
   * @param spec Map输出规格，包含键值类型、校验类型等配置
   * @param spilledRecords 溢出记录计数器，用于统计
   * @return 创建好的IFileWriter实例
   */
  static IFileWriter * create(const std::string & filepath, const MapOutputSpec & spec,
      Counter * spilledRecords);

  /**
   * @brief 构造IFileWriter实例
   * @param stream 输出流，指向要写入的IFile文件
   * @param checksumType 校验和类型
   * @param ktype 键类型
   * @param vtype 值类型
   * @param codec 压缩编解码器名称
   * @param recordCounter 记录计数器
   * @param deleteTargetStream 是否在析构时删除输出流，默认false
   */
  IFileWriter(OutputStream * stream, ChecksumType checksumType, KeyValueType ktype,
      KeyValueType vtype, const string & codec, Counter * recordCounter,
      bool deleteTargetStream = false);

  /**
   * @brief 析构IFileWriter，根据配置释放输出流资源
   */
  virtual ~IFileWriter();

  /**
   * @brief 开始一个新分区的写入
   */
  void startPartition();

  /**
   * @brief 结束当前分区的写入，记录分区信息
   */
  void endPartition();

  /**
   * @brief 写入一个<key, value>对到当前分区
   * @param key 键数据指针
   * @param keyLen 键长度
   * @param value 值数据指针
   * @param valueLen 值长度
   */
  virtual void write(const char * key, uint32_t keyLen, const char * value, uint32_t valueLen);

  /**
   * @brief 获取写入完成后的溢写信息，包含所有分区分段信息
   * @return 溢写信息实例指针
   */
  SingleSpillInfo * getSpillInfo();

  /**
   * @brief 获取当前写入统计信息
   * @param offset [输出] 逻辑偏移量
   * @param realOffset [输出] 实际物理偏移量
   * @param recordCount [输出] 总记录数
   */
  void getStatistics(uint64_t & offset, uint64_t & realOffset, uint64_t & recordCount);

  /**
   * @brief Collector接口的collect实现，转发给write方法
   * @param key 键数据指针
   * @param keyLen 键长度
   * @param value 值数据指针
   * @param valueLen 值长度
   */
  virtual void collect(const void * key, uint32_t keyLen, const void * value, uint32_t valueLen) {
    write((const char*)key, keyLen, (const char*)value, valueLen);
  }
};

} // namespace NativeTask

#endif /* IFILE_H_ */