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

#ifndef PARTITIONINDEX_H_
#define PARTITIONINDEX_H_

#include <stdint.h>
#include <string>

/**
 * @file SpillInfo.h
 * @brief 原生MapReduce任务溢写文件索引信息管理头文件，属于NativeTask模块
 *
 * 负责管理Map任务溢写产生的临时文件分段信息，支持溢写文件合并读取，用于Shuffle阶段数据处理
 */

namespace NativeTask {

using std::string;

/**
 * @struct IFileSegment
 * @brief 存储单个分区在溢写文件中的段位置信息
 *
 * 记录分区数据在压缩和未压缩流中的结束偏移量，用于快速定位不同分区的数据范围
 */
struct IFileSegment {
  // 未压缩流中当前分区结束偏移量
  uint64_t uncompressedEndOffset;
  // 压缩流中当前分区结束偏移量
  uint64_t realEndOffset;
};

/**
 * @class SingleSpillInfo
 * @brief 单个溢写文件的索引信息
 *
 * 存储单次Map溢写产生的临时文件的路径、分段索引、校验和类型、键值类型编码压缩等元信息
 * 用于Shuffle阶段从多个溢写文件中拉取对应分区的数据
 */
class SingleSpillInfo {
public:
  // 分区数量
  uint32_t length;
  // 溢写文件路径
  std::string path;
  // 各分区分段信息数组
  IFileSegment * segments;
  // 校验和类型
  ChecksumType checkSumType;
  // 键编码类型
  KeyValueType keyType;
  // 值编码类型
  KeyValueType valueType;
  // 压缩编解码器名称
  std::string codec;

  /**
   * @brief 构造单个溢写文件信息对象
   * @param segments 分区分段信息数组
   * @param len 分区数量
   * @param path 溢写文件路径
   * @param checksum 校验和类型
   * @param ktype 键编码类型
   * @param vtype 值编码类型
   * @param inputCodec 压缩编解码器名称
   */
  SingleSpillInfo(IFileSegment * segments, uint32_t len, const string & path, ChecksumType checksum,
      KeyValueType ktype, KeyValueType vtype, const string & inputCodec)
      : length(len), path(path), segments(segments), checkSumType(checksum), keyType(ktype),
          valueType(vtype), codec(inputCodec) {
  }

  /**
   * @brief 析构函数，释放分段信息数组内存
   */
  ~SingleSpillInfo() {
    delete[] segments;
  }

  /**
   * @brief 删除对应的溢写文件
   */
  void deleteSpillFile();

  /**
   * @brief 获取整个溢写文件未压缩总长度
   * @return 未压缩总字节数
   */
  uint64_t getEndPosition() {
    return segments ? segments[length - 1].uncompressedEndOffset : 0;
  }

  /**
   * @brief 获取整个溢写文件压缩后实际总长度
   * @return 实际存储总字节数
   */
  uint64_t getRealEndPosition() {
    return segments ? segments[length - 1].realEndOffset : 0;
  }

  /**
   * @brief 将溢写索引信息写入指定文件
   * @param filepath 索引文件输出路径
   */
  void writeSpillInfo(const std::string & filepath);
};

/**
 * @class SpillInfos
 * @brief 管理所有单次溢写文件的索引集合
 *
 * Map任务可能会多次溢写，该类聚合所有溢写文件的索引信息，提供统一访问接口
 * 用于归并排序阶段对多个溢写文件的同一分区数据进行多路归并
 */
class SpillInfos {
public:
  // 所有单次溢写索引列表
  std::vector<SingleSpillInfo*> spills;
  /**
   * @brief 构造空溢写索引集合
   */
  SpillInfos() {
  }

  /**
   * @brief 析构函数，释放所有单个溢写索引对象内存
   */
  ~SpillInfos() {
    for (size_t i = 0; i < spills.size(); i++) {
      delete spills[i];
    }
    spills.clear();
  }

  /**
   * @brief 删除所有溢写对应的磁盘临时文件
   */
  void deleteAllSpillFiles() {
    for (size_t i = 0; i < spills.size(); i++) {
      spills[i]->deleteSpillFile();
    }
  }

  /**
   * @brief 添加单个溢写索引到集合
   * @param sri 单个溢写索引对象指针
   */
  void add(SingleSpillInfo * sri) {
    spills.push_back(sri);
  }

  /**
   * @brief 获取溢写文件总数
   * @return 溢写文件数量
   */
  uint32_t getSpillCount() const {
    return spills.size();
  }

  /**
   * @brief 获取指定索引位置的单个溢写信息
   * @param index 溢写索引下标
   * @return 对应单个溢写信息对象指针
   */
  SingleSpillInfo* getSingleSpillInfo(int index) {
    return spills.at(index);
  }
};

} // namespace NativeTask

#endif /* PARTITIONINDEX_H_ */