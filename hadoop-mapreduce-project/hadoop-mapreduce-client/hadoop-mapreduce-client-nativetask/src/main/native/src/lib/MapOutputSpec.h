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
 * @file MapOutputSpec.h
 * @brief  Map任务输出规范定义头文件，定义Native MapReduce任务中Map输出的排序、压缩、存储相关配置
 */

#ifndef MAPOUTPUTSPEC_H_
#define MAPOUTPUTSPEC_H_

#include <string>
#include "util/Checksum.h"
#include "util/WritableUtils.h"
#include "NativeTask.h"

namespace NativeTask {

using std::string;

/**
 * @enum SortAlgorithm
 * @brief 内部排序算法枚举，指定Map输出数据使用的排序实现
 */
enum SortAlgorithm {
  CQSORT = 0,        ///< 使用C标准库qsort排序
  CPPSORT = 1,       ///< 使用C++ STL sort排序
  DUALPIVOTSORT = 2, ///< 使用双枢轴快速排序
};

/**
 * @enum OutputFileType
 * @brief Spill溢出文件类型枚举，指定Map输出溢出数据的存储格式
 */
enum OutputFileType {
  INTERMEDIATE = 0, ///< 简单键值序列文件格式
  IFILE = 1,         ///< 标准Hadoop IFile格式
};

/**
 * @enum SortOrder
 * @brief 键值对排序要求枚举，指定Map输出需要满足的排序规则
 */
enum SortOrder {
  FULLORDER = 0, ///< 全排序，符合Hadoop标准规范，所有键按顺序排列
  GROUPBY = 1,   ///< 分组排序，只要求相同键聚合在一起，不要求整体有序
  NOSORT = 2,    ///< 不排序，不对键做任何排序要求
};

/**
 * @enum CompressionType
 * @brief Map输出压缩类型枚举，指定Map输出数据使用的压缩算法
 */
enum CompressionType {
  PLAIN = 0,    ///< 不压缩，原始数据存储
  SNAPPY = 1,   ///< 使用Snappy压缩算法压缩
};

/**
 * @class MapOutputSpec
 * @brief Map任务输出配置规范类，存储Map输出的各类配置参数，从作业配置中解析生成
 *
 * 该类封装了Native Map任务输出所需的全部配置，包括键值类型、排序规则、压缩算法、校验方式等，
 * 用于指导Native任务对Map输出进行排序、溢出和持久化存储。
 */
class MapOutputSpec {
public:
  KeyValueType keyType;        ///< Map输出键的数据类型
  KeyValueType valueType;      ///< Map输出值的数据类型
  SortOrder sortOrder;         ///< 排序顺序要求
  SortAlgorithm sortAlgorithm; ///< 使用的排序算法
  string codec;                ///< 压缩编解码器名称
  ChecksumType checksumType;   ///< 校验和类型，用于溢出文件数据完整性校验

  /**
   * @brief 从配置对象中解析生成Map输出规范
   * @param config 输入配置对象，包含作业相关配置参数
   * @param spec 输出解析后的Map输出规范对象
   */
  static void getSpecFromConfig(Config * config, MapOutputSpec & spec);
};

} // namespace NativeTask

#endif /* MAPOUTPUTSPEC_H_ */