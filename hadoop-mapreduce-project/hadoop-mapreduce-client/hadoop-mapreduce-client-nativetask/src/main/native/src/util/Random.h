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
 * @file Random.h
 * @brief 伪随机数生成器头文件，复现Java java.lang.Random算法，支持多种分布随机数和测试数据生成
 * 
 * 属于Hadoop MapReduce原生任务模块，用于在原生代码中生成测试数据和随机样本，
 * 支持均匀分布、对数正态分布随机数，以及随机字节、随机单词生成，方便构建测试数据集。
 */

#ifndef RANDOM_H_
#define RANDOM_H_

#include <stdint.h>
#include <string>

namespace NativeTask {

using std::string;

/**
 * @class Random
 * @brief 伪随机数生成器，兼容Java java.lang.Random算法，支持多种随机数据生成
 * 
 * 核心职责：
 * 1. 复现Java的线性同余伪随机算法，保证和Java端生成结果一致
 * 2. 提供多种数据类型和分布的随机数生成
 * 3. 支持测试数据生成（随机字节序列、随机单词），用于MapReduce任务测试
 */
class Random {
protected:
  // 线性同余发生器参数，和Java java.lang.Random保持一致
  static const int64_t multiplier = 0x5DEECE66DULL;
  static const int64_t addend = 0xBL;
  static const int64_t mask = (1ULL << 48) - 1;
protected:
  // 当前随机种子
  int64_t _seed;

  /**
   * @brief 生成指定位数的下一个伪随机数
   * @param bits 需要生成的随机比特位数
   * @return 指定比特位长度的伪随机整数
   */
  int32_t next(int bits);
public:
  /**
   * @brief 构造函数，使用默认种子初始化随机数生成器
   */
  Random();

  /**
   * @brief 构造函数，使用指定种子初始化随机数生成器
   * @param seed 随机种子
   */
  Random(int64_t seed);

  /**
   * @brief 析构函数
   */
  ~Random();

  /**
   * @brief 设置随机种子，重置随机数生成状态
   * @param seed 新的随机种子
   */
  void setSeed(int64_t seed);

  /**
   * @brief 生成[INT_MIN, INT_MAX]范围内均匀分布的32位有符号整数
   * @return 均匀分布32位有符号整数
   */
  int32_t next_int32();

  /**
   * @brief 生成[0, 2^32-1]范围内均匀分布的32位无符号整数
   * @return 均匀分布32位无符号整数
   */
  uint32_t next_uint32();

  /**
   * @brief 生成[0, 2^64-1]范围内均匀分布的64位无符号整数
   * @return 均匀分布64位无符号整数
   */
  uint64_t next_uint64();

  /**
   * @brief 生成[0, n)范围内均匀分布的32位有符号整数
   * @param n 随机数上限（不包含）
   * @return 均匀分布整数
   */
  int32_t next_int32(int32_t n);

  /**
   * @brief 生成[0.0, 1.0)范围内均匀分布的单精度浮点数
   * @return 均匀分布单精度浮点数
   */
  float nextFloat();

  /**
   * @brief 生成[0.0, 1.0)范围内均匀分布的双精度浮点数
   * @return 均匀分布双精度浮点数
   */
  double nextDouble();

  /**
   * @brief 生成log2正态分布的随机数，范围[0, 2^64-1]
   * @return log2分布随机数
   */
  uint64_t nextLog2();

  /**
   * @brief 生成log2正态分布的随机数，范围[0, range)
   * @param range 随机数上限（不包含）
   * @return log2分布随机数
   */
  uint64_t nextLog2(uint64_t range);

  /**
   * @brief 生成log10正态分布的随机数，范围[0, range)
   * @param range 随机数上限（不包含）
   * @return log10分布随机数
   */
  uint64_t nextLog10(uint64_t range);

  /**
   * @brief 从指定字符范围中随机选择一个字节
   * @param range 候选字符集合，例如"ABCDEFG", "01234566789"
   * @return 随机选中的字符
   */
  char nextByte(const string & range);

  /**
   * @brief 生成指定长度的随机字节序列，每个字节从指定候选范围选择
   * @param length 生成序列长度
   * @param range 候选字符集合
   * @return 随机生成的字节序列
   */
  string nextBytes(uint32_t length, const string & range);

  /**
   * @brief 从预定义100词词库中随机选择一个单词，和RandomTextWriter保持一致，用于生成测试数据
   * @param limit 仅使用词库前limit个单词，-1表示使用全部词库
   * @return 随机选中单词的C字符串指针
   */
  const char * nextWord(int64_t limit = -1);

  /**
   * @brief 从预定义100词词库中随机选择一个单词，写入目标字符串，用于生成测试数据
   * @param dest 输出参数，存储生成的单词
   * @param limit 仅使用词库前limit个单词，-1表示使用全部词库
   */
  void nextWord(string & dest, int64_t limit = -1);
};

} // namespace NativeTask

#endif /* RANDOM_H_ */