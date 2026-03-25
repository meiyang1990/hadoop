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
 * @file Checksum.h
 * @brief 本地任务校验和计算工具头文件，提供多种校验和类型的计算能力
 * 
 * 该文件属于Hadoop MapReduce本地任务模块，为本地任务处理的数据块提供
 * 标准CRC校验和计算实现，支持多种校验和类型，保证数据传输完整性。
 */

#ifndef CHECKSUM_H_
#define CHECKSUM_H_

#include <stdint.h>
#include <sys/types.h>

namespace NativeTask {

/** 标准CRC32校验和计算函数，使用slice-by-8优化算法 */
extern uint32_t crc32_sb8(uint32_t, const uint8_t *, size_t);
/** CRC32C（Castagnoli）校验和计算函数，使用slice-by-8优化算法 */
extern uint32_t crc32c_sb8(uint32_t, const uint8_t *, size_t);

/**
 * @brief 校验和类型枚举
 * 定义当前支持的所有校验和类型
 */
enum ChecksumType {
  CHECKSUM_NONE,    /**< 不计算校验和 */
  CHECKSUM_CRC32,   /**< 标准CRC32校验和 */
  CHECKSUM_CRC32C,  /**< CRC32C（Castagnoli）校验和 */
};

/**
 * @class Checksum
 * @brief 校验和计算工具类，提供统一接口计算不同类型校验和
 * 
 * 封装了不同类型校验和的初始化、更新、结果获取逻辑，对上层提供统一接口，
 * 支持标准CRC32和CRC32C两种常用校验和算法，用于保证数据传输和存储的完整性。
 * 所有方法均为静态方法，无需实例化即可使用。
 */
class Checksum {
public:
  /**
   * @brief 根据校验和类型初始化学校验和初始值
   * @param type 校验和类型
   * @return 校验和初始值
   */
  static uint32_t init(ChecksumType type) {
    switch (type) {
    case CHECKSUM_NONE:
      return 0;
    case CHECKSUM_CRC32:
      return 0xffffffff;
    case CHECKSUM_CRC32C:
      return 0xffffffff;
    }
    return 0;
  }

  /**
   * @brief 更新校验和，处理输入缓冲区数据
   * @param type 校验和类型
   * @param value 输入当前校验和，输出更新后的校验和
   * @param buff 待处理数据缓冲区指针
   * @param length 待处理数据长度（字节）
   */
  static void update(ChecksumType type, uint32_t & value, const void * buff, uint32_t length) {
    switch (type) {
    case CHECKSUM_NONE:
      return;
    case CHECKSUM_CRC32:
      value = crc32_sb8(value, (const uint8_t *)buff, length);
      return;
    case CHECKSUM_CRC32C:
      value = crc32c_sb8(value, (const uint8_t *)buff, length);
      return;
    }
    return;
  }

  /**
   * @brief 获取最终校验和结果
   * @param type 校验和类型
   * @param value 计算过程中的校验和中间值
   * @return 最终的校验和结果
   */
  static uint32_t getValue(ChecksumType type, uint32_t value) {
    switch (type) {
    case CHECKSUM_NONE:
      return 0;
    case CHECKSUM_CRC32:
    case CHECKSUM_CRC32C:
      return ~value;
    }
    return 0;
  }
};

} // namespace NativeTask

#endif /* CHECKSUM_H_ */