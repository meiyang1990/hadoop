// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file unless in compliance
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
 * @file MemoryBlock.cc
 * 本文件属于Hadoop MapReduce本地任务模块，实现内存块管理，用于存储Map任务输出的键值对，
 * 支持对内存中键值对进行排序，是MapReduce Spill过程中核心的内存数据结构。
 */

#include <algorithm>

#include "NativeTask.h"
#include "lib/commons.h"
#include "util/Timer.h"
#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "lib/IFile.h"
#include "lib/SpillInfo.h"
#include "lib/Combiner.h"

#include "lib/MemoryBlock.h"
#include "lib/MemoryPool.h"
#include "util/DualPivotQuickSort.h"

namespace NativeTask {

class MemoryPool;

/**
 * @brief 内存块构造函数，初始化存储区、大小和读写位置
 * @param pos 预分配的内存起始地址
 * @param size 内存块总大小
 */
MemoryBlock::MemoryBlock(char * pos, uint32_t size)
    : _base(pos), _size(size), _position(0), _sorted(false) {
}

/**
 * @brief 根据索引获取对应键值对的KVBuffer对象
 * @param index 键值对在偏移数组中的索引
 * @return 对应索引的KVBuffer指针，索引越界返回NULL
 */
KVBuffer * MemoryBlock::getKVBuffer(uint32_t index) {
  if (index >= _kvOffsets.size()) {
    return NULL;
  }
  uint32_t offset = _kvOffsets.at(index);
  KVBuffer * kvbuffer = (KVBuffer*)(_base + offset);
  return kvbuffer;
}

/**
 * @brief 对内存块中存储的所有键值对按指定算法排序
 * @param type 排序算法类型，支持标准C++排序和双轴快速排序
 * @param comparator 键比较器，定义排序规则
 */
void MemoryBlock::sort(SortAlgorithm type, ComparatorPtr comparator) {
  // 未排序且键值对数量大于1才执行排序
  if ((!_sorted) && (_kvOffsets.size() > 1)) {
    switch (type) {
    case CPPSORT:
      // 使用C++标准库sort排序
      std::sort(_kvOffsets.begin(), _kvOffsets.end(), ComparatorForStdSort(_base, comparator));
      break;
    case DUALPIVOTSORT: {
      // 使用自定义双轴快速排序，性能更优
      DualPivotQuicksort(_kvOffsets, ComparatorForDualPivotSort(_base, comparator));
    }
      break;
    default:
      // 不支持的排序算法抛出异常
      THROW_EXCEPTION(UnsupportException, "Sort Algorithm not support");
    }
  }
  // 标记为已排序
  _sorted = true;
}
} // namespace NativeTask