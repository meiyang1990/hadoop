// 这个文件已经全部加上中文注释
/*
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

#include "lib/commons.h"
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "lib/NativeObjectFactory.h"
#include "lib/PartitionBucket.h"
#include "lib/Merge.h"
#include "NativeTask.h"
#include "util/WritableUtils.h"
#include "util/DualPivotQuickSort.h"
#include "lib/Combiner.h"
#include "lib/TaskCounters.h"
#include "lib/MinHeap.h"
#include "lib/PartitionBucketIterator.h"

/**
 * @file PartitionBucket.cc
 * @brief MapReduce原生任务分区桶实现，管理单个Reduce分区的内存键值对存储
 */

namespace NativeTask {

/**
 * 获取分区桶的键值对迭代器
 * @return 分区桶迭代器实例，空桶返回NULL
 */
KVIterator * PartitionBucket::getIterator() {
  if (_memBlocks.size() == 0) {
    return NULL;
  }
  return new PartitionBucketIterator(this, _keyComparator);
}

/**
 * 将分区桶中的数据溢出写入到磁盘IFile文件
 * @param writer 磁盘文件写入器
 * @throw IOException 写入IO异常
 * @throw UnsupportException 不支持的操作异常
 */
void PartitionBucket::spill(IFileWriter * writer)
  throw(IOException, UnsupportException) {
  KVIterator * iterator = getIterator();
  if (NULL == iterator || NULL == writer) {
    return;
  }

  if (_combineRunner == NULL) {
    Buffer key;
    Buffer value;

    // 无Combiner，直接遍历写出所有键值对
    while (iterator->next(key, value)) {
      writer->write(key.data(), key.length(), value.data(), value.length());
    }
  } else {
    // 有Combiner，先合并再写出
    _combineRunner->combine(CombineContext(UNKNOWN), iterator, writer);
  }
  delete iterator;
}

/**
 * 对分区桶中所有内存块按键进行排序
 * @param type 排序算法类型
 */
void PartitionBucket::sort(SortAlgorithm type) {
  if (_memBlocks.size() == 0) {
    return;
  }
  if ((!_sorted)) {
    // 遍历对每个内存块单独排序
    for (uint32_t i = 0; i < _memBlocks.size(); i++) {
      MemoryBlock * block = _memBlocks[i];
      block->sort(type, _keyComparator);
    }
  }
  _sorted = true;
}

} // namespace NativeTask