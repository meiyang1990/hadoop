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

/**
 * @file PartitionBucketIterator.cc
 * @brief MapReduce本地任务归并阶段，分区桶排序迭代器实现
 * 
 * 该文件实现了归并多个内存块中已排序键值对的迭代器，使用最小堆进行多路归并，
 * 用于Map端Spill阶段输出归并和Reduce端输入归并，保证输出键有序。
 */

#include <algorithm>

#include "lib/commons.h"
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "lib/NativeObjectFactory.h"
#include "lib/PartitionBucketIterator.h"
#include "lib/Merge.h"
#include "NativeTask.h"
#include "util/WritableUtils.h"
#include "util/DualPivotQuickSort.h"
#include "lib/Combiner.h"
#include "lib/TaskCounters.h"
#include "lib/MinHeap.h"

namespace NativeTask {

/////////////////////////////////////////////////////////////////
// PartitionBucketIterator
/////////////////////////////////////////////////////////////////

/**
 * @brief 构造分区桶多路归并迭代器
 * 
 * 初始化最小堆结构，为每个内存块创建迭代器，仅保留非空块迭代器，
 * 然后构建最小堆完成多路归并的初始化。
 * 
 * @param pb 分区桶指针，包含多个已排序的内存块
 * @param comparator 键比较器，用于堆排序维持堆序
 */
PartitionBucketIterator::PartitionBucketIterator(PartitionBucket * pb, ComparatorPtr comparator)
    : _pb(pb), _comparator(comparator), _first(true) {
  // 获取分区桶中内存块总数
  uint32_t blockCount = _pb->getMemoryBlockCount();
  // 遍历所有内存块创建迭代器
  for (uint32_t i = 0; i < blockCount; i++) {
    MemoryBlock * block = _pb->getMemoryBlock(i);
    MemBlockIteratorPtr blockIterator = new MemBlockIterator(block);
    // 仅将包含数据的块迭代器加入堆
    if (blockIterator->next()) {
      _heap.push_back(blockIterator);
    } else {
      delete blockIterator;
    }
  }
  // 如果多个块，构建最小堆准备多路归并
  if (_heap.size() > 1) {
    makeHeap(&(_heap[0]), &(_heap[0]) + _heap.size(), _comparator);
  }
}

/**
 * @brief 析构函数，释放所有块迭代器资源
 */
PartitionBucketIterator::~PartitionBucketIterator() {
  for (uint32_t i = 0; i < _heap.size(); i++) {
    MemBlockIteratorPtr ptr = _heap[i];
    if (NULL != ptr) {
      delete ptr;
      _heap[i] = NULL;
    }
  }
}

/**
 * @brief 移动到下一个键值对位置，维护堆结构有序
 * 
 * 取出当前堆顶的最小键，将迭代器前进并调整堆结构，如果当前块已经遍历完，
 * 则弹出该迭代器并重新调整堆。
 * 
 * @return true 存在下一个键值对，false 所有块遍历完成
 */
bool PartitionBucketIterator::next() {
  size_t cur_heap_size = _heap.size();
  if (cur_heap_size > 0) {
    // 不是第一次调用，需要推进堆顶迭代器并调整堆
    if (!_first) {
      // 当前块还有剩余数据，推进后调整堆结构
      if (_heap[0]->next()) {
        // 仅一个块，直接返回
        if (cur_heap_size == 1) {
          return true;
        } else if (cur_heap_size == 2) {
          // 两个元素，简单比较交换调整
          MemBlockIteratorPtr * base = &(_heap[0]);

          if (_comparator(base[1], base[0])) {
            std::swap(base[0], base[1]);
          }
        } else {
          // 多个元素，执行堆化调整
          MemBlockIteratorPtr * base = &(_heap[0]);
          heapify(base, 1, cur_heap_size, _comparator);
        }
      } else {
        // 当前块已经遍历完毕，弹出堆顶迭代器
        delete _heap[0];
        MemBlockIteratorPtr * base = &(_heap[0]);
        popHeap(base, base + cur_heap_size, _comparator);
        _heap.pop_back();
      }
    } else {
      // 标记首次调用已完成
      _first = false;
    }
    // 返回是否还有数据
    return _heap.size() > 0;
  }
  // 堆已空，遍历完成
  return false;
}

/**
 * @brief 获取下一个键值对，同时填充key和value缓冲区
 * 
 * @param key 输出键缓冲区
 * @param value 输出值缓冲区
 * @return true 获取成功，false 遍历完成
 */
bool PartitionBucketIterator::next(Buffer & key, Buffer & value) {
  bool result = next();
  if (result) {
    // 取出堆顶迭代器当前的键值对
    MemBlockIteratorPtr * base = &(_heap[0]);
    KVBuffer * kvBuffer = base[0]->getKVBuffer();

    // 重置缓冲区指向当前键值对
    key.reset(kvBuffer->getKey(), kvBuffer->keyLength);
    value.reset(kvBuffer->getValue(), kvBuffer->valueLength);

    return true;
  }
  return false;
}

} // namespace NativeTask