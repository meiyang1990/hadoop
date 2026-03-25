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
 * @file PartitionBucket.h
 * @brief MapReduce本地任务单个分区数据存储桶，用于存放同一个分区的map输出键值对
 */

#ifndef PARTITION_BUCKET_H_
#define PARTITION_BUCKET_H_

#include "NativeTask.h"
#include "lib/MemoryPool.h"
#include "lib/MemoryBlock.h"
#include "util/Timer.h"
#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "lib/IFile.h"
#include "lib/SpillInfo.h"
#include "lib/Combiner.h"

namespace NativeTask {

/**
 * @brief 单个Reduce分区的存储桶，存储Map任务输出中对应分区的所有键值对
 * 
 * 核心职责：管理内存中单个分区的键值对存储，支持分配缓冲区、排序、合并、溢写操作
 * 采用多个内存块链式存储，避免预分配大块内存，提升内存利用率
 */
class PartitionBucket {
  friend class PartitionBucketIterator;
  friend class TestPartitionBucket;

private:
  // 存储该分区所有数据的内存块列表
  std::vector<MemoryBlock *> _memBlocks;
  // 内存分配池，用于申请新的内存块
  MemoryPool * _pool;
  // 当前桶所属的分区ID
  uint32_t _partition;
  // 单个内存块的默认大小
  uint32_t _blockSize;
  // 键比较器，用于排序
  ComparatorPtr _keyComparator;
  // Combiner执行器，可选，用于排序后合并相同键
  ICombineRunner * _combineRunner;
  // 标记当前分区数据是否已排序
  bool _sorted;

public:
  /**
   * @brief 构造函数，初始化单个分区存储桶
   * @param pool 内存分配池，用于分配存储数据的内存
   * @param partition 分区ID
   * @param comparator 键比较器，用于排序键值对
   * @param combineRunner Combiner执行器，可为空表示不需要合并
   * @param blockSize 单个内存块默认大小
   */
  PartitionBucket(MemoryPool * pool, uint32_t partition, ComparatorPtr comparator,
      ICombineRunner * combineRunner, uint32_t blockSize)
      : _pool(pool), _partition(partition), _blockSize(blockSize),
          _keyComparator(comparator), _combineRunner(combineRunner),  _sorted(false) {
    if (NULL == _pool || NULL == comparator) {
      THROW_EXCEPTION_EX(IOException, "pool is NULL, or comparator is not set");
    }

    if (NULL != combineRunner) {
      LOG("[PartitionBucket] combine runner has been set");
    }
  }

  /**
   * @brief 析构函数，释放所有占用的内存块
   */
  ~PartitionBucket() {
    reset();
  }

  /**
   * @brief 获取当前桶的分区ID
   * @return 分区ID
   */
  uint32_t getPartitionId() {
    return _partition;
  }

  /**
   * @brief 重置桶，释放所有已分配的内存块，清空数据
   */
  void reset() {
    for (uint32_t i = 0; i < _memBlocks.size(); i++) {
      if (NULL != _memBlocks[i]) {
        delete _memBlocks[i];
        _memBlocks[i] = NULL;
      }
    }
    _memBlocks.clear();
  }

  /**
   * @brief 获取桶中键值对的迭代器
   * @return 键值对迭代器指针
   */
  KVIterator * getIterator();

  /**
   * @brief 获取当前桶中总键值对数量
   * @return 键值对总数
   */
  uint32_t getKVCount() const {
    uint32_t size = 0;
    // 遍历所有内存块累加键值对数量
    for (uint32_t i = 0; i < _memBlocks.size(); i++) {
      MemoryBlock * block = _memBlocks[i];
      if (NULL != block) {
        size += block->getKVCount();
      }
    }
    return size;
  }

  /**
   * @brief 为单个键值对分配存储缓冲区
   * @param kvLength 键值对总长度
   * @return 分配得到的键值对缓冲区，内存不足返回NULL
   * @throws OutOfMemoryException 当总申请内存超过配置的io.sort.mb限制时抛出异常
   */
  KVBuffer * allocateKVBuffer(uint32_t kvLength) {
    if (kvLength == 0) {
      LOG("KV Length is empty, no need to allocate buffer for it");
      return NULL;
    }
    // 新分配数据后，原有排序状态失效
    _sorted = false;
    MemoryBlock * memBlock = NULL;
    uint32_t memBlockSize = _memBlocks.size();
    if (memBlockSize > 0) {
      // 获取最后一个内存块，优先使用它的剩余空间
      memBlock = _memBlocks[memBlockSize - 1];
    }
    // 如果最后一个内存块有足够剩余空间，直接分配
    if (NULL != memBlock && memBlock->remainSpace() >= kvLength) {
      return memBlock->allocateKVBuffer(kvLength);
    } else {
      // 需要分配新的内存块，计算最小需求和预期大小
      uint32_t min = kvLength;
      uint32_t expect = std::max(_blockSize, min);
      uint32_t allocated = 0;
      char * buff = _pool->allocate(min, expect, allocated);
      // 内存分配成功，创建新的内存块并分配缓冲区
      if (NULL != buff) {
        memBlock = new MemoryBlock(buff, allocated);
        _memBlocks.push_back(memBlock);
        return memBlock->allocateKVBuffer(kvLength);
      }
    }
    // 内存分配失败
    return NULL;
  }

  /**
   * @brief 对当前分区所有键值对按key排序
   * @param type 排序算法类型
   */
  void sort(SortAlgorithm type);

  /**
   * @brief 将排序合并后的分区数据溢写到磁盘
   * @param writer IFile写入器，用于写入溢写文件
   * @throws IOException 写入磁盘异常
   * @throws UnsupportException 不支持的操作异常
   */
  void spill(IFileWriter * writer) throw (IOException, UnsupportException);

  /**
   * @brief 获取当前桶使用的内存块数量
   * @return 内存块数量
   */
  uint32_t getMemoryBlockCount() const {
    return _memBlocks.size();
  }

  /**
   * @brief 获取指定索引的内存块
   * @param index 内存块索引
   * @return 内存块指针
   */
  MemoryBlock * getMemoryBlock(uint32_t index) const {
    return _memBlocks[index];
  }
};

}
;
//namespace NativeTask

#endif /* PARTITION_BUCKET_H_ */