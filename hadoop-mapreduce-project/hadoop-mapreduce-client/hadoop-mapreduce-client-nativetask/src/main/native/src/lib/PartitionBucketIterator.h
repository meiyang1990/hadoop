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
 * @file PartitionBucketIterator.h
 * @brief 本地任务中分区桶的键值对迭代器，用于归并排序多个内存块中的键值对
 * 
 * 属于Hadoop MapReduce本地任务模块，负责对同一个Reduce分区内的多个内存块键值对
 * 做二路归并排序，输出有序的键值对序列，为后续Combiner合并或溢写做准备
 */

#ifndef PARTITION_BUCKET_ITERATOR_H_
#define PARTITION_BUCKET_ITERATOR_H_

#include "NativeTask.h"
#include "lib/MemoryPool.h"
#include "util/Timer.h"
#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "lib/IFile.h"
#include "lib/SpillInfo.h"
#include "lib/Combiner.h"
#include "lib/PartitionBucket.h"

namespace NativeTask {

/**
 * @class PartitionBucketIterator
 * @brief 分区桶有序键值对迭代器，基于最小堆实现多路归并排序
 * 
 * 核心职责：将同一个分区内多个已排序的内存块中的键值对，归并排序为一个全局有序的
 * 键值对序列，支持顺序遍历访问，用于Map端输出排序、Combiner执行和溢写阶段
 */
class PartitionBucketIterator : public KVIterator {
protected:
  PartitionBucket * _pb;
  std::vector<MemBlockIteratorPtr> _heap;
  MemBlockComparator _comparator;
  bool _first;

public:
  /**
   * @brief 构造函数，初始化分区桶迭代器
   * @param pb 目标分区桶指针，包含多个已排序的内存块
   * @param comparator 键比较器指针，用于归并排序时比较键的顺序
   */
  PartitionBucketIterator(PartitionBucket * pb, ComparatorPtr comparator);

  /**
   * @brief 析构函数，释放迭代器占用的资源
   */
  virtual ~PartitionBucketIterator();

  /**
   * @brief 获取下一个键值对，实现KVIterator接口
   * @param key 输出参数，存储取出的键
   * @param value 输出参数，存储取出的值
   * @return 如果成功取出返回true，遍历完成返回false
   */
  virtual bool next(Buffer & key, Buffer & value);

private:
  /**
   * @brief 内部方法，调整最小堆获取下一个最小键
   * @return 调整成功且还有元素返回true，否则返回false
   */
  bool next();
};

}
;
//namespace NativeTask

#endif /* PARTITION_BUCKET_ITERATOR_H_ */