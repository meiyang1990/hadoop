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
 * @file Merge.cc
 * MapReduce本地任务归并排序核心实现，对多个溢写输出进行多路归并，支持Combine优化
 */

#include "lib/commons.h"
#include "util/Timer.h"
#include "util/StringUtil.h"
#include "lib/Merge.h"
#include "lib/FileSystem.h"

namespace NativeTask {

/**
 * 创建基于IFile格式溢写文件的归并项
 * @param spill 单个溢写信息，包含文件路径和元数据
 * @return 创建好的归并项对象
 */
IFileMergeEntry * IFileMergeEntry::create(SingleSpillInfo * spill) {
  InputStream * fileOut = FileSystem::getLocal().open(spill->path);
  IFileReader * reader = new IFileReader(fileOut, spill, true);
  return new IFileMergeEntry(reader);
}

/**
 * 多路归并器
 * 负责对Map任务生成的多个溢写文件进行排序归并，支持可选的Combine操作
 */
Merger::Merger(IFileWriter * writer, Config * config, ComparatorPtr comparator,
    ICombineRunner * combineRunner)
    : _writer(writer), _config(config), _combineRunner(combineRunner), _first(true),
        _comparator(comparator) {
}

/**
 * 析构函数，释放归并项资源
 */
Merger::~Merger() {
  _heap.clear();
  for (size_t i = 0; i < _entries.size(); i++) {
    delete _entries[i];
  }
  _entries.clear();
}

/**
 * 添加一个待归并的输入项
 * @param pme 归并项指针
 */
void Merger::addMergeEntry(MergeEntryPtr pme) {
  _entries.push_back(pme);
}

/**
 * 准备下一个分区的归并，检查所有输入项分区数量一致性
 * @return true 存在新分区可以归并，false 所有分区处理完成
 */
bool Merger::startPartition() {
  bool firstPartitionState = false;
  // 遍历所有归并项，检查分区状态一致性
  for (size_t i = 0; i < _entries.size(); i++) {
    bool partitionState = _entries[i]->nextPartition();
    if (i == 0) {
      firstPartitionState = partitionState;
    }
    if (firstPartitionState != partitionState) {
      THROW_EXCEPTION(IOException, "MergeEntry partition number not equal");
    }
  }
  if (firstPartitionState) { // do have new partition
    // 通知输出器开始新分区
    _writer->startPartition();
  }
  return firstPartitionState;
}

/**
 * 结束当前分区归并，通知输出器完成分区写入
 */
void Merger::endPartition() {
  _writer->endPartition();
}

/**
 * 初始化归并堆，将所有有数据的输入项加入堆并构建小顶堆
 */
void Merger::initHeap() {
  _heap.clear();
  for (size_t i = 0; i < _entries.size(); i++) {
    MergeEntryPtr pme = _entries[i];
    if (pme->next()) {
      _heap.push_back(pme);
    }
  }
  // 根据比较器构建有序堆
  makeHeap(&(_heap[0]), &(_heap[0]) + _heap.size(), _comparator);
}

/**
 * 获取下一个排序后的键值对位置，调整堆结构
 * @return true 还有下一个元素，false 当前分区所有元素处理完成
 */
bool Merger::next() {
  size_t cur_heap_size = _heap.size();
  if (cur_heap_size > 0) {
    if (!_first) {
      if (_heap[0]->next()) { // have more, adjust heap
        // 当前堆顶归并项还有更多元素，调整堆结构
        if (cur_heap_size == 1) {
          return true;
        } else if (cur_heap_size == 2) {
          // 只有两个元素，直接比较交换即可
          MergeEntryPtr * base = &(_heap[0]);

          if (_comparator(base[1], base[0])) {
            std::swap(base[0], base[1]);
          }
        } else {
          // 多个元素执行堆化调整
          MergeEntryPtr * base = &(_heap[0]);
          heapify(base, 1, cur_heap_size, _comparator);
        }
        return true;
      } else { // no more, pop heap
        // 当前堆顶归并项已无元素，弹出堆
        MergeEntryPtr * base = &(_heap[0]);
        popHeap(base, base + cur_heap_size, _comparator);
        _heap.pop_back();
      }
    } else {
      // 第一个元素，无需调整堆
      _first = false;
    }
    return _heap.size() > 0;
  }
  return false;
}

/**
 * 获取下一个排序后的键值对
 * @param key 输出参数，键缓冲区
 * @param value 输出参数，值缓冲区
 * @return true 获取成功，false 当前分区处理完成
 */
bool Merger::next(Buffer & key, Buffer & value) {
  bool result = next();
  if (result) {
    MergeEntryPtr * base = &(_heap[0]);
    // 重置缓冲区指向堆顶元素的键值数据
    key.reset(base[0]->getKey(), base[0]->getKeyLength());
    value.reset(base[0]->getValue(), base[0]->getValueLength());
    return true;
  } else {
    return false;
  }
}

/**
 * 执行完整的多路归并，处理所有分区
 */
void Merger::merge() {
  uint64_t total_record = 0;
  _heap.reserve(_entries.size());
  MergeEntryPtr * base = &(_heap[0]);
  // 按分区逐个归并
  while (startPartition()) {
    initHeap();
    if (_heap.size() == 0) {
      endPartition();
      continue;
    }
    _first = true;
    if (_combineRunner == NULL) {
      // 无Combine，直接顺序写出所有排序后的键值对
      while (next()) {
        _writer->write(base[0]->getKey(), base[0]->getKeyLength(), base[0]->getValue(),
            base[0]->getValueLength());
        total_record++;
      }
    } else {
      // 有Combine，委托CombineRunner执行合并优化
      _combineRunner->combine(CombineContext(UNKNOWN), this, _writer);
    }
    endPartition();
  }
}

} // namespace NativeTask