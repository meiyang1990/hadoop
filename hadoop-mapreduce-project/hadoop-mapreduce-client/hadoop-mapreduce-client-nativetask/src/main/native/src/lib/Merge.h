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
 * @file Merge.h
 * @brief MapReduce本地任务归并排序核心头文件，提供多输入流归并能力
 *
 * 负责在Native MapReduce任务中，对内存分区和磁盘溢出文件进行多路归并排序，
 * 支持Combine函数合并，输出排序后的最终分区文件
 */

#ifndef MERGE_H_
#define MERGE_H_

#include "NativeTask.h"
#include "lib/Buffers.h"
#include "lib/MapOutputCollector.h"
#include "lib/IFile.h"
#include "lib/MinHeap.h"

namespace NativeTask {

/**
 * @class MergeEntry
 * @brief 归并输入项抽象基类，定义归并中单个输入源的接口
 *
 * 代表一个参与归并的输入源，可以是内存中的分区数据或磁盘上的溢出文件，
 * 提供遍历分区和键值对的统一接口
 */
class MergeEntry {

protected:
  // 调用next()成功后填充当前键值对信息
  const char * _key;
  const char * _value;
  uint32_t _keyLength;
  uint32_t _valueLength;

public:
  MergeEntry()
      : _key(NULL), _value(NULL), _keyLength(0), _valueLength(0) {
  }

  const char * getKey() const {
    return _key;
  }

  const char * getValue() const {
    return _value;
  }

  uint32_t getKeyLength() const {
    return _keyLength;
  }

  uint32_t getValueLength() const {
    return _valueLength;
  }

  virtual ~MergeEntry() {
  }

  /**
   * 移动到下一个分区
   * @return true 成功移动到下一个分区，false 已无更多分区
   */
  virtual bool nextPartition() = 0;

  /**
   * 移动到下一个键值对
   * @return true 成功获取下一个键值对，false 已无更多键值对
   */
  virtual bool next() = 0;
};

/**
 * @typedef MergeEntryPtr
 * @brief MergeEntry指针类型别名
 */
typedef MergeEntry * MergeEntryPtr;

/**
 * @class MergeEntryComparator
 * @brief MergeEntry键比较器，用于最小堆排序
 *
 * 包装用户提供的键比较函数，实现归并过程中MergeEntry的排序
 */
class MergeEntryComparator {
private:
  ComparatorPtr _keyComparator;

public:
  MergeEntryComparator(ComparatorPtr comparator)
      : _keyComparator(comparator) {
  }

public:
  bool operator()(const MergeEntryPtr lhs, const MergeEntryPtr rhs) {
    return (*_keyComparator)(lhs->getKey(), lhs->getKeyLength(), rhs->getKey(), rhs->getKeyLength())
        < 0;
  }
};

/**
 * @class MemoryMergeEntry
 * @brief 内存分区归并输入项，处理内存中Map输出分区
 *
 * 遍历内存中多个PartitionBucket的键值对，提供给归并过程使用
 */
class MemoryMergeEntry : public MergeEntry {
protected:

  PartitionBucket ** _partitions;
  uint32_t _number;
  int64_t _index;

  KVIterator * _iterator;
  Buffer keyBuffer;
  Buffer valueBuffer;

public:
  MemoryMergeEntry(PartitionBucket ** partitions, uint32_t numberOfPartitions)
      : _partitions(partitions), _number(numberOfPartitions), _index(-1), _iterator(NULL) {
  }

  virtual ~MemoryMergeEntry() {
    if (NULL != _iterator) {
      delete _iterator;
      _iterator = NULL;
    }
  }

  virtual bool nextPartition() {
    ++_index;
    if (_index < _number) {
      PartitionBucket * current = _partitions[_index];
      if (NULL != _iterator) {
        delete _iterator;
        _iterator = NULL;
      }
      if (NULL != current) {
        _iterator = current->getIterator();
      }
      return true;
    }
    return false;
  }

  virtual bool next() {
    if (NULL == _iterator) {
      return false;
    }
    bool hasNext = _iterator->next(keyBuffer, valueBuffer);

    if (hasNext) {
      _keyLength = keyBuffer.length();
      _key = keyBuffer.data();
      _valueLength = valueBuffer.length();
      _value = valueBuffer.data();
      assert(_value != NULL);
      return true;
    }
    // 提前标记错误状态
    _keyLength = 0xffffffff;
    _valueLength = 0xffffffff;
    _key = NULL;
    _value = NULL;
    return false;
  }
};

/**
 * @class IFileMergeEntry
 * @brief 磁盘溢出文件归并输入项，处理IFile格式的中间溢出文件
 *
 * 读取磁盘上的IFile格式溢出文件，遍历其中分区和键值对，提供给归并过程使用
 */
class IFileMergeEntry : public MergeEntry {
protected:
  IFileReader * _reader;
  bool new_partition;
public:
  /**
   * @param reader IFile读取器，由本对象管理生命周期
   */

  static IFileMergeEntry * create(SingleSpillInfo * spill);

  IFileMergeEntry(IFileReader * reader)
      : _reader(reader) {
    new_partition = false;
  }

  virtual ~IFileMergeEntry() {
    delete _reader;
    _reader = NULL;
  }

  virtual bool nextPartition() {
    return _reader->nextPartition();
  }

  virtual bool next() {
    _key = _reader->nextKey(_keyLength);
    if (unlikely(NULL == _key)) {
      // 提前标记错误状态
      _keyLength = 0xffffffffU;
      _valueLength = 0xffffffffU;
      return false;
    }
    _value = _reader->value(_valueLength);
    return true;
  }
};

/**
 * @class Merger
 * @brief 多路归并器，对多个输入源执行排序归并，输出排序结果
 *
 * 基于最小堆实现多路归并，支持同时合并内存分区和磁盘溢出文件，
 * 支持在归并过程中执行Combine函数合并，最终输出排序后的IFile格式结果
 */
class Merger : public KVIterator {

private:
  vector<MergeEntryPtr> _entries;
  vector<MergeEntryPtr> _heap;
  IFileWriter * _writer;
  Config * _config;
  ICombineRunner * _combineRunner;
  bool _first;
  MergeEntryComparator _comparator;

public:
  /**
   * 构造归并器
   * @param writer 输出结果写入器
   * @param config 配置对象
   * @param comparator 键比较器指针
   * @param combineRunner Combine函数执行器，可为空表示不需要Combine
   */
  Merger(IFileWriter * writer, Config * config, ComparatorPtr comparator,
      ICombineRunner * combineRunner = NULL);

  ~Merger();

  /**
   * 添加一个归并输入项
   * @param pme 归并输入项指针
   */
  void addMergeEntry(MergeEntryPtr pme);

  /**
   * 执行多路归并，输出结果到IFile
   */
  void merge();

  virtual bool next(Buffer & key, Buffer & value);
protected:
  /**
   * 开始处理一个新分区，初始化最小堆
   * @return true 分区存在且初始化成功，false 已无更多分区
   */
  bool startPartition();
  /**
   * 结束当前分区处理，执行清理
   */
  void endPartition();
  /**
   * 初始化归并最小堆
   */
  void initHeap();
  /**
   * 取出当前最小键，推进对应输入源
   * @return true 成功取出下一个键，false 当前分区已处理完
   */
  bool next();
};

} // namespace NativeTask

#endif /* MERGE_H_ */