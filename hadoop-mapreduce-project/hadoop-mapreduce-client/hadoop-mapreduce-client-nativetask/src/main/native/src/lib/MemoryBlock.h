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
 * @file MemoryBlock.h
 * @brief 本地任务内存块管理头文件，提供MapReduce本地排序的内存KV存储与排序能力
 * 
 * 该文件属于Hadoop MapReduce本地任务模块，负责管理排序阶段的内存分配，
 * 支持双枢轴快速排序和标准排序，提供迭代器和比较器用于多路归并排序。
 */

#include "commons.h"

#ifndef MEMORYBLOCK_H_
#define MEMORYBLOCK_H_

namespace NativeTask {

class MemoryPool;

/**
 * @brief 双枢轴快速排序比较器类
 * 
 * 用于双枢轴快速排序算法中，基于偏移量比较内存中两个KV键的大小
 */
class ComparatorForDualPivotSort {
private:
  const char * _base;
  ComparatorPtr _keyComparator;
public:
  ComparatorForDualPivotSort(const char * base, ComparatorPtr comparator)
      : _base(base), _keyComparator(comparator) {
  }

  inline int operator()(uint32_t lhs, uint32_t rhs) {
    KVBuffer * left = (KVBuffer *)(_base + lhs);
    KVBuffer * right = (KVBuffer *)(_base + rhs);
    return (*_keyComparator)(left->content, left->keyLength, right->content, right->keyLength);
  }
};

/**
 * @brief 标准库排序比较器类
 * 
 * 用于C++标准库排序算法中，基于偏移量比较内存中两个KV键的大小
 * 返回bool类型结果适配标准排序接口
 */
class ComparatorForStdSort {
private:
  const char * _base;
  ComparatorPtr _keyComparator;
public:
  ComparatorForStdSort(const char * base, ComparatorPtr comparator)
      : _base(base), _keyComparator(comparator) {
  }

public:
  inline bool operator()(uint32_t lhs, uint32_t rhs) {
    KVBuffer * left = (KVBuffer *)(_base + lhs);
    KVBuffer * right = (KVBuffer *)(_base + rhs);
    int ret = (*_keyComparator)(left->getKey(), left->keyLength, right->getKey(), right->keyLength);
    return ret < 0;
  }
};

/**
 * @class MemoryBlock
 * @brief 内存块类，管理一块连续内存用于存储KV键值对，支持排序
 * 
 * 核心职责：
 * 1. 在预分配的连续内存块中分配KV缓冲区
 * 2. 记录所有KV的偏移位置
 * 3. 支持多种排序算法对存储的KV按key排序
 * 用于MapReduce本地排序阶段，保存溢出到内存的待排序数据
 */
class MemoryBlock {
private:
  char * _base;               // 内存块起始地址
  uint32_t _size;             // 内存块总大小
  uint32_t _position;         // 当前分配位置指针
  std::vector<uint32_t> _kvOffsets; // 存储所有KV在内存块中的偏移量
  bool _sorted;               // 标记当前内存块中的KV是否已经排序完成

public:
  MemoryBlock(char * pos, uint32_t size);

  /// 获取内存块起始地址
  char * base() {
    return _base;
  }

  /// 获取内存块是否已排序
  bool sorted() {
    return _sorted;
  }

  /**
   * @brief 分配指定大小的KV缓冲区
   * @param length 需要分配的内存大小
   * @return 分配得到的KV缓冲区指针，分配失败返回NULL
   */
  KVBuffer * allocateKVBuffer(uint32_t length) {
    if (length > remainSpace()) {
      LOG("Unable to allocate kv from memory buffer, length: %d, remain: %d", length, remainSpace());
      return NULL;
    }
    // 新分配KV后，内存状态变未排序
    _sorted = false;
    // 记录当前KV偏移
    _kvOffsets.push_back(_position);
    // 计算分配地址
    char * space = _base + _position;
    // 移动分配指针
    _position += length;
    return (KVBuffer *)space;
  }

  /// 获取剩余可用内存大小
  uint32_t remainSpace() const {
    return _size - _position;
  }

  /// 获取当前存储的KV数量
  uint32_t getKVCount() {
    return _kvOffsets.size();
  }

  /// 根据索引获取对应KV缓冲区指针
  KVBuffer * getKVBuffer(uint32_t index);

  /// 按指定排序算法对内存中的KV按key排序
  void sort(SortAlgorithm type, ComparatorPtr comparator);
};
//class MemoryBlock

/**
 * @class MemBlockIterator
 * @brief 内存块迭代器类，用于顺序遍历MemoryBlock中的所有KV
 * 
 * 提供顺序访问接口，用于排序后KV的输出和多路归并阶段遍历各个内存块
 */
class MemBlockIterator {
private:
  MemoryBlock * _memBlock;    // 关联的内存块
  uint32_t _end;              // 遍历结束位置（KV总数）
  uint32_t _current;          // 当前遍历位置索引
  KVBuffer * _kvBuffer;        // 当前指向的KV缓冲区

public:
  /**
   * @brief 构造函数，绑定到指定内存块
   * @param memBlock 需要遍历的内存块
   */
  MemBlockIterator(MemoryBlock * memBlock)
      : _memBlock(memBlock), _end(0), _current(0), _kvBuffer(NULL) {
    _end = memBlock->getKVCount();
  }

  /// 获取当前迭代位置的KV缓冲区
  KVBuffer * getKVBuffer() {
    return _kvBuffer;
  }

  /**
   * @brief 移动到下一个KV
   * @return true 成功移动到下一个KV，false 已经遍历完所有KV
   */
  bool next() {
    if (_current >= _end) {
      return false;
    }
    // 获取当前索引的KV
    this->_kvBuffer = _memBlock->getKVBuffer(_current);
    // 索引自增
    ++_current;
    return true;
  }
};
//class MemoryBlockIterator

/// 内存块迭代器指针类型定义
typedef MemBlockIterator * MemBlockIteratorPtr;

/**
 * @class MemBlockComparator
 * @brief 内存块迭代器比较器类，用于多路归并排序比较不同迭代器当前KV的key大小
 * 
 * 用于优先队列中，选择当前最小的key，支持将空迭代器视为无穷大处理
 */
class MemBlockComparator {
private:
  ComparatorPtr _keyComparator; // 键比较函数指针

public:
  /**
   * @brief 构造函数，使用指定键比较器
   * @param comparator 键比较函数指针
   */
  MemBlockComparator(ComparatorPtr comparator)
      : _keyComparator(comparator) {
  }

public:
  /**
   * @brief 比较两个内存块迭代器当前key的大小
   * @param lhs 左操作数迭代器指针
   * @param rhs 右操作数迭代器指针
   * @return true 如果左迭代器key小于右迭代器key，否则false
   * @note 空迭代器(NULL)被视为无穷大，会被排在后面
   */
  bool operator()(const MemBlockIteratorPtr lhs, const MemBlockIteratorPtr rhs) {

    KVBuffer * left = lhs->getKVBuffer();
    KVBuffer * right = rhs->getKVBuffer();

    // 将空指针视为无穷大，这样空迭代器会优先出堆
    if (NULL == left) {
      return false;
    }

    if (NULL == right) {
      return true;
    }

    return (*_keyComparator)(left->content, left->keyLength, right->content, right->keyLength) < 0;
  }
};

} //namespace NativeTask

#endif /* MEMORYBLOCK_H_ */