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
 * @file MemoryPool.h
 * 本文件属于Hadoop MapReduce本地任务运行模块，提供预分配内存池实现，用于Map输出数据的批量内存分配
 */

#ifndef MEMORYPOOL_H_
#define MEMORYPOOL_H_

#include "lib/Buffers.h"
#include "lib/MapOutputSpec.h"
#include "NativeTask.h"
#include "util/StringUtil.h"

namespace NativeTask {

/**
 * @class MemoryPool
 * @brief 预分配式内存池，为MapReduce本地任务提供快速批量内存分配，减少碎片化malloc调用
 * 
 * 核心职责：提前预分配一块连续的大块内存，运行时从这块内存中快速分配小块内存，
 * 用于存放Map阶段输出的中间数据，提升内存分配效率，减少内存碎片
 */
class MemoryPool {
private:
  char * _base;
  uint32_t _capacity;
  uint32_t _used;

public:

  /**
   * @brief 构造函数，初始化空内存池
   */
  MemoryPool()
      : _base(NULL), _capacity(0), _used(0) {
  }

  /**
   * @brief 析构函数，释放预分配的内存块
   */
  ~MemoryPool() {
    if (NULL != _base) {
      free(_base);
      _base = NULL;
    }
  }

  /**
   * @brief 初始化内存池，分配指定容量的连续内存块
   * @param capacity 需要分配的内存池总容量（字节）
   * @throws OutOfMemoryException 当系统内存不足分配失败时抛出异常
   */
  void init(uint32_t capacity) throw (OutOfMemoryException) {
    // 如果已有内存容量不足，重新分配更大内存块
    if (capacity > _capacity) {
      if (NULL != _base) {
        free(_base);
        _base = NULL;
      }
      _base = (char*)malloc(capacity);
      if (NULL == _base) {
        THROW_EXCEPTION(OutOfMemoryException, "Not enough memory to init MemoryBlockPool");
      }
      _capacity = capacity;
    }
    // 重置已使用内存指针
    reset();
  }

  /**
   * @brief 重置内存池，清空已使用记录，复用整个内存块
   */
  void reset() {
    _used = 0;
  }

  /**
   * @brief 从内存池中分配指定范围大小的内存
   * @param min 最小需要分配的内存大小（字节），不能小于该值
   * @param expect 期望分配的内存大小（字节），尽量分配该大小
   * @param[out] allocated 实际分配得到的内存大小
   * @return 分配得到的内存块起始地址，内存不足时返回NULL
   */
  char * allocate(uint32_t min, uint32_t expect, uint32_t & allocated) {
    // 连最小需求都无法满足，分配失败
    if (_used + min > _capacity) {
      return NULL;
    } 
    // 无法满足期望大小，只分配最小需求
    else if (_used + expect > _capacity) {
      char * buff = _base + _used;
      allocated = min;
      _used += min;
      return buff;
    } 
    // 满足期望大小，分配期望大小
    else {
      char * buff = _base + _used;
      allocated = expect;
      _used += expect;
      return buff;
    }
  }
};

} // namespace NativeTask

#endif /* MEMORYPOOL_H_ */