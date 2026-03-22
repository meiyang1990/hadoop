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
 * @file Iterator.h
 * @brief MapReduce本地任务迭代器头文件，定义键值分组迭代器实现
 */

#ifndef ITERATOR_H_
#define ITERATOR_H_

#include "NativeTask.h"

namespace NativeTask {

/**
 * @class KeyGroupIteratorImpl
 * @brief 键分组迭代器实现，将底层键值迭代器转换为按键分组的迭代器
 * 
 * 用于Reduce阶段，把排序后的同键值对分组，支持遍历同一个键下的所有值
 * 继承自KeyGroupIterator抽象接口，提供分组迭代能力
 */
class KeyGroupIteratorImpl : public KeyGroupIterator {
protected:
  // 键分组迭代器当前状态
  KeyGroupIterState _keyGroupIterState;
  // 底层键值对迭代器
  KVIterator * _iterator;
  // 当前分组的键存储
  string _currentGroupKey;
  // 当前键缓冲区
  Buffer _key;
  // 当前值缓冲区
  Buffer _value;
  // 是否为第一个键标识
  bool _first;

public:
  /**
   * @brief 构造函数，基于底层键值迭代器构造分组迭代器
   * @param iterator 底层键值对迭代器指针
   */
  KeyGroupIteratorImpl(KVIterator * iterator);

  /**
   * @brief 移动到下一个键分组
   * @return 是否存在下一个键分组，true表示存在，false表示遍历结束
   */
  bool nextKey();

  /**
   * @brief 获取当前分组的键
   * @param len 输出参数，存储键的长度
   * @return 键数据的指针
   */
  const char * getKey(uint32_t & len);

  /**
   * @brief 获取当前键下的下一个值
   * @param len 输出参数，存储值的长度
   * @return 值数据的指针
   */
  const char * nextValue(uint32_t & len);

protected:
  /**
   * @brief 移动到底层迭代器的下一个键值对
   * @return 是否存在下一个键值对
   */
  bool next();
};

} //namespace NativeTask
#endif