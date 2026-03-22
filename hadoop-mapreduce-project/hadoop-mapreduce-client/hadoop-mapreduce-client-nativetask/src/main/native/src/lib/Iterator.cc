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
 * @file Iterator.cc
 * 原生MapReduce任务键值分组迭代器实现
 * 为MapReduce shuffle阶段提供按键分组迭代能力，支持相同键的多个值遍历
 */

#include "lib/Iterator.h"
#include "lib/commons.h"

namespace NativeTask {

/**
 * 键分组迭代器实现类构造函数
 * 包装基础键值对迭代器，实现按键分组遍历功能
 * @param iterator 基础键值对迭代器
 */
KeyGroupIteratorImpl::KeyGroupIteratorImpl(KVIterator * iterator)
    : _keyGroupIterState(NEW_KEY), _iterator(iterator), _first(true) {
}

/**
 * 移动到下一个键分组，准备遍历当前键的所有值
 * @return true 存在下一个键分组；false 所有分组遍历完成
 */
bool KeyGroupIteratorImpl::nextKey() {
  // 已经没有更多分组，直接返回
  if (_keyGroupIterState == NO_MORE) {
    return false;
  }

  uint32_t temp;
  // 消费完当前分组剩余的所有值，直到遇到下一个新键
  while (_keyGroupIterState == SAME_KEY || _keyGroupIterState == NEW_KEY_VALUE) {
    nextValue(temp);
  }
  // 此时已经定位到新键位置，准备返回新分组
  if (_keyGroupIterState == NEW_KEY) {
    // 处理第一个分组的初始化
    if (_first == true) {
      _first = false;
      // 获取第一个键值对
      if (!next()) {
        _keyGroupIterState = NO_MORE;
        return false;
      }
    }
    // 更新状态为新键待返回第一个值
    _keyGroupIterState = NEW_KEY_VALUE;
    // 保存当前分组键，用于后续比较相同键
    _currentGroupKey.assign(_key.data(), _key.length());
    return true;
  }
  return false;
}

/**
 * 获取当前分组的键
 * @param len 输出参数，存储键长度
 * @return 键数据指针
 */
const char * KeyGroupIteratorImpl::getKey(uint32_t & len) {
  len = (uint32_t)_key.length();
  return _key.data();
}

/**
 * 获取当前键分组的下一个值
 * @param len 输出参数，存储值长度
 * @return 值数据指针，没有更多值时返回NULL
 */
const char * KeyGroupIteratorImpl::nextValue(uint32_t & len) {
  switch (_keyGroupIterState) {
  // 尚未定位到有效分组，返回空
  case NEW_KEY: {
    return NULL;
  }
  // 当前分组已经返回过至少一个值，继续尝试获取下一个值
  case SAME_KEY: {
    // 获取下一个键值对
    if (next()) {
      // 键长度相同且内容相等，说明是同组的新值
      if (_key.length() == _currentGroupKey.length()) {
        if (fmemeq(_key.data(), _currentGroupKey.c_str(), _key.length())) {
          len = _value.length();
          return _value.data();
        }
      }
      // 遇到不同键，更新状态，当前分组遍历结束
      _keyGroupIterState = NEW_KEY;
      return NULL;
    }
    // 所有键值对遍历完成
    _keyGroupIterState = NO_MORE;
    return NULL;
  }
  // 新分组，返回第一个值
  case NEW_KEY_VALUE: {
    // 更新状态为当前键需要继续找下一个值
    _keyGroupIterState = SAME_KEY;
    len = _value.length();
    return _value.data();
  }
  // 遍历完成
  case NO_MORE:
    return NULL;
  }
  return NULL;
}

/**
 * 从底层迭代器获取下一个键值对
 * @return true 获取成功；false 遍历完成
 */
bool KeyGroupIteratorImpl::next() {
  bool result = _iterator->next(_key, _value);
  return result;
}

} // namespace NativeTask