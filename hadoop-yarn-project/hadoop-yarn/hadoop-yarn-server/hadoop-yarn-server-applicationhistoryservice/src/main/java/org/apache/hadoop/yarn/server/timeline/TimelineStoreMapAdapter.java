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

package org.apache.hadoop.yarn.server.timeline;

import java.io.Closeable;
import java.util.Iterator;

/**
 * 时间线存储的Map结构适配器接口，为不同底层Map实现提供统一访问接口
 * 应用历史服务中用于抽象存储层映射结构，解耦具体实现与上层业务逻辑
 * @param <K> 键类型
 * @param <V> 值类型
 */
interface TimelineStoreMapAdapter<K, V> {
  /**
   * 根据键获取对应的值
   * @param key 键
   * @return 键对应的值，不存在则返回null
   */
  V get(K key);

  /**
   * 向映射中添加键值对
   * @param key 键
   * @param value 值
   */
  void put(K key, V value);

  /**
   * 从映射中移除指定键的映射关系
   * @param keyToRemove 要移除的键
   */
  void remove(K keyToRemove);

  /**
   * 获取值集合的迭代器
   * @return 支持关闭的迭代器实例
   */
  CloseableIterator<V> valueSetIterator();

  /**
   * 获取从指定最小值开始的值集合迭代器，仅当V实现Comparable接口时生效
   * @param minV 起始最小值，迭代只返回大于等于该值的元素
   * @return 支持关闭的范围迭代器实例
   */
  CloseableIterator<V> valueSetIterator(V minV);

  /**
   * 扩展Iterator接口，增加Closeable能力，支持资源清理
   * @param <V> 迭代元素类型
   */
  interface CloseableIterator<V> extends Iterator<V>, Closeable {}
}