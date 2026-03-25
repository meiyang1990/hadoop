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
package org.apache.hadoop.hdfs.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;

/**
 * HDFS实例去重与引用计数工具类。<br>
 * 维护每个实例的引用计数，当引用计数降为0时自动从映射中移除该实例。<br>
 * 存储的元素类型必须实现{@link ReferenceCounter}接口维护自身计数<br>
 * 注意：该类本身不是线程安全的（依赖底层ConcurrentHashMap保证并发安全性）
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReferenceCountMap<E extends ReferenceCountMap.ReferenceCounter> {

  // 存储实例引用映射，键值均为实例本身，用于去重
  private Map<E, E> referenceMap = new ConcurrentHashMap<>();

  /**
   * 添加一个实例引用。如果实例已存在，仅增加其引用计数；不存在则插入并初始化计数。
   * 
   * @param key 要添加引用的实例
   * @return 去重后的实际引用实例（已存在则返回原实例，不存在则返回新实例）
   */
  public E put(E key) {
    E value = referenceMap.putIfAbsent(key, key);
    if (value == null) {
      value = key;
    }
    value.incrementAndGetRefCount();
    return value;
  }

  /**
   * 移除一个实例引用。减少实例的引用计数，当引用计数降为0时，从映射中彻底删除该实例。
   * 
   * @param key 要移除引用的实例
   */
  public void remove(E key) {
    E value = referenceMap.get(key);
    if (value != null && value.decrementAndGetRefCount() == 0) {
      referenceMap.remove(key);
    }
  }

  /**
   * 获取当前映射中所有存活实例的不可变列表，仅用于测试。
   * 
   * @return 所有存活实例的不可变列表
   */
  @VisibleForTesting
  public ImmutableList<E> getEntries() {
    return new ImmutableList.Builder<E>().addAll(referenceMap.keySet()).build();
  }

  /**
   * 获取指定实例的当前引用计数
   * @param key 要查询的实例
   * @return 实例当前引用计数，实例不存在则返回0
   */
  public long getReferenceCount(E key) {
    ReferenceCounter counter = referenceMap.get(key);
    if (counter != null) {
      return counter.getRefCount();
    }
    return 0;
  }

  /**
   * 获取当前映射中存活的唯一实例总数
   * @return 唯一实例数量
   */
  public int getUniqueElementsSize() {
    return referenceMap.size();
  }

  /**
   * 清空映射中所有内容，仅用于测试
   */
  @VisibleForTesting
  public void clear() {
    referenceMap.clear();
  }

  /**
   * 引用计数持有者接口，定义维护引用计数的标准方法
   */
  public static interface ReferenceCounter {
    /**
     * 获取当前引用计数
     * @return 当前引用计数值
     */
    public int getRefCount();

    /**
     * 增加引用计数并返回新值
     * @return 增加后的引用计数值
     */
    public int incrementAndGetRefCount();

    /**
     * 减少引用计数并返回新值
     * @return 减少后的引用计数值
     */
    public int decrementAndGetRefCount();
  }
}