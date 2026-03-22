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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 高频使用名称缓存，用于复用重复名称对象，减少内存占用
 * （例如INode中存储的文件名byte[]表示，很多文件名会重复出现）
 * 
 * 工作机制：初始化阶段先添加所有文件名，缓存在临时Map中跟踪每个名称的使用次数
 * 当名称使用次数超过{@code useThreshold}阈值时，将其提升到永久缓存中
 * 
 * 所有名称添加完成后，需要调用{@link #initialized()}完成初始化，
 * 丢弃用于跟踪计数的临时Map，释放内存，缓存进入可用状态
 * 
 * <p>
 * 此类需要外部进行同步控制，不保证线程安全
 * 
 * @param <K> 要缓存的名称类型
 */
class NameCache<K> {
  /**
   * 用于跟踪名称使用次数的内部类
   */
  private class UseCount {
    int count;
    final K value;  // 名称的内部存储值

    UseCount(final K value) {
      count = 1;
      this.value = value;
    }
    
    void increment() {
      count++;
    }
    
    int get() {
      return count;
    }
  }

  static final Logger LOG = LoggerFactory.getLogger(NameCache.class.getName());

  /** 标识缓存是否正在初始化阶段 */
  private boolean initialized = false;

  /** 使用次数阈值，超过该阈值的名称会被放入永久缓存 */
  private final int useThreshold;

  /** 缓存命中成功的总次数 */
  private int lookups = 0;

  /** 永久缓存：存储已提升的高频名称 */
  final HashMap<K, K> cache = new HashMap<K, K>();

  /** 初始化阶段临时存储：跟踪每个名称的出现次数 */
  Map<K, UseCount> transientMap = new HashMap<K, UseCount>();

  /**
   * 构造函数
   * @param useThreshold 提升到永久缓存的使用次数阈值
   */
  NameCache(int useThreshold) {
    this.useThreshold = useThreshold;
  }
  
  /**
   * 将名称放入缓存或跟踪其使用次数，如果名称已在缓存中则返回缓存的内部对象
   * 
   * @param name 要查询的名称
   * @return 如果找到返回缓存的内部对象；否则返回null
   */
  K put(final K name) {
    // 先查询永久缓存
    K internal = cache.get(name);
    if (internal != null) {
      // 缓存命中，计数增加
      lookups++;
      return internal;
    }

    // 仅在初始化阶段跟踪使用次数
    if (!initialized) {
      UseCount useCount = transientMap.get(name);
      if (useCount != null) {
        // 已在临时Map中，使用次数加1
        useCount.increment();
        // 超过阈值则提升到永久缓存
        if (useCount.get() >= useThreshold) {
          promote(name);
        }
        return useCount.value;
      }
      // 首次出现，新增到临时Map
      useCount = new UseCount(name);
      transientMap.put(name, useCount);
    }
    return null;
  }
  
  /**
   * 获取缓存命中成功的总次数
   * @return 缓存命中成功次数
   */
  int getLookupCount() {
    return lookups;
  }

  /**
   * 获取永久缓存中存储的名称数量
   * @return 缓存中名称个数
   */
  int size() {
    return cache.size();
  }

  /**
   * 标记缓存初始化完成，不再跟踪使用次数，清空临时Map释放堆内存
   */
  void initialized() {
    LOG.info("initialized with " + size() + " entries " + lookups + " lookups");
    this.initialized = true;
    transientMap.clear();
    transientMap = null;
  }
  
  /**
   * 将高频使用名称从临时Map提升到永久缓存
   */
  private void promote(final K name) {
    // 从临时Map移除
    transientMap.remove(name);
    // 放入永久缓存
    cache.put(name, name);
    // 累加命中次数
    lookups += useThreshold;
  }

  /**
   * 重置缓存状态，重新进入初始化阶段
   */
  public void reset() {
    initialized = false;
    cache.clear();
    if (transientMap == null) {
      transientMap = new HashMap<K, UseCount>();
    } else {
      transientMap.clear();
    }
  }
}