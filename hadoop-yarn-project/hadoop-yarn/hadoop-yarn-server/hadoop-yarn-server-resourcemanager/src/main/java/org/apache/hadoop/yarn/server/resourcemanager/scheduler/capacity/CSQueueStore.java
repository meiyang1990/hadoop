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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * 容量调度器队列存储容器，支持通过全路径和短名称两种方式查询队列，
 * 处理短名称歧义问题，保证并发读写安全。
 */
public class CSQueueStore {
  // 队列存储真值映射表，以队列全路径为key存储所有队列，是唯一权威数据源
  private final Map<String, CSQueue> fullNameQueues = new HashMap<>();

  // 短名称到全路径列表的映射，存储所有存在该短名称的队列全路径，
  // 用于删除队列后重新处理短名称歧义
  private final Map<String, Set<String>> shortNameToLongNames = new HashMap<>();

  // 查询缓存映射，存储可唯一确定的全路径/无歧义短名称到队列的映射，
  // 用于加速查询，队列增删时会更新该缓存
  private final Map<String, CSQueue> getMap = new HashMap<>();

  // 读写锁，支持并发查询，仅在增删队列时加写锁阻塞修改
  private ReadWriteLock modificationLock = new ReentrantReadWriteLock();

  /**
   * 获取所有队列的不可变映射，以队列全路径为key。
   * @return 包含所有队列的不可变映射
   */
  Map<String, CSQueue> getFullNameQueues() {
    return ImmutableMap.copyOf(fullNameQueues);
  }

  /**
   * 获取所有可通过短名称无歧义访问的队列映射，以短名称为key。
   * @return 包含无歧义短名称队列的不可变映射
   */
  @VisibleForTesting
  Map<String, CSQueue> getShortNameQueues() {
    try {
      modificationLock.readLock().lock();
      return ImmutableMap.copyOf(
          fullNameQueues
              .entrySet()
              .stream()
              // 过滤出缓存命中且对应到当前队列的无歧义短名称
              .filter(
                  entry -> getMap.get(entry.getValue().getQueueShortName())
                      == entry.getValue())
              .collect(
                  Collectors.toMap(
                      entry->entry.getValue().getQueueShortName(),
                      entry->entry.getValue()))
      );
    } finally {
      modificationLock.readLock().unlock();
    }
  }

  /**
   * 根据当前短名称对应的队列数量，更新查询缓存中的短名称映射。
   * 如果只有一个队列则缓存该映射，多个则移除缓存表示歧义。
   * @param shortName 需要更新的短名称
   */
  private void updateGetMapForShortName(String shortName) {
    // root队列特殊处理，root始终作为全路径，不加入短名称缓存
    if (shortName.equals(CapacitySchedulerConfiguration.ROOT)) {
      return;
    }
    // 获取该短名称对应的所有队列全路径
    Set<String> fullNames = this.shortNameToLongNames.get(shortName);

    // 只有一个队列时，将短名称加入缓存
    if (fullNames != null && fullNames.size() == 1) {
      getMap.put(shortName,
          fullNameQueues.get(fullNames.iterator().next()));
    } else {
      // 多个队列时，移除缓存表示无法通过短名称唯一确定
      getMap.remove(shortName);
    }
  }

  /**
   * 添加队列到存储，更新全路径映射、短名称歧义映射和查询缓存。
   * @param queue 待添加的队列
   */
  public void add(CSQueue queue) {
    String fullName = queue.getQueuePath();
    String shortName = queue.getQueueShortName();

    try {
      modificationLock.writeLock().lock();

      // 更新全路径映射和查询缓存
      fullNameQueues.put(fullName, queue);
      getMap.put(fullName, queue);

      // 非root队列需要更新短名称歧义映射
      if (!shortName.equals(CapacitySchedulerConfiguration.ROOT)) {
        // 获取或创建该短名称对应的全路径集合
        Set<String> fullNamesSet =
            this.shortNameToLongNames.getOrDefault(shortName, new HashSet<>());

        // 添加当前队列全路径到集合
        fullNamesSet.add(fullName);
        this.shortNameToLongNames.put(shortName, fullNamesSet);
      }

      // 更新短名称查询缓存
      updateGetMapForShortName(shortName);
    } finally {
      modificationLock.writeLock().unlock();
    }
  }

  /**
   * 从存储中移除指定队列，更新所有映射关系。
   * @param queue 待移除的队列
   */
  public void remove(CSQueue queue) {
    // 空队列直接返回，保持和HashMap行为一致
    if (queue == null) {
      return;
    }
    try {
      modificationLock.writeLock().lock();

      String fullName = queue.getQueuePath();
      String shortName = queue.getQueueShortName();

      // 从全路径映射和查询缓存中移除
      fullNameQueues.remove(fullName);
      getMap.remove(fullName);

      // 非root队列需要更新短名称歧义映射
      if (!shortName.equals(CapacitySchedulerConfiguration.ROOT)) {
        Set<String> fullNamesSet = this.shortNameToLongNames.get(shortName);
        fullNamesSet.remove(fullName);
        // 集合为空则移除整个短名称条目释放内存
        if (fullNamesSet.size() == 0) {
          this.shortNameToLongNames.remove(shortName);
        }
      }

      // 更新短名称查询缓存
      updateGetMapForShortName(shortName);

    } finally {
      modificationLock.writeLock().unlock();
    }
  }

  /**
   * 根据名称从存储中移除队列，支持全路径或短名称。
   * @param name 待移除队列的名称
   */
  public void remove(String name) {
    CSQueue queue = get(name);
    if (queue != null) {
      remove(queue);
    }
  }

  /**
   * 通过全路径查询队列。
   * @param fullName 队列全路径
   * @return 队列实例，不存在则返回null
   */
  CSQueue getByFullName(String fullName) {
    if (fullName == null) {
      return null;
    }

    try {
      modificationLock.readLock().lock();
      return fullNameQueues.getOrDefault(fullName, null);
    } finally {
      modificationLock.readLock().unlock();
    }
  }

  /**
   * 检查指定短名称是否存在歧义（对应两个及以上队列）。
   * @param shortName 待检查的短名称
   * @return 存在歧义返回true，否则返回false
   */
  boolean isAmbiguous(String shortName) {
    if (shortName == null) {
      return false;
    }

    boolean ret = true;
    try {
      modificationLock.readLock().lock();
      Set<String> fullNamesSet = this.shortNameToLongNames.get(shortName);

      if (fullNamesSet == null || fullNamesSet.size() <= 1) {
        ret = false;
      }
    } finally {
      modificationLock.readLock().unlock();
    }

    return ret;
  }

  /**
   * 通过名称查询队列，同时支持全路径和无歧义短名称。
   * @param name 队列名称（全路径或短名称）
   * @return 队列实例，不存在或歧义则返回null
   */
  public CSQueue get(String name) {
    if (name == null) {
      return null;
    }
    try {
      modificationLock.readLock().lock();
      return getMap.getOrDefault(name, null);
    } finally {
      modificationLock.readLock().unlock();
    }
  }

  /**
   * 清空存储，移除所有队列引用。
   */
  public void clear() {
    try {
      modificationLock.writeLock().lock();
      fullNameQueues.clear();
      shortNameToLongNames.clear();
      getMap.clear();
    } finally {
      modificationLock.writeLock().unlock();
    }
  }

  /**
   * 获取所有队列的不可变集合。
   * @return 包含所有队列的不可变列表
   */
  public Collection<CSQueue> getQueues() {
    try {
      modificationLock.readLock().lock();
      return ImmutableList.copyOf(fullNameQueues.values());
    } finally {
      modificationLock.readLock().unlock();
    }
  }
}