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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * 基于内存的时间线存储服务实现，仅用于测试目的。
 * 所有方法都做了同步处理，避免内存数据并发修改问题。
 * 错误实例化可能导致读写操作访问不同内存存储实例，因此不建议在生产环境使用。
 */
@Private
@Unstable
public class MemoryTimelineStore extends KeyValueBasedTimelineStore {

  /**
   * 基于HashMap的时间线存储适配器，实现TimelineStoreMapAdapter接口
   * 封装HashMap基础操作，提供按值范围迭代能力
   * @param <K> 键类型
   * @param <V> 值类型
   */
  static class HashMapStoreAdapter<K, V>
      implements TimelineStoreMapAdapter<K, V> {
    Map<K, V> internalMap = new HashMap<>();

    @Override
    public V get(K key) {
      return internalMap.get(key);
    }

    @Override
    public void put(K key, V value) {
      internalMap.put(key, value);
    }

    @Override
    public void remove(K key) {
      internalMap.remove(key);
    }

    @Override
    public CloseableIterator<V>
    valueSetIterator() {
      // 将所有值排序后返回迭代器
      return wrapClosableIterator(new TreeSet<>(internalMap.values())
          .iterator());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CloseableIterator<V> valueSetIterator(V minV) {
      // 如果值支持比较，只返回大于等于最小值的结果
      if (minV instanceof Comparable) {
        TreeSet<V> tempTreeSet = new TreeSet<>();
        for (V value : internalMap.values()) {
          if (((Comparable) value).compareTo(minV) >= 0) {
            tempTreeSet.add(value);
          }
        }
        return wrapClosableIterator(tempTreeSet.iterator());
      } else {
        // 不支持比较则返回所有值
        return valueSetIterator();
      }
    }

    /**
     * 将原生迭代器包装为可关闭迭代器
     * @param iterator 原生迭代器
     * @return 可关闭迭代器实例
     */
    private CloseableIterator<V> wrapClosableIterator(
        final Iterator<V> iterator) {
      return new CloseableIterator<V>() {
        private final Iterator<V> internalIterator = iterator;
        @Override
        public void close() throws IOException {
          // Not implemented
        }

        @Override
        public boolean hasNext() {
          return internalIterator.hasNext();
        }

        @Override
        public V next() {
          return internalIterator.next();
        }

        @Override
        public void remove() {
          internalIterator.remove();
        }
      };

    }
  }

  /**
   * 默认构造函数，使用类名作为存储名称
   */
  public MemoryTimelineStore() {
    this(MemoryTimelineStore.class.getName());
  }

  /**
   * 带名称的构造函数，初始化各类存储容器
   * @param name 存储名称
   */
  public MemoryTimelineStore(String name) {
    super(name);
    entities = new HashMapStoreAdapter<>();
    entityInsertTimes = new HashMapStoreAdapter<>();
    domainById = new HashMapStoreAdapter<>();
    domainsByOwner = new HashMapStoreAdapter<>();
  }

}