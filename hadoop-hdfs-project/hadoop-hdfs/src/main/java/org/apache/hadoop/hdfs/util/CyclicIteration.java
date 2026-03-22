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

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 为NavigableMap提供循环迭代器，实现从起点出发遍历所有元素后回到起点的环形遍历
 * 按照导航映射的排序规则遍历，当到达最后一个元素后自动从第一个元素继续遍历
 * 常用于HDFS中需要环形轮询节点、数据块等场景
 * @param <K> 映射的键类型
 * @param <V> 映射的值类型
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CyclicIteration<K, V> implements Iterable<Map.Entry<K, V>> {
  private final NavigableMap<K, V> navigablemap;
  private final NavigableMap<K, V> tailmap;

  /**
   * 构造循环迭代对象，从指定起始键的下一个元素开始遍历
   * @param navigablemap 待遍历的导航映射
   * @param startingkey 遍历起始键，遍历从该键之后第一个元素开始（不包含该键本身）
   */
  public CyclicIteration(NavigableMap<K, V> navigablemap, K startingkey) {
    if (navigablemap == null || navigablemap.isEmpty()) {
      this.navigablemap = null;
      this.tailmap = null;
    }
    else {
      this.navigablemap = navigablemap;
      this.tailmap = navigablemap.tailMap(startingkey, false); 
    }
  }

  @Override
  public Iterator<Map.Entry<K, V>> iterator() {
    return new CyclicIterator();
  }

  /**
   * 实现循环迭代逻辑的内部迭代器类
   */
  private class CyclicIterator implements Iterator<Map.Entry<K, V>> {
    private boolean hasnext;
    private Iterator<Map.Entry<K, V>> i;
    /** 遍历开始的第一个条目，用于判断是否已经遍历完一轮 */
    private final Map.Entry<K, V> first;
    /** 下一个待返回的条目 */
    private Map.Entry<K, V> next;
    
    private CyclicIterator() {
      hasnext = navigablemap != null;
      if (hasnext) {
        // 从起始键后的尾部映射开始创建迭代器
        i = tailmap.entrySet().iterator();
        // 获取第一个遍历元素
        first = nextEntry();
        next = first;
      }
      else {
        i = null;
        first = null;
        next = null;
      }
    }

    /**
     * 获取下一个元素，当前迭代器遍历完后自动从头部重新开始
     * @return 下一个映射条目
     */
    private Map.Entry<K, V> nextEntry() {
      // 当前迭代器遍历完，重置到映射头部重新开始
      if (!i.hasNext()) {
        i = navigablemap.entrySet().iterator();
      }
      return i.next();
    }

    @Override
    public boolean hasNext() {
      return hasnext;
    }

    @Override
    public Map.Entry<K, V> next() {
      if (!hasnext) {
        throw new NoSuchElementException();
      }

      final Map.Entry<K, V> curr = next;
      next = nextEntry();
      // 当回到起始元素时，说明已经完成一轮遍历，结束迭代
      hasnext = !next.equals(first);
      return curr;
    }

    /** 不支持删除操作 */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported");
    }
  }
}