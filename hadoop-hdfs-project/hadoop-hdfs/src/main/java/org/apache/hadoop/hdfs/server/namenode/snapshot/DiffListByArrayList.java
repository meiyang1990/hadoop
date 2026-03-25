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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/snapshot/DiffListByArrayList.java
 * <p>
 * 基于ArrayList实现的可扩容差异列表，用于存储HDFS快照的inode差异信息
 * 实现了DiffList接口，提供基于数组的差异列表存储与查询能力
 * @param <T> 列表中存储的对象类型，需要支持基于整数索引的比较
 */
public class DiffListByArrayList<T extends Comparable<Integer>>
    implements DiffList<T> {
  private final List<T> list;

  /**
   * 构造函数，基于已有列表创建差异列表包装对象
   * @param list 已有存储差异数据的列表
   */
  DiffListByArrayList(List<T> list) {
    this.list = list;
  }

  /**
   * 构造函数，创建指定初始容量的空差异列表
   * @param initialCapacity 初始容量
   */
  public DiffListByArrayList(int initialCapacity) {
    this(new ArrayList<>(initialCapacity));
  }

  @Override
  public T get(int i) {
    return list.get(i);
  }

  @Override
  public boolean isEmpty() {
    return list.isEmpty();
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public T remove(int i) {
    // DeletionOrdered模式下只允许删除第一个元素（索引0）
    assert !SnapshotManager.isDeletionOrdered() || i == 0;
    return list.remove(i);
  }

  @Override
  public boolean addLast(T t) {
    return list.add(t);
  }

  @Override
  public void addFirst(T t) {
    list.add(0, t);
  }

  @Override
  public int binarySearch(int i) {
    return Collections.binarySearch(list, i);
  }

  @Override
  public Iterator<T> iterator() {
    return list.iterator();
  }

  /**
   * 获取指定索引范围内子列表，用于获取目录指定范围的最小差异集合
   * @param startIndex 起始索引（包含）
   * @param endIndex 结束索引（不包含）
   * @param dir 目标目录节点
   * @return 索引对应的子列表
   */
  @Override
  public List<T> getMinListForRange(int startIndex, int endIndex,
      INodeDirectory dir) {
    return list.subList(startIndex, endIndex);
  }
}