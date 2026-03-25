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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 文件级注释：该接口定义了HDFS快照功能中存储和管理INode差异列表的通用规范，
 * 为不同实现方式的差异列表提供统一访问接口，支撑快照差异的高效查找与合并操作。
 *
 * This interface defines the methods used to store and manage InodeDiffs.
 * @param <T> Type of the object in this list.
 */
public interface DiffList<T extends Comparable<Integer>> extends Iterable<T> {
  /** 空差异列表单例实例 */
  DiffList EMPTY_LIST = new DiffListByArrayList(Collections.emptyList());

  /**
   * 获取一个空的DiffList实例
   * Returns an empty DiffList.
   */
  static <T extends Comparable<Integer>> DiffList<T> emptyList() {
    return EMPTY_LIST;
  }

  /**
   * 将给定DiffList包装为不可修改的视图，防止外部修改内部状态
   * Returns an unmodifiable diffList.
   * @param diffs 原始可修改的DiffList
   * @param <T> 列表中元素的类型
   * @return 不可修改的DiffList视图
   */
  static <T extends Comparable<Integer>> DiffList<T> unmodifiableList(
      DiffList<T> diffs) {
    return new DiffList<T>() {
      @Override
      public T get(int i) {
        return diffs.get(i);
      }

      @Override
      public boolean isEmpty() {
        return diffs.isEmpty();
      }

      @Override
      public int size() {
        return diffs.size();
      }

      @Override
      public T remove(int i) {
        throw new UnsupportedOperationException("This list is unmodifiable.");
      }

      @Override
      public boolean addLast(T t) {
        throw new UnsupportedOperationException("This list is unmodifiable.");
      }

      @Override
      public void addFirst(T t) {
        throw new UnsupportedOperationException("This list is unmodifiable.");
      }

      @Override
      public int binarySearch(int i) {
        return diffs.binarySearch(i);
      }

      @Override
      public Iterator<T> iterator() {
        return diffs.iterator();
      }

      @Override
      public List<T> getMinListForRange(int startIndex, int endIndex,
          INodeDirectory dir) {
        return diffs.getMinListForRange(startIndex, endIndex, dir);
      }
    };
  }

  /**
   * 获取列表中指定位置的元素
   *
   * @param index 待返回元素的索引
   * @return 列表中指定位置的元素
   * @throws IndexOutOfBoundsException 如果索引超出范围
   *         (<code>index &lt; 0 || index &gt;= size()</code>)
   */
  T get(int index);

  /**
   * 判断列表是否不包含任何元素
   *
   * @return 如果列表为空返回true，否则返回false
   */
  boolean isEmpty();

  /**
   * 获取列表中元素的数量
   * @return 列表中元素的数量
   */
  int size();

  /**
   * 删除列表中指定位置的元素
   * @param index 待删除元素的索引
   * @return 被删除的元素
   */
  T remove(int index);

  /**
   * 在列表末尾添加一个元素
   * @param t 待添加的元素
   * @return 插入成功返回true
   */
  boolean addLast(T t);

  /**
   * 在列表开头添加一个元素
   * @param t 待添加的元素
   */
  void addFirst(T t);

  /**
   * 使用二分查找算法搜索指定快照ID对应的位置
   * @param key 待搜索的快照ID键
   * @return 如果找到则返回元素索引，否则返回(-插入点 - 1)
   */
  int binarySearch(int key);

  /**
   * 获取在指定索引范围[startIndex, endIndex]内合并差异所需的最少元素列表，
   * 用于快速计算该范围内的累计差异，减少不必要的合并操作
   * @param startIndex 起始差异索引
   * @param endIndex 结束差异索引
   * @param dir 对应的目录INode节点
   * @return 合并指定范围差异所需的最少元素列表
   */
  List<T> getMinListForRange(int startIndex, int endIndex, INodeDirectory dir);

}