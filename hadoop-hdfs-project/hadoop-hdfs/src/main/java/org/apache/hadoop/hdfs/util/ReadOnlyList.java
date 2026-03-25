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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 只读列表接口，仅支持读取操作，不允许修改列表内容
 * 
 * @param <E> 列表元素类型
 */
@InterfaceAudience.Private
public interface ReadOnlyList<E> extends Iterable<E> {
  /**
   * 判断列表是否为空
   * @return 为空返回true，否则返回false
   */
  boolean isEmpty();

  /**
   * 获取列表元素数量
   * @return 列表大小
   */
  int size();

  /**
   * 获取指定索引位置的元素
   * @param i 索引位置
   * @return 对应索引的元素
   */
  E get(int i);
  
  /**
   * 为ReadOnlyList提供工具方法的静态工具类
   */
  public static class Util {
    /**
     * 获取一个空的只读列表
     * @return 空只读列表
     */
    public static <E> ReadOnlyList<E> emptyList() {
      return ReadOnlyList.Util.asReadOnlyList(Collections.<E>emptyList());
    }

    /**
     * 对已排序的只读列表执行二分查找，逻辑同{@link Collections#binarySearch(List, Object)}
     * @param list 已排序的只读列表
     * @param key 待查找的键
     * @return 查找结果，找到返回元素索引，没找到返回插入点，规则同{@link Collections#binarySearch(List, Object)}
     */
    public static <K, E extends Comparable<K>> int binarySearch(
        final ReadOnlyList<E> list, final K key) {
      int lower = 0;
      for(int upper = list.size() - 1; lower <= upper; ) {
        // 计算中间点，使用无符号右移避免溢出
        final int mid = (upper + lower) >>> 1;

        final int d = list.get(mid).compareTo(key);
        if (d == 0) {
          // 找到目标元素，返回索引
          return mid;
        } else if (d > 0) {
          // 中间元素大于目标，收缩上边界
          upper = mid - 1;
        } else {
          // 中间元素小于目标，收缩下边界
          lower = mid + 1;
        }
      }
      // 未找到目标，返回插入点
      return -(lower + 1);
    }

    /**
     * 将普通List包装为ReadOnlyList视图
     * @param list 原List
     * @return 基于原List的只读视图
     */
    public static <E> ReadOnlyList<E> asReadOnlyList(final List<E> list) {
      return new ReadOnlyList<E>() {
        @Override
        public Iterator<E> iterator() {
          return list.iterator();
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
        public E get(int i) {
          return list.get(i);
        }

        @Override
        public String toString() {
          return list.toString();
        }
      };
    }

    /**
     * 将ReadOnlyList转换为List视图，所有修改操作都会抛出不支持异常
     * @param list 原只读列表
     * @return 仅支持读取的List视图
     */
    public static <E> List<E> asList(final ReadOnlyList<E> list) {
      return new List<E>() {
        @Override
        public Iterator<E> iterator() {
          return list.iterator();
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
        public E get(int i) {
          return list.get(i);
        }

        @Override
        public Object[] toArray() {
          final Object[] a = new Object[size()];
          for(int i = 0; i < a.length; i++) {
            a[i] = get(i);
          }
          return a;
        }

        //All methods below are not supported.

        @Override
        public boolean add(E e) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void add(int index, E element) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(int index, Collection<? extends E> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public int indexOf(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public int lastIndexOf(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public ListIterator<E> listIterator() {
          throw new UnsupportedOperationException();
        }

        @Override
        public ListIterator<E> listIterator(int index) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
          throw new UnsupportedOperationException();
        }

        @Override
        public E remove(int index) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
          throw new UnsupportedOperationException();
        }

        @Override
        public E set(int index, E element) {
          throw new UnsupportedOperationException();
        }

        @Override
        public List<E> subList(int fromIndex, int toIndex) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <T> T[] toArray(T[] a) {
          throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
          if (list.isEmpty()) {
            return "[]";
          }
          final Iterator<E> i = list.iterator();
          final StringBuilder b = new StringBuilder("[").append(i.next());
          for(; i.hasNext();) {
            b.append(", ").append(i.next());
          }
          return b + "]";
        }
      };
    }
  }
}