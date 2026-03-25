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

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * 文件：org.apache.hadoop.hdfs.util.LightWeightLinkedSet
 * 低内存占用的有序链式哈希集合实现，使用数组存储元素，链表解决哈希冲突。
 * 额外维护一个全元素双向链表保证插入顺序遍历，不支持null元素。
 * 该类非线程安全。
 */
public class LightWeightLinkedSet<T> extends LightWeightHashSet<T> {
  /**
   * 双向链表节点，用于维护全元素的插入顺序链表
   */
  static class DoubleLinkedElement<T> extends LinkedElement<T> {
    // 全元素链表中当前节点的前驱和后继引用
    private DoubleLinkedElement<T> before;
    private DoubleLinkedElement<T> after;

    public DoubleLinkedElement(T elem, int hashCode) {
      super(elem, hashCode);
      this.before = null;
      this.after = null;
    }

    @Override
    public String toString() {
      return super.toString();
    }
  }

  // 全元素双向链表的头节点
  private DoubleLinkedElement<T> head;
  // 全元素双向链表的尾节点
  private DoubleLinkedElement<T> tail;

  // 迭代器书签，用于记录断点位置，支持分段遍历
  private LinkedSetIterator bookmark;

  /**
   * 构造函数，指定初始容量、加载因子阈值
   * @param initCapacity 内部数组的推荐初始容量
   * @param maxLoadFactor 扩容触发的最大加载因子
   * @param minLoadFactor 缩容触发的最小加载因子
   */
  public LightWeightLinkedSet(int initCapacity, float maxLoadFactor,
      float minLoadFactor) {
    super(initCapacity, maxLoadFactor, minLoadFactor);
    head = null;
    tail = null;
    bookmark = new LinkedSetIterator();
  }

  /**
   * 默认构造函数，使用默认参数初始化
   */
  public LightWeightLinkedSet() {
    this(MINIMUM_CAPACITY, DEFAULT_MAX_LOAD_FACTOR, DEFAUT_MIN_LOAD_FACTOR);
  }

  /**
   * 将指定元素添加到哈希表中
   * @param element 待添加的元素
   * @return true 元素不存在添加成功；false 元素已存在添加失败
   */
  @Override
  protected boolean addElem(final T element) {
    // 检查元素非空
    if (element == null) {
      throw new IllegalArgumentException("Null element is not supported.");
    }
    // 计算哈希值和数组索引
    final int hashCode = element.hashCode();
    final int index = getIndex(hashCode);
    // 元素已存在直接返回false
    if (getContainedElem(index, element, hashCode) != null) {
      return false;
    }

    // 修改计数递增，集合大小递增
    modification++;
    size++;

    // 更新哈希桶链表
    DoubleLinkedElement<T> le = new DoubleLinkedElement<T>(element, hashCode);
    le.next = entries[index];
    entries[index] = le;

    // 将新节点插入到全元素双向链表尾部
    le.after = null;
    le.before = tail;
    if (tail != null) {
      tail.after = le;
    }
    tail = le;
    if (head == null) {
      head = le;
      bookmark.next = head;
    }

    // 如果书签当前指向null，更新书签指向新添加的元素
    if (bookmark.next == null) {
      bookmark.next = le;
    }
    return true;
  }

  /**
   * 根据元素key删除对应节点
   * @param key 待删除的元素
   * @return 删除成功返回对应节点，不存在返回null
   */
  @Override
  protected DoubleLinkedElement<T> removeElem(final T key) {
    DoubleLinkedElement<T> found = (DoubleLinkedElement<T>) (super
        .removeElem(key));
    if (found == null) {
      return null;
    }

    // 更新全元素双向链表，移除当前节点
    if (found.after != null) {
      found.after.before = found.before;
    }
    if (found.before != null) {
      found.before.after = found.after;
    }
    if (head == found) {
      head = head.after;
    }
    if (tail == found) {
      tail = tail.before;
    }

    // 如果删除的是书签指向的节点，书签后移到下一个节点
    if (found == this.bookmark.next) {
      this.bookmark.next = found.after;
    }
    return found;
  }

  /**
   * 弹出并返回链表中的第一个元素（按插入顺序最先插入）
   * @return 第一个元素，集合为空返回null
   */
  public T pollFirst() {
    if (head == null) {
      return null;
    }
    T first = head.element;
    this.remove(first);
    return first;
  }

  /**
   * 弹出n个元素，按插入顺序弹出最先插入的元素
   * @param n 需要弹出的元素数量
   * @return 弹出的元素列表，按插入顺序排序
   */
  @Override
  public List<T> pollN(int n) {
    if (n >= size) {
      // 需要弹出所有元素，使用更快的全量弹出实现
      return pollAll();
    }
    List<T> retList = new ArrayList<T>(n);
    while (n-- > 0 && head != null) {
      T curr = head.element;
      this.removeElem(curr);
      retList.add(curr);
    }
    // 根据需要缩容内部数组
    shrinkIfNecessary();
    return retList;
  }

  /**
   * 弹出所有元素并按插入顺序返回，直接遍历链表实现，比父类方法更快
   * @return 所有元素按插入顺序组成的列表
   */
  @Override
  public List<T> pollAll() {
    List<T> retList = new ArrayList<T>(size);
    while (head != null) {
      retList.add(head.element);
      head = head.after;
    }
    this.clear();
    return retList;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <U> U[] toArray(U[] a) {
    if (a == null) {
      throw new NullPointerException("Input array can not be null");
    }
    if (a.length < size) {
      // 数组长度不足，反射创建新数组
      a = (U[]) java.lang.reflect.Array.newInstance(a.getClass()
          .getComponentType(), size);
    }
    int currentIndex = 0;
    // 按插入顺序遍历链表填充数组
    DoubleLinkedElement<T> current = head;
    while (current != null) {
      T curr = current.element;
      a[currentIndex++] = (U) curr;
      current = current.after;
    }
    return a;
  }

  @Override
  public Iterator<T> iterator() {
    return new LinkedSetIterator();
  }

  /**
   * 按插入顺序遍历集合的迭代器实现，支持fail-fast机制
   */
  private class LinkedSetIterator implements Iterator<T> {
    /** 初始修改计数，用于fail-fast检查 */
    private final int startModification = modification;
    /** 下一个待返回的元素节点 */
    private DoubleLinkedElement<T> next = head;

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public T next() {
      // 集合被修改，抛出并发修改异常
      if (modification != startModification) {
        throw new ConcurrentModificationException("modification="
            + modification + " != startModification = " + startModification);
      }
      // 已经没有更多元素，抛出异常
      if (next == null) {
        throw new NoSuchElementException();
      }
      final T e = next.element;
      // 移动指针到下一个元素
      next = next.after;
      return e;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported.");
    }
  }

  /**
   * 清空集合，将内部数组重置为初始容量
   */
  @Override
  public void clear() {
    super.clear();
    this.head = null;
    this.tail = null;
    this.resetBookmark();
  }

  /**
   * 获取从书签位置开始的新迭代器，并更新书签为当前迭代器位置
   * @return 从当前书签位置开始的迭代器
   */
  public Iterator<T> getBookmark() {
    LinkedSetIterator toRet = new LinkedSetIterator();
    toRet.next = this.bookmark.next;
    this.bookmark = toRet;
    return toRet;
  }

  /**
   * 将书签重置到链表头部
   */
  public void resetBookmark() {
    this.bookmark.next = this.head;
  }
}