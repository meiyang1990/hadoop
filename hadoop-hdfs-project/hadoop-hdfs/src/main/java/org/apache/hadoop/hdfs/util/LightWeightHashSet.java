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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @file LightWeightHashSet.java
 * 低内存占用的链式哈希集合实现，使用数组存储元素，链表解决哈希冲突。不支持null元素。
 * 该实现非线程安全，适用于对内存占用敏感的场景。
 */
public class LightWeightHashSet<T> implements Collection<T> {
  /**
   * 哈希表中的链表节点，存储实际元素和哈希值、下一个节点引用
   * @param <T> 存储的元素类型
   */
  static class LinkedElement<T> {
    protected final T element;

    // 当前哈希桶链表中下一个节点的引用
    protected LinkedElement<T> next;

    // 存储元素的哈希值，避免重复计算
    protected final int hashCode;

    /**
     * 构造链表节点，存储元素和哈希值
     * @param elem 实际存储的元素
     * @param hash 元素的哈希值
     */
    public LinkedElement(T elem, int hash) {
      this.element = elem;
      this.next = null;
      this.hashCode = hash;
    }

    @Override
    public String toString() {
      return element.toString();
    }
  }

  protected static final float DEFAULT_MAX_LOAD_FACTOR = 0.75f;
  protected static final float DEFAUT_MIN_LOAD_FACTOR = 0.2f;
  protected static final int MINIMUM_CAPACITY = 16;

  static final int MAXIMUM_CAPACITY = 1 << 30;
  private static final Logger LOG =
      LoggerFactory.getLogger(LightWeightHashSet.class);

  /**
   * 哈希表的桶数组，每个桶对应一个链表的头节点。数组长度必须是2的幂。
   */
  protected LinkedElement<T>[] entries;
  /** 哈希表当前容量（桶数组长度） */
  private int capacity;
  /** 集合中实际存储的元素个数，不是桶数组长度 */
  protected int size = 0;
  /** 哈希掩码，用于快速计算桶索引（capacity - 1） */
  private int hash_mask;
  /** 初始化时的容量，缩容时不会小于该值 */
  private final int initialCapacity;

  /**
   * 修改计数，用于快速失败机制，迭代时检测到集合修改会抛出ConcurrentModificationException
   * @see ConcurrentModificationException
   */
  protected int modification = 0;

  private float maxLoadFactor;
  private float minLoadFactor;
  private final int expandMultiplier = 2;

  private int expandThreshold;
  private int shrinkThreshold;

  /**
   * 构造指定初始容量和装载因子的轻量级哈希集合
   * @param initCapacity 推荐的初始容量
   * @param maxLoadFactor 扩容触发阈值，当元素数量超过容量*maxLoadFactor时扩容
   * @param minLoadFactor 缩容触发阈值，当元素数量低于容量*minLoadFactor时缩容
   */
  @SuppressWarnings("unchecked")
  public LightWeightHashSet(int initCapacity, float maxLoadFactor,
      float minLoadFactor) {

    if (maxLoadFactor <= 0 || maxLoadFactor > 1.0f)
      throw new IllegalArgumentException("Illegal maxload factor: "
          + maxLoadFactor);

    if (minLoadFactor <= 0 || minLoadFactor > maxLoadFactor)
      throw new IllegalArgumentException("Illegal minload factor: "
          + minLoadFactor);

    this.initialCapacity = computeCapacity(initCapacity);
    this.capacity = this.initialCapacity;
    this.hash_mask = capacity - 1;

    this.maxLoadFactor = maxLoadFactor;
    this.expandThreshold = (int) (capacity * maxLoadFactor);
    this.minLoadFactor = minLoadFactor;
    this.shrinkThreshold = (int) (capacity * minLoadFactor);

    entries = new LinkedElement[capacity];
    if (LOG.isDebugEnabled()) {
      LOG.debug("initial capacity=" + initialCapacity + ", max load factor= "
          + maxLoadFactor + ", min load factor= " + minLoadFactor);
    }
  }

  /**
   * 使用默认参数构造轻量级哈希集合：初始容量16，最大负载因子0.75，最小负载因子0.2
   */
  public LightWeightHashSet() {
    this(MINIMUM_CAPACITY, DEFAULT_MAX_LOAD_FACTOR, DEFAUT_MIN_LOAD_FACTOR);
  }

  /**
   * 使用指定最小容量和默认负载因子构造轻量级哈希集合
   * @param minCapacity 最小容量
   */
  public LightWeightHashSet(int minCapacity) {
    this(minCapacity, DEFAULT_MAX_LOAD_FACTOR, DEFAUT_MIN_LOAD_FACTOR);
  }

  /**
   * 检查集合是否为空
   * @return 空返回true，否则返回false
   */
  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * 获取当前哈希表容量，仅用于测试
   * @return 当前容量
   */
  public int getCapacity() {
    return capacity;
  }

  /**
   * 获取集合中元素数量
   * @return 元素数量
   */
  @Override
  public int size() {
    return size;
  }

  /**
   * 根据哈希值计算对应的桶索引
   * @param hashCode 元素哈希值
   * @return 桶数组索引
   */
  protected int getIndex(int hashCode) {
    return hashCode & hash_mask;
  }

  /**
   * 检查集合是否包含指定元素
   * @return 包含返回true，否则返回false
   */
  @SuppressWarnings("unchecked")
  @Override
  public boolean contains(final Object key) {
    return getElement((T)key) != null;
  }
  
  /**
   * 获取集合中与给定键相等的元素，如果不存在则返回null
   * @param key 要查找的键
   * @return 匹配的元素，不存在则返回null
   */
  public T getElement(final T key) {
    // 校验元素非空
    if (key == null) {
      throw new IllegalArgumentException("Null element is not supported.");
    }
    // 计算哈希并获取桶索引
    final int hashCode = key.hashCode();
    final int index = getIndex(hashCode);
    return getContainedElem(index, key, hashCode);
  }

  /**
   * 在指定桶中查找匹配元素
   * @param index 桶索引
   * @param key 要查找的键
   * @param hashCode 键的哈希值
   * @return 匹配的元素，不存在则返回null
   */
  protected T getContainedElem(int index, final T key, int hashCode) {
    for (LinkedElement<T> e = entries[index]; e != null; e = e.next) {
      // 哈希相同且元素相等，找到匹配
      if (hashCode == e.hashCode && e.element.equals(key)) {
        return e.element;
      }
    }
    // 未找到匹配元素
    return null;
  }

  /**
   * 添加集合中所有元素，必要时触发扩容
   * @param toAdd 要添加的元素集合
   * @return 集合发生变化返回true，否则返回false
   */
  @Override
  public boolean addAll(Collection<? extends T> toAdd) {
    boolean changed = false;
    for (T elem : toAdd) {
      changed |= addElem(elem);
    }
    expandIfNecessary();
    return changed;
  }

  /**
   * 添加单个元素，必要时触发扩容
   * @param element 要添加的元素
   * @return 元素不存在则添加成功返回true，已存在返回false
   */
  @Override
  public boolean add(final T element) {
    boolean added = addElem(element);
    expandIfNecessary();
    return added;
  }

  /**
   * 将元素添加到哈希表中，不触发扩容检查
   * @param element 要添加的元素
   * @return 添加成功返回true，元素已存在返回false
   */
  protected boolean addElem(final T element) {
    // 校验元素非空
    if (element == null) {
      throw new IllegalArgumentException("Null element is not supported.");
    }
    // 计算哈希和桶索引
    final int hashCode = element.hashCode();
    final int index = getIndex(hashCode);
    // 元素已存在，返回false
    if (getContainedElem(index, element, hashCode) != null) {
      return false;
    }

    // 修改计数+1，元素数量+1
    modification++;
    size++;

    // 将新节点插入到桶链表头部
    LinkedElement<T> le = new LinkedElement<T>(element, hashCode);
    le.next = entries[index];
    entries[index] = le;
    return true;
  }

  /**
   * 删除指定元素，必要时触发缩容
   * @param key 要删除的元素
   * @return 删除成功返回true，元素不存在返回false
   */
  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(final Object key) {
    // 校验元素非空
    if (key == null) {
      throw new IllegalArgumentException("Null element is not supported.");
    }
    LinkedElement<T> removed = removeElem((T) key);
    shrinkIfNecessary();
    return removed == null ? false : true;
  }

  /**
   * 删除指定元素，不触发缩容检查
   * @param key 要删除的元素
   * @return 被删除的节点，不存在返回null
   */
  protected LinkedElement<T> removeElem(final T key) {
    LinkedElement<T> found = null;
    final int hashCode = key.hashCode();
    final int index = getIndex(hashCode);
    if (entries[index] == null) {
      // 桶为空，直接返回
      return null;
    } else if (hashCode == entries[index].hashCode &&
            entries[index].element.equals(key)) {
      // 要删除的是桶链表头节点
      modification++;
      size--;
      found = entries[index];
      entries[index] = found.next;
    } else {
      // 头节点不匹配，遍历链表查找
      LinkedElement<T> prev = entries[index];
      for (found = prev.next; found != null;) {
        if (hashCode == found.hashCode &&
                found.element.equals(key)) {
          // 找到匹配，删除节点
          modification++;
          size--;
          prev.next = found.next;
          found.next = null;
          break;
        } else {
          prev = found;
          found = found.next;
        }
      }
    }
    return found;
  }

  /**
   * 弹出指定数量n个元素，从哈希表中移除并返回这些元素
   * 删除顺序不保证与插入顺序一致
   * @param n 要弹出的元素数量
   * @return 弹出的元素列表
   */
  public List<T> pollN(int n) {
    if (n >= size) {
      return pollAll();
    }
    List<T> retList = new ArrayList<T>(n);
    if (n == 0) {
      return retList;
    }
    boolean done = false;
    int currentBucketIndex = 0;

    while (!done) {
      LinkedElement<T> current = entries[currentBucketIndex];
      while (current != null) {
        retList.add(current.element);
        current = current.next;
        entries[currentBucketIndex] = current;
        size--;
        modification++;
        if (--n == 0) {
          done = true;
          break;
        }
      }
      currentBucketIndex++;
    }
    shrinkIfNecessary();
    return retList;
  }

  /**
   * 弹出所有元素，清空集合并返回所有元素
   * @return 所有元素组成的列表
   */
  public List<T> pollAll() {
    List<T> retList = new ArrayList<T>(size);
    for (int i = 0; i < entries.length; i++) {
      LinkedElement<T> current = entries[i];
      while (current != null) {
        retList.add(current.element);
        current = current.next;
      }
    }
    this.clear();
    return retList;
  }

  /**
   * 将指定长度数组填充集合元素并返回，元素会被从集合中移除
   * 如果数组长度大于集合大小，会重新创建匹配集合大小的数组
   * @param array 目标数组
   * @return 填充了元素的数组
   */
  @SuppressWarnings("unchecked")
  public T[] pollToArray(T[] array) {
    int currentIndex = 0;
    LinkedElement<T> current = null;

    if (array.length == 0) {
      return array;
    }
    if (array.length > size) {
      array = (T[]) java.lang.reflect.Array.newInstance(array.getClass()
          .getComponentType(), size);
    }
    // 如果需要取出全部元素，使用快速遍历
    if (array.length == size) {
      for (int i = 0; i < entries.length; i++) {
        current = entries[i];
        while (current != null) {
          array[currentIndex++] = current.element;
          current = current.next;
        }
      }
      this.clear();
      return array;
    }

    boolean done = false;
    int currentBucketIndex = 0;

    while (!done) {
      current = entries[currentBucketIndex];
      while (current != null) {
        array[currentIndex++] = current.element;
        current = current.next;
        entries[currentBucketIndex] = current;
        size--;
        modification++;
        if (currentIndex == array.length) {
          done = true;
          break;
        }
      }
      currentBucketIndex++;
    }
    shrinkIfNecessary();
    return array;
  }

  /**
   * 根据用户指定的初始容量计算实际容量，结果为不小于输入的最小2的幂
   * 限制容量在[MINIMUM_CAPACITY, MAXIMUM_CAPACITY]范围内
   * @param initial 用户指定的初始容量
   * @return 计算后的实际容量
   */
  private int computeCapacity(int initial) {
    if (initial < MINIMUM_CAPACITY) {
      return MINIMUM_CAPACITY;
    }
    if (initial > MAXIMUM_CAPACITY) {
      return MAXIMUM_CAPACITY;
    }
    int capacity = 1;
    while (capacity < initial) {
      capacity <<= 1;
    }
    return capacity;
  }

  /**
   * 将哈希表重新哈希到新容量
   * @param cap 目标容量
   */
  @SuppressWarnings("unchecked")
  private void resize(int cap) {
    int newCapacity = computeCapacity(cap);
    if (newCapacity == this.capacity) {
      // 容量未变化，无需调整
      return;
    }
    this.capacity = newCapacity;
    this.expandThreshold = (int) (capacity * maxLoadFactor);
    this.shrinkThreshold = (int) (capacity * minLoadFactor);
    this.hash_mask = capacity - 1;
    LinkedElement<T>[] temp = entries;
    entries = new LinkedElement[capacity];
    // 遍历所有节点重新哈希到新桶
    for (int i = 0; i < temp.length; i++) {
      LinkedElement<T> curr = temp[i];
      while (curr != null) {
        LinkedElement<T> next = curr.next;
        int index = getIndex(curr.hashCode);
        curr.next = entries[index];
        entries[index] = curr;
        curr = next;
      }
    }
  }

  /**
   * 检查是否需要缩容，满足条件则执行缩容
   */
  protected void shrinkIfNecessary() {
    if (size < this.shrinkThreshold && capacity > initialCapacity) {
      resize(capacity / expandMultiplier);
    }
  }

  /**
   * 检查是否需要扩容，满足条件则执行扩容
   */
  protected void expandIfNecessary() {
    if (size > this.expandThreshold && capacity < MAXIMUM_CAPACITY) {
      resize(capacity * expandMultiplier