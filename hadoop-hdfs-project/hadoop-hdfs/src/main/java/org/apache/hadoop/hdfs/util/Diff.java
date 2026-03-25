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

import org.apache.hadoop.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @file org/apache/hadoop/hdfs/util/Diff.java
 * @brief 存储集合从旧状态到新状态的变更差异，支持增量变更计算、合并和回滚
 *
 * 基于有序集合的变更算法，维护新增元素列表(c-list)和删除元素列表(d-list)，
 * 支持对创建、删除、修改操作的记录与回滚，可合并多个连续差异得到最终状态变更。
 * 核心用于HDFS命名空间增量快照、编辑日志合并等场景，高效记录状态变更。
 * 
 * @param <K> 元素键类型
 * @param <E> 元素类型，必须实现 {@link Element} 接口
 */
public class Diff<K, E extends Diff.Element<K>> {
  /**
   * Diff中元素需要实现的接口，提供按键比较的能力
   * @param <K> 键类型
   */
  public static interface Element<K> extends Comparable<K> {
    /** @return 当前元素的键 */
    public K getKey();
  }

  /**
   * 元素处理回调接口，用于在差异合并时处理被覆盖/删除的元素
   * @param <E> 元素类型
   */
  public static interface Processor<E> {
    /** 处理给定元素 */
    public void process(E element);
  }

  /**
   * 包装单个元素的容器，用于返回查找结果
   * @param <E> 元素类型
   */
  public static class Container<E> {
    private final E element;

    private Container(E element) {
      this.element = element;
    }

    /** @return 容器中存储的元素 */
    public E getElement() {
      return element;
    }
  }
  
  /**
   * 撤销操作信息存储，用于delete和modify操作的回滚
   * @param <E> 元素类型
   */
  public static class UndoInfo<E> {
    private final int createdInsertionPoint;
    private final E trashed;
    private final Integer deletedInsertionPoint;
    
    private UndoInfo(final int createdInsertionPoint, final E trashed,
        final Integer deletedInsertionPoint) {
      this.createdInsertionPoint = createdInsertionPoint;
      this.trashed = trashed;
      this.deletedInsertionPoint = deletedInsertionPoint;
    }
    
    /** @return 被替换/删除的原始元素，用于回滚 */
    public E getTrashedElement() {
      return trashed;
    }
  }

  /** 默认列表初始容量 */
  private static final int DEFAULT_ARRAY_INITIAL_CAPACITY = 4;

  /**
   * 在有序列表中二分查找指定键对应的元素
   * @param elements 有序元素列表，可以为null
   * @param name 要查找的键
   * @return 列表为null返回-1；否则返回Collections.binarySearch定义的查找结果，负数表示未找到，其绝对值减一为插入点
   */
  protected static <K, E extends Comparable<K>> int search(
      final List<E> elements, final K name) {
    return elements == null? -1: Collections.binarySearch(elements, name);
  }

  /**
   * 从列表指定位置移除元素，并校验移除元素和预期一致
   * @param elements 目标列表
   * @param i 查找结果（负数），插入点为-i-1
   * @param expected 预期移除的元素
   */
  private static <E> void remove(final List<E> elements, final int i,
      final E expected) {
    final E removed = elements.remove(-i - 1);
    Preconditions.checkState(removed == expected,
        "removed != expected=%s, removed=%s.", expected, removed);
  }

  /** 新增元素列表：当前状态相对于旧状态新增的元素 */
  private List<E> created;
  /** 删除元素列表：当前状态相对于旧状态删除的元素 */
  private List<E> deleted;
  
  /** 构造空差异对象 */
  protected Diff() {}

  /**
   * 构造指定初始新增和删除列表的差异对象
   * @param created 初始新增元素列表
   * @param deleted 初始删除元素列表
   */
  protected Diff(final List<E> created, final List<E> deleted) {
    this.created = created;
    this.deleted = deleted;
  }

  /**
   * 获取新增元素列表的不可修改视图
   * @return 不可修改的新增元素列表
   */
  public List<E> getCreatedUnmodifiable() {
    return created != null? Collections.unmodifiableList(created)
        : Collections.emptyList();
  }

  /**
   * 更新新增列表指定位置的元素，校验键一致性
   * @param index 列表索引
   * @param element 新元素
   * @return 被替换的旧元素
   */
  public E setCreated(int index, E element) {
    final E old = created.set(index, element);
    if (old.compareTo(element.getKey()) != 0) {
      throw new AssertionError("Element mismatched: element=" + element
          + " but old=" + old);
    }
    return old;
  }

  /**
   * 从新增列表移除指定元素
   * @param element 要移除的元素
   * @return 移除成功返回true，元素不存在返回false
   */
  public boolean removeCreated(final E element) {
    if (created != null) {
      final int i = search(created, element.getKey());
      if (i >= 0 && created.get(i) == element) {
        created.remove(i);
        return true;
      }
    }
    return false;
  }

  /** 清空新增列表 */
  public void clearCreated() {
    if (created != null) {
      created.clear();
    }
  }

  /**
   * 获取删除元素列表的不可修改视图
   * @return 不可修改的删除元素列表
   */
  public List<E> getDeletedUnmodifiable() {
    return deleted != null? Collections.unmodifiableList(deleted)
        : Collections.emptyList();
  }

  /**
   * 检查删除列表是否包含指定键
   * @param key 要检查的键
   * @return 包含返回true，否则返回false
   */
  public boolean containsDeleted(final K key) {
    if (deleted != null) {
      return search(deleted, key) >= 0;
    }
    return false;
  }

  /**
   * 检查删除列表是否包含指定元素
   * @param element 要检查的元素
   * @return 包含返回true，否则返回false
   */
  public boolean containsDeleted(final E element) {
    return getDeleted(element.getKey()) == element;
  }

  /**
   * 根据键从删除列表查找元素
   * @param key 元素键
   * @return 找到返回对应元素，找不到返回null
   */
  public E getDeleted(final K key) {
    if (deleted != null) {
      final int c = search(deleted, key);
      if (c >= 0) {
        return deleted.get(c);
      }
    }
    return null;
  }

  /**
   * 从删除列表移除指定元素
   * @param element 要移除的元素
   * @return 移除成功返回true，元素不存在返回false
   */
  public boolean removeDeleted(final E element) {
    if (deleted != null) {
      final int i = search(deleted, element.getKey());
      if (i >= 0 && deleted.get(i) == element) {
        deleted.remove(i);
        return true;
      }
    }
    return false;
  }

  /** 清空删除列表 */
  public void clearDeleted() {
    if (deleted != null) {
      deleted.clear();
    }
  }

  /**
   * 检查差异是否为空（无任何变更）
   * @return 没有新增也没有删除返回true，否则返回false
   */
  public boolean isEmpty() {
    return (created == null || created.isEmpty())
        && (deleted == null || deleted.isEmpty());
  }
  
  /**
   * 将元素添加到新增列表的指定插入点
   * @param element 要添加的元素
   * @param i 二分查找结果（必须为负数，表示元素不存在）
   */
  private void addCreated(final E element, final int i) {
    if (i >= 0) {
      throw new AssertionError("Element already exists: element=" + element
          + ", created=" + created);
    }
    if (created == null) {
      created = new ArrayList<>(DEFAULT_ARRAY_INITIAL_CAPACITY);
    }
    created.add(-i - 1, element);
  }

  /**
   * 将元素添加到删除列表的指定插入点
   * @param element 要添加的元素
   * @param i 二分查找结果（必须为负数，表示元素不存在）
   */
  private void addDeleted(final E element, final int i) {
    if (i >= 0) {
      throw new AssertionError("Element already exists: element=" + element
          + ", deleted=" + deleted);
    }
    if (deleted == null) {
      deleted = new ArrayList<>(DEFAULT_ARRAY_INITIAL_CAPAC);
    }
    deleted.add(-i - 1, element);
  }


  /**
   * 记录创建元素操作，添加到新增列表
   * @param element 要创建的元素
   * @return 插入点，用于撤销操作
   */
  public int create(final E element) {
    final int c = search(created, element.getKey());
    addCreated(element, c);
    return c;
  }

  /**
   * 撤销上一次创建元素操作
   * @param element 要撤销的元素
   * @param insertionPoint create操作返回的插入点
   */
  public void undoCreate(final E element, final int insertionPoint) {
    remove(created, insertionPoint, element);
  }

  /**
   * 记录删除元素操作，处理不同情况更新新增/删除列表
   * @param element 要删除的元素
   * @return 撤销信息，用于回滚该操作
   */
  public UndoInfo<E> delete(final E element) {
    final int c = search(created, element.getKey());
    E previous = null;
    Integer d = null;
    if (c >= 0) {
      // 删除的是刚新增的元素，直接从新增列表移除
      previous = created.remove(c);
    } else {
      // 原状态已存在的元素，添加到删除列表
      d = search(deleted, element.getKey());
      addDeleted(element, d);
    }
    return new UndoInfo<E>(c, previous, d);
  }
  
  /**
   * 撤销上一次删除元素操作
   * @param element 被删除的元素
   * @param undoInfo delete操作返回的撤销信息
   */
  public void undoDelete(final E element, final UndoInfo<E> undoInfo) {
    final int c = undoInfo.createdInsertionPoint;
    if (c >= 0) {
      created.add(c, undoInfo.trashed);
    } else {
      remove(deleted, undoInfo.deletedInsertionPoint, element);
    }
  }

  /**
   * 记录修改元素操作，将旧元素加入删除列表、新元素加入新增列表
   * @param oldElement 修改前的旧元素
   * @param newElement 修改后的新元素
   * @return 撤销信息，用于回滚该操作
   */
  public UndoInfo<E> modify(final E oldElement, final E newElement) {
    Preconditions.checkArgument(oldElement != newElement,
        "They are the same object: oldElement == newElement = %s", newElement);
    Preconditions.checkArgument(oldElement.compareTo(newElement.getKey()) == 0,
        "The names do not match: oldElement=%s, newElement=%s",
        oldElement, newElement);
    final int c = search(created, newElement.getKey());
    E previous = null;
    Integer d = null;
    if (c >= 0) {
      // 元素已经在新增列表，直接替换
      previous = created.set(c, newElement);
      
      // 保留旧元素用于回滚
      previous = oldElement;
    } else {
      d = search(deleted, oldElement.getKey());
      if (d < 0) {
        // 不在两个列表中，新增新元素、删除旧元素
        addCreated(newElement, c);
        addDeleted(oldElement, d);
      }
    }
    return new UndoInfo<E>(c, previous, d);
  }

  /**
   * 撤销上一次修改元素操作
   * @param oldElement 修改前的旧元素
   * @param newElement 修改后的新元素
   * @param undoInfo modify操作返回的撤销信息
   */
  public void undoModify(final E oldElement, final E newElement,
      final UndoInfo<E> undoInfo) {
    final int c = undoInfo.createdInsertionPoint;
    if (c >= 0) {
      created.set(c, undoInfo.trashed);
    } else {
      final int d = undoInfo.deletedInsertionPoint;
      if (d < 0) {
        remove(created, c, newElement);
        remove(deleted, d, oldElement);
      }
    }
  }

  /**
   * 根据键从差异中查找旧状态对应的元素
   * @param name 元素键
   * @return null表示差异中无记录，需要从当前状态查找；否则返回包装了旧状态元素的容器，容器元素为null表示旧状态不存在该元素
   */
  public Container<E> accessPrevious(final K name) {
    return accessPrevious(name, created, deleted);
  }

  private static <K, E extends Diff.Element<K>> Container<E> accessPrevious(
      final K name, final List<E> clist, final List<E> dlist) {
    final int d = search(dlist, name);
    if (d >= 0) {
      // 元素在删除列表，说明旧状态存在，当前被删除
      return new Container<E>(dlist.get(d));
    } else {
      final int c = search(clist, name);
      // 元素在新增列表，说明旧状态不存在
      return c < 0? null: new Container<E>(null);
    }
  }

  /**
   * 根据键从差异中查找当前状态对应的元素
   * @param name 元素键
   * @return null表示差异中无记录，需要从旧状态查找；否则返回包装了当前状态元素的容器，容器元素为null表示当前状态不存在该元素
   */
  public Container<E> accessCurrent(K name) {
    return accessPrevious(name, deleted, created);
  }

  /**
   * 将当前差异应用到旧状态列表，计算得到当前状态列表
   * @param previous 旧状态有序列表
   * @return 应用差异后的当前状态有序列表
   */
  public List<E> apply2Previous(final List<E> previous) {
    return apply2Previous(previous,
        getCreatedUnmodifiable(), getDeletedUnmodifiable());
  }

  private static <K, E extends Diff.Element<K>> List<E> apply2Previous(
      final List<E> previous, final List<E> clist, final List<E> dlist) {
    // 假设前提:
    // (A1) 所有列表都是有序的
    // (A2) 删除列表中所有元素都一定存在于旧状态
    // (A3) 新增列表中所有元素一定不存在于 (旧状态 - 删除列表) 中
    // 第一步：从旧状态删除dlist中的元素，得到中间结果tmp
    final List<E> tmp = new ArrayList<E>(previous.size() - dlist.size());
    {
      // tmp = previous - dlist
      final Iterator<E> i = previous.iterator();
      for(E deleted : dlist) {
        E e = i.next(); //根据A2假设，dlist非空时一定存在元素
        int cmp = 0;
        // 找到对应元素前，把较小元素加入tmp
        for(; (cmp = e.compareTo(deleted.getKey())) < 0; e = i.next()) {
          tmp.add(e);
        }
        Preconditions.checkState(cmp == 0); // 校验A2假设
      }
      // 把剩余元素加入tmp
      for(; i.hasNext(); ) {
        tmp.add(i.next());
      }
    }

    // 第二步：合并tmp