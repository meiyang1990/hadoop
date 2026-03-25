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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList.Type;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.IntrusiveCollection.Element;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

/**
 * 文件功能说明：表示HDFS中被DataNode缓存的块，维护该块在不同缓存列表中的位置信息，
 * 支持同时加入多个缓存列表（不同DataNode的已缓存、待缓存、待移除缓存列表），
 * 用于NameNode的块缓存管理功能。
 */
/**
 * Represents a cached block.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
/**
 * 缓存块信息类，维护HDFS中已缓存块的元数据，同时支持侵入式集合接入多个缓存列表
 * 实现了IntrusiveCollection.Element接口支持侵入式链表，实现了LightWeightGSet.LinkedElement
 * 接口支持在LightWeightGSet中存储
 */
public final class CachedBlock implements Element, 
    LightWeightGSet.LinkedElement {
  private static final Object[] EMPTY_ARRAY = new Object[0];

  /**
   * 块ID，唯一标识该缓存块对应的数据块
   */
  private final long blockId;

  /**
   * 用于LightWeightGSet链表结构的下一个元素指针
   */
  private LinkedElement nextElement;

  /**
   * 复合存储缓存副本数和标记位：bit 15保存标记位，bit 0-14保存缓存副本因子
   */
  private short replicationAndMark;

  /**
   * 多链表指针存储数组，每个三元组存储(所属列表, 前驱节点指针, 后继节点指针)
   * 允许一个CachedBlock同时存在于多个不同DataNode的缓存列表中
   */
  private Object[] triplets;

  /**
   * 构造一个缓存块对象
   * @param blockId 数据块ID
   * @param replication 缓存副本数
   * @param mark 标记位，用于缓存流程中的状态标记
   */
  public CachedBlock(long blockId, short replication, boolean mark) {
    this.blockId = blockId;
    this.triplets = EMPTY_ARRAY;
    setReplicationAndMark(replication, mark);
  }

  /**
   * 获取该缓存块对应的数据块ID
   * @return 数据块ID
   */
  public long getBlockId() {
    return blockId;
  }

  @Override
  public int hashCode() {
    // 基于块ID计算哈希值
    return (int)(blockId^(blockId>>>32));
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) { return false; }
    if (o == this) { return true; }
    if (o.getClass() != this.getClass()) {
      return false;
    }
    CachedBlock other = (CachedBlock)o;
    // 相同块ID视为同一个缓存块
    return other.blockId == blockId;
  }

  /**
   * 设置缓存副本数和标记位
   * @param replication 缓存副本数
   * @param mark 标记位
   */
  public void setReplicationAndMark(short replication, boolean mark) {
    assert replication >= 0;
    // 按位拼接存储：副本数左移1位，最低位存储标记
    replicationAndMark = (short)((replication << 1) | (mark ? 0x1 : 0x0));
  }

  /**
   * 获取标记位状态
   * @return true表示已标记，false表示未标记
   */
  public boolean getMark() {
    return ((replicationAndMark & 0x1) != 0);
  }

  /**
   * 获取期望缓存副本数
   * @return 缓存副本数
   */
  public short getReplication() {
    return (short) (replicationAndMark >>> 1);
  }

  /**
   * 检查当前缓存块是否已经存在于指定缓存列表中
   * @param cachedBlocksList 待检查的缓存列表
   * @return true表示已在列表中，false表示不在
   */
  public boolean isPresent(CachedBlocksList cachedBlocksList) {
    for (int i = 0; i < triplets.length; i += 3) {
      CachedBlocksList list = (CachedBlocksList)triplets[i];
      if (list == cachedBlocksList) {
        return true;
      }
    }
    return false;
  }

  /**
   * 获取当前缓存块所在的所有DataNode列表，可按缓存列表类型过滤
   * @param type 过滤的列表类型，null不过滤
   * @return 符合条件的DataNode列表，返回的列表修改不影响原状态
   */
  public List<DatanodeDescriptor> getDatanodes(Type type) {
    List<DatanodeDescriptor> nodes = new LinkedList<DatanodeDescriptor>();
    // 遍历所有三元组，按类型过滤收集DataNode
    for (int i = 0; i < triplets.length; i += 3) {
      CachedBlocksList list = (CachedBlocksList)triplets[i];
      if ((type == null) || (list.getType() == type)) {
        nodes.add(list.getDatanode());
      }
    }
    return nodes;
  }

  @Override
  /**
   * 将当前元素插入到指定侵入式集合内部，添加新的三元组存储链表指针
   */
  public void insertInternal(IntrusiveCollection<? extends Element> list, Element prev,
      Element next) {
    // 先检查是否已经在列表中，避免重复插入
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        throw new RuntimeException("Trying to re-insert an element that " +
            "is already in the list.");
      }
    }
    // 扩展三元组数组，添加新的列表指针信息
    Object newTriplets[] = Arrays.copyOf(triplets, triplets.length + 3);
    newTriplets[triplets.length] = list;
    newTriplets[triplets.length + 1] = prev;
    newTriplets[triplets.length + 2] = next;
    triplets = newTriplets;
  }
  
  @Override
  /**
   * 设置当前元素在指定列表中的前驱节点
   */
  public void setPrev(IntrusiveCollection<? extends Element> list, Element prev) {
    // 遍历查找对应列表的三元组
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        triplets[i + 1] = prev;
        return;
      }
    }
    throw new RuntimeException("Called setPrev on an element that wasn't " +
        "in the list.");
  }

  @Override
  /**
   * 设置当前元素在指定列表中的后继节点
   */
  public void setNext(IntrusiveCollection<? extends Element> list, Element next) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        triplets[i + 2] = next;
        return;
      }
    }
    throw new RuntimeException("Called setNext on an element that wasn't " +
        "in the list.");
  }

  @Override
  /**
   * 从指定侵入式集合中移除当前元素，删除对应三元组
   */
  public void removeInternal(IntrusiveCollection<? extends Element> list) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        // 创建新数组，拷贝除当前三元组之外的内容
        Object[] newTriplets = new Object[triplets.length - 3];
        System.arraycopy(triplets, 0, newTriplets, 0, i);
        System.arraycopy(triplets, i + 3, newTriplets, i,
            triplets.length - (i + 3));
        triplets = newTriplets;
        return;
      }
    }
    throw new RuntimeException("Called remove on an element that wasn't " +
        "in the list.");
  }

  @Override
  /**
   * 获取当前元素在指定列表中的前驱节点
   * @return 前驱节点
   */
  public Element getPrev(IntrusiveCollection<? extends Element> list) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        return (Element)triplets[i + 1];
      }
    }
    throw new RuntimeException("Called getPrev on an element that wasn't " +
        "in the list.");
  }

  @Override
  /**
   * 获取当前元素在指定列表中的后继节点
   * @return 后继节点
   */
  public Element getNext(IntrusiveCollection<? extends Element> list) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        return (Element)triplets[i + 2];
      }
    }
    throw new RuntimeException("Called getNext on an element that wasn't " +
        "in the list.");
  }

  @Override
  /**
   * 检查当前元素是否在指定侵入式集合中
   * @return true表示在集合中，false表示不在
   */
  public boolean isInList(IntrusiveCollection<? extends Element> list) {
    for (int i = 0; i < triplets.length; i += 3) {
      if (triplets[i] == list) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public String toString() {
    // 拼接缓存块信息字符串输出
    return new StringBuilder().append("{").
        append("blockId=").append(blockId).append(", ").
        append("replication=").append(getReplication()).append(", ").
        append("mark=").append(getMark()).append("}").
        toString();
  }

  @Override // LightWeightGSet.LinkedElement 
  /**
   * 设置LightWeightGSet中的下一个元素
   */
  public void setNext(LinkedElement next) {
    this.nextElement = next;
  }

  @Override // LightWeightGSet.LinkedElement 
  /**
   * 获取LightWeightGSet中的下一个元素
   * @return 下一个元素指针
   */
  public LinkedElement getNext() {
    return nextElement;
  }
}