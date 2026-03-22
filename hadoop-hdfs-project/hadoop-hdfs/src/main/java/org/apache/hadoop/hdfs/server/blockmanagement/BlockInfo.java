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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.util.LightWeightGSet;

import static org.apache.hadoop.hdfs.server.namenode.INodeId.INVALID_INODE_ID;

/**
 * @file org/apache/hadoop/hdfs/server/blockmanagement/BlockInfo.java
 * @brief HDFS块元数据抽象基类，维护普通块或纠删码块组的归属信息和存储位置信息
 * 
 * 对于给定块（或纠删码块组），本类维护两个核心信息：
 * 1. 该块所属的{@link BlockCollection}（文件/块组）
 * 2. 该块副本（或纠删码块组中所有块）所在的DataNode存储信息
 */
@InterfaceAudience.Private
public abstract class BlockInfo extends Block
    implements LightWeightGSet.LinkedElement {

  public static final BlockInfo[] EMPTY_ARRAY = {};

  /**
   * 副本系数，对于纠删码块组该值为0
   */
  private short replication;

  /**
   * 所属块集合（文件）ID
   */
  private volatile long bcId;

  /** 实现LightWeightGSet.LinkedElement接口所需，用于块映射链表链接 */
  private LightWeightGSet.LinkedElement nextLinkedElement;

  /**
   * 存储三元组数组，每个存储位置占用三个数组元素：
   * triplets[3*i]     = {@link DatanodeStorageInfo} 存储信息引用
   * triplets[3*i+1]   = 该存储的块链表中前驱块引用
   * triplets[3*i+2]   = 该存储的块链表中后继块引用
   * 
   * 使用三元组数组而非LinkedList是为了优化内存占用：LinkedList每个条目需要额外42字节，而三元组只需要16字节
   */
  protected Object[] triplets;

  /** 构建中块特性，仅当块处于构建中状态时非空 */
  private BlockUnderConstructionFeature uc;

  /**
   * 构造块信息对象，用于块映射表存储
   * @param size 副本系数（普通块）或块组中总块数（纠删码）
   */
  public BlockInfo(short size) {
    this.triplets = new Object[3 * size];
    this.bcId = INVALID_INODE_ID;
    this.replication = isStriped() ? 0 : size;
  }

  /**
   * 基于已有Block构造块信息对象
   */
  public BlockInfo(Block blk, short size) {
    super(blk);
    this.triplets = new Object[3 * size];
    this.bcId = INVALID_INODE_ID;
    this.replication = isStriped() ? 0 : size;
  }

  /**
   * 获取块副本系数
   * @return 副本系数
   */
  public short getReplication() {
    return replication;
  }

  /**
   * 设置块副本系数
   * @param repl 新的副本系数
   */
  public void setReplication(short repl) {
    this.replication = repl;
  }

  /**
   * 获取所属块集合ID
   * @return 块集合ID
   */
  public long getBlockCollectionId() {
    return bcId;
  }

  /**
   * 设置所属块集合ID
   * @param id 块集合ID
   */
  public void setBlockCollectionId(long id) {
    this.bcId = id;
  }

  /**
   * 标记该块已删除
   */
  public void delete() {
    setBlockCollectionId(INVALID_INODE_ID);
  }

  /**
   * 检查该块是否已删除
   * @return true表示块已删除，不再属于任何文件
   */
  public boolean isDeleted() {
    return bcId == INVALID_INODE_ID;
  }

  /**
   * 获取该块所有存储位置的迭代器
   * @return 存储信息迭代器
   */
  public Iterator<DatanodeStorageInfo> getStorageInfos() {
    return new BlocksMap.StorageIterator(this);
  }

  /**
   * 根据索引获取存储对应的DataNode
   * @param index 三元组索引
   * @return DataNode描述符，未找到返回null
   */
  public DatanodeDescriptor getDatanode(int index) {
    DatanodeStorageInfo storage = getStorageInfo(index);
    return storage == null ? null : storage.getDatanodeDescriptor();
  }

  /**
   * 根据索引获取存储信息
   * @param index 三元组索引
   * @return DataNode存储信息
   */
  DatanodeStorageInfo getStorageInfo(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 < triplets.length : "Index is out of bound";
    return (DatanodeStorageInfo)triplets[index * 3];
  }

  /**
   * 根据索引获取前驱块
   * @param index 三元组索引
   * @return 前驱块信息
   */
  BlockInfo getPrevious(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index * 3 + 1];
    assert info == null ||
        info.getClass().getName().startsWith(BlockInfo.class.getName()) :
        "BlockInfo is expected at " + (index * 3 + 1);
    return info;
  }

  /**
   * 根据索引获取后继块
   * @param index 三元组索引
   * @return 后继块信息
   */
  BlockInfo getNext(int index) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo)triplets[index * 3 + 2];
    assert info == null || info.getClass().getName().startsWith(
        BlockInfo.class.getName()) :
        "BlockInfo is expected at " + (index * 3 + 2);
    return info;
  }

  /**
   * 设置索引位置对应的存储信息
   * @param index 三元组索引
   * @param storage 存储信息
   */
  void setStorageInfo(int index, DatanodeStorageInfo storage) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 < triplets.length : "Index is out of bound";
    triplets[index * 3] = storage;
  }

  /**
   * 设置指定索引位置的前驱块，返回原前驱块
   *
   * @param index 存储索引
   * @param to 要设置的新前驱块
   * @return 原前驱块
   */
  BlockInfo setPrevious(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 1 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo) triplets[index * 3 + 1];
    triplets[index * 3 + 1] = to;
    return info;
  }

  /**
   * 设置指定索引位置的后继块，返回原后继块
   *
   * @param index 存储索引
   * @param to 要设置的新后继块
   * @return 原后继块
   */
  BlockInfo setNext(int index, BlockInfo to) {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert index >= 0 && index * 3 + 2 < triplets.length : "Index is out of bound";
    BlockInfo info = (BlockInfo) triplets[index * 3 + 2];
    triplets[index * 3 + 2] = to;
    return info;
  }

  /**
   * 获取当前块可容纳的最大存储位置数量
   * @return 最大存储位置数量
   */
  public int getCapacity() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";
    return triplets.length / 3;
  }

  /**
   * 统计当前块所在的DataNode数量（即NameNode已收到块报告的副本数量）
   * @return 数据节点数量
   */
  public abstract int numNodes();

  /**
   * 为块添加一个DataNode存储位置
   * @param storage 要添加的存储信息
   * @param reportedBlock DataNode上报的块，仅纠删码块使用，块ID包含该块在组内索引信息
   * @return 添加成功返回true，已存在返回false
   */
  abstract boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock);

  /**
   * 移除块的一个DataNode存储位置
   * @param storage 要移除的存储信息
   * @return 移除成功返回true，不存在返回false
   */
  abstract boolean removeStorage(DatanodeStorageInfo storage);

  /**
   * 检查是否为纠删码条带块
   * @return true表示是纠删码块
   */
  public abstract boolean isStriped();

  /**
   * 获取块类型（普通块、纠删码组等）
   * @return 块类型枚举
   */
  public abstract BlockType getBlockType();

  /**
   * 检查该块是否没有关联任何DataNode存储
   * @return true表示无存储关联
   */
  abstract boolean hasNoStorage();

  /**
   * 检查该块是否存在PROVIDED类型副本
   * @return true表示存在PROVIDED存储上的副本
   */
  abstract boolean isProvided();

  /**
   * 根据DataNode描述符查找对应的存储信息
   * @param dn 目标DataNode
   * @return 存储信息，未找到返回null
   */
  DatanodeStorageInfo findStorageInfo(DatanodeDescriptor dn) {
    int len = getCapacity();
    DatanodeStorageInfo providedStorageInfo = null;
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if(cur != null) {
        if (cur.getStorageType() == StorageType.PROVIDED) {
          // PROVIDED存储需要匹配存储ID，而非DataNode
          if (dn.getStorageInfo(cur.getStorageID()) != null) {
            // 不立即返回，需要继续检查其他可能的本地存储
            providedStorageInfo = cur;
          }
        } else if (cur.getDatanodeDescriptor() == dn) {
          return cur;
        }
      }
    }
    return providedStorageInfo;
  }

  /**
   * 根据存储信息查找对应的索引
   * @return 索引，未找到返回-1
   */
  int findStorageInfo(DatanodeStorageInfo storageInfo) {
    int len = getCapacity();
    for(int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo cur = getStorageInfo(idx);
      if (cur == storageInfo) {
        return idx;
      }
    }
    return -1;
  }

  /**
   * 将当前块插入到指定存储的块链表头部
   * 如果链表为空则创建新链表
   * @return 当前块，作为新链表头
   */
  BlockInfo listInsert(BlockInfo head, DatanodeStorageInfo storage) {
    int dnIndex = this.findStorageInfo(storage);
    assert dnIndex >= 0 : "Data node is not found: current";
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is already in the list and cannot be inserted.";
    this.setPrevious(dnIndex, null);
    this.setNext(dnIndex, head);
    if (head != null) {
      head.setPrevious(head.findStorageInfo(storage), this);
    }
    return this;
  }

  /**
   * 将当前块从指定存储的块链表中移除
   * 如果当前块是链表头，则返回下一块作为新头
   * @return 新链表头，删除后链表为空返回null
   */
  BlockInfo listRemove(BlockInfo head, DatanodeStorageInfo storage) {
    if (head == null) {
      return null;
    }
    int dnIndex = this.findStorageInfo(storage);
    if (dnIndex < 0) { // 当前块不在该数据节点链表中
      return head;
    }

    BlockInfo next = this.getNext(dnIndex);
    BlockInfo prev = this.getPrevious(dnIndex);
    this.setNext(dnIndex, null);
    this.setPrevious(dnIndex, null);
    if (prev != null) {
      prev.setNext(prev.findStorageInfo(storage), next);
    }
    if (next != null) {
      next.setPrevious(next.findStorageInfo(storage), prev);
    }
    if (this == head) { // 删除的是链表头
      head = next;
    }
    return head;
  }

  /**
   * 将当前块从存储链表中移除，并移动到链表头部
   *
   * @return 新链表头
   */
  public BlockInfo moveBlockToHead(BlockInfo head, DatanodeStorageInfo storage,
      int curIndex, int headIndex) {
    if (head == this) {
      return this;
    }
    BlockInfo next = this.setNext(curIndex, head);
    BlockInfo prev = this.setPrevious(curIndex, null);

    head.setPrevious(headIndex, this);
    prev.setNext(prev.findStorageInfo(storage), next);
    if (next != null) {
      next.setPrevious(next.findStorageInfo(storage), prev);
    }
    return this;
  }

  @Override
  public int hashCode() {
    // 父类实现已满足需求
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // 依赖父类实现即可
    return (this == obj) || super.equals(obj);
  }

  @Override
  public LightWeightGSet.LinkedElement getNext() {
    return nextLinkedElement;
  }

  @Override
  public void setNext(LightWeightGSet.LinkedElement next) {
    this.nextLinkedElement = next;
  }

  /* 构建中块特性相关方法 */

  /**
   * 获取构建中块特性对象
   * @return 构建中特性，完整块返回null
   */
  public BlockUnderConstructionFeature getUnderConstructionFeature() {
    return uc;
  }

  /**
   * 获取块构建状态
   * @return 块构建状态枚举
   */
  public BlockUCState getBlockUCState() {
    return uc == null ? BlockUCState.COMPLETE : uc.getBlockUCState();
  }

  /**
   * 检查块是否已完成
   *
   * @return true表示块状态为COMPLETE
   */
  public boolean isComplete() {
    return getBlockUCState().equals(BlockUCState.COMPLETE);
  }

  /**
   * 检查块是否处于恢复中
   * @return true表示块状态为UNDER_RECOVERY
   */
  public boolean isUnderRecovery() {
    return getBlockUCState().equals(BlockUCState.UNDER_RECOVERY);
  }

  /**
   * 检查块是否已完成或已提交
   * @return true表示块状态为COMPLETE或COMMITTED
   */
  public final boolean isCompleteOrCommitted() {
    final BlockUCState state = getBlockUCState();
    return state.equals(BlockUCState.COMPLETE) ||
        state.equals(BlockUCState.COMMITTED);
  }

  /**
   * 转换为构建中块，添加或更新构建中特性
   * @param s 初始构建状态
   * @param targets 预期存储位置数组
   */
  public void convertToBlockUnderConstruction(BlockUCState s,
      D