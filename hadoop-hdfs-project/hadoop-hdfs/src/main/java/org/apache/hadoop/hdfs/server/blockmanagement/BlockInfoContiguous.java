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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;

/**
 * HDFS连续块的元数据信息类，用于传统三副本复制方案的块。
 * 继承自BlockInfo，专门为采用复制方案存储的连续块设计，维护块所在数据节点存储信息。
 */
@InterfaceAudience.Private
public class BlockInfoContiguous extends BlockInfo {

  /**
   * 构造函数，初始化指定容量的连续块信息。
   * @param size 期望存储的最大数据节点数量（副本数）
   */
  public BlockInfoContiguous(short size) {
    super(size);
  }

  /**
   * 构造函数，基于已有Block对象初始化指定容量的连续块信息。
   * @param blk 基础块对象
   * @param size 期望存储的最大数据节点数量（副本数）
   */
  public BlockInfoContiguous(Block blk, short size) {
    super(blk, size);
  }

  /**
   * 确保三元组数组有足够空间容纳新增指定数量的存储信息。
   * 如果空间不足则扩容数组，原有数据会复制到新数组中。
   * @param num 需要新增的存储信息数量
   * @return 第一个空闲位置的索引
   */
  private int ensureCapacity(int num) {
    assert this.triplets != null : "BlockInfo is not initialized";
    int last = numNodes();
    if (triplets.length >= (last+num)*3) {
      return last;
    }
    /* Not enough space left. Create a new array. Should normally
     * happen only when replication is manually increased by the user. */
    Object[] old = triplets;
    triplets = new Object[(last+num)*3];
    System.arraycopy(old, 0, triplets, 0, last * 3);
    return last;
  }

  /**
   * 添加数据节点存储信息到当前块，新增一个副本记录。
   * @param storage 要添加的数据节点存储信息
   * @param reportedBlock 数据节点上报的块信息
   * @return 始终返回true表示添加成功
   */
  @Override
  boolean addStorage(DatanodeStorageInfo storage, Block reportedBlock) {
    Preconditions.checkArgument(this.getBlockId() == reportedBlock.getBlockId(),
        "reported blk_%s is different from stored blk_%s",
        reportedBlock.getBlockId(), this.getBlockId());
    // 确保有新增存储的空间，获取第一个空闲位置
    int lastNode = ensureCapacity(1);
    // 写入存储信息
    setStorageInfo(lastNode, storage);
    // 初始化前后链表指针为null
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  /**
   * 从当前块移除指定数据节点存储信息，删除一个副本记录。
   * @param storage 要移除的数据节点存储信息
   * @return 移除成功返回true，存储不存在返回false
   */
  @Override
  boolean removeStorage(DatanodeStorageInfo storage) {
    int dnIndex = findStorageInfo(storage);
    if (dnIndex < 0) { // 未找到目标存储
      return false;
    }
    assert getPrevious(dnIndex) == null && getNext(dnIndex) == null :
        "Block is still in the list and must be removed first.";
    // 获取最后一个有效存储节点索引
    int lastNode = numNodes()-1;
    // 用最后一个节点覆盖当前被删除节点，实现O(1)删除
    setStorageInfo(dnIndex, getStorageInfo(lastNode));
    setNext(dnIndex, getNext(lastNode));
    setPrevious(dnIndex, getPrevious(lastNode));
    // 清空最后一个位置的三元组数据
    setStorageInfo(lastNode, null);
    setNext(lastNode, null);
    setPrevious(lastNode, null);
    return true;
  }

  /**
   * 检查当前块是否存在外部提供存储（PROVIDED类型）的副本。
   * @return 如果存在至少一个PROVIDED类型存储返回true，否则返回false
   */
  @Override
  boolean isProvided() {
    int len = getCapacity();
    for (int idx = 0; idx < len; idx++) {
      DatanodeStorageInfo storage = getStorageInfo(idx);
      if (storage != null
          && storage.getStorageType().equals(StorageType.PROVIDED)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 获取当前块已存储的数据节点（副本）数量。
   * @return 有效副本数量
   */
  @Override
  public int numNodes() {
    assert this.triplets != null : "BlockInfo is not initialized";
    assert triplets.length % 3 == 0 : "Malformed BlockInfo";

    // 从数组末尾向前查找第一个有效节点，返回对应位置+1
    for (int idx = getCapacity()-1; idx >= 0; idx--) {
      if (getDatanode(idx) != null) {
        return idx + 1;
      }
    }
    return 0;
  }

  /**
   * 判断当前块是否为纠删码条状块。
   * @return 连续复制块固定返回false
   */
  @Override
  public final boolean isStriped() {
    return false;
  }

  /**
   * 获取当前块的类型。
   * @return 固定返回CONTIGUOUS（连续复制块类型）
   */
  @Override
  public BlockType getBlockType() {
    return BlockType.CONTIGUOUS;
  }

  /**
   * 检查当前块是否未关联任何存储信息。
   * @return 没有存储信息返回true，否则返回false
   */
  @Override
  final boolean hasNoStorage() {
    return getStorageInfo(0) == null;
  }
}