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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件：org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo
 * 模块：HDFS服务端块管理模块
 * 描述：表示DataNode上的一个存储卷，NameNode端维护DataNode各个存储卷的元数据信息，包括容量、使用情况、存储状态、块列表等核心信息
 * 核心职责：管理单个DataNode存储卷的状态、容量使用统计以及该存储卷上存储的所有数据块元数据
 */
public class DatanodeStorageInfo {
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};

  /**
   * 将DatanodeStorageInfo数组转换为对应DatanodeInfo数组
   * @param storages 输入的存储信息数组
   * @return 转换后的DatanodeInfo数组
   */
  public static DatanodeInfo[] toDatanodeInfos(
      DatanodeStorageInfo[] storages) {
    return storages == null ? null: toDatanodeInfos(Arrays.asList(storages));
  }

  /**
   * 将DatanodeStorageInfo列表转换为对应DatanodeInfo数组
   * @param storages 输入的存储信息列表
   * @return 转换后的DatanodeInfo数组
   */
  static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo> storages) {
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
    for(int i = 0; i < storages.size(); i++) {
      datanodes[i] = storages.get(i).getDatanodeDescriptor();
    }
    return datanodes;
  }

  /**
   * 将DatanodeStorageInfo数组转换为对应DatanodeDescriptor数组
   * @param storages 输入的存储信息数组
   * @return 转换后的DatanodeDescriptor数组
   */
  static DatanodeDescriptor[] toDatanodeDescriptors(
      DatanodeStorageInfo[] storages) {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[storages.length];
    for (int i = 0; i < storages.length; ++i) {
      datanodes[i] = storages[i].getDatanodeDescriptor();
    }
    return datanodes;
  }

  /**
   * 将DatanodeStorageInfo数组转换为对应存储ID数组
   * @param storages 输入的存储信息数组
   * @return 转换后的存储ID数组
   */
  public static String[] toStorageIDs(DatanodeStorageInfo[] storages) {
    if (storages == null) {
      return null;
    }
    String[] storageIDs = new String[storages.length];
    for(int i = 0; i < storageIDs.length; i++) {
      storageIDs[i] = storages[i].getStorageID();
    }
    return storageIDs;
  }

  /**
   * 将DatanodeStorageInfo数组转换为对应存储类型数组
   * @param storages 输入的存储信息数组
   * @return 转换后的存储类型数组
   */
  public static StorageType[] toStorageTypes(DatanodeStorageInfo[] storages) {
    if (storages == null) {
      return null;
    }
    StorageType[] storageTypes = new StorageType[storages.length];
    for(int i = 0; i < storageTypes.length; i++) {
      storageTypes[i] = storages[i].getStorageType();
    }
    return storageTypes;
  }

  /**
   * 从传入的DatanodeStorage对象更新当前存储的状态和类型
   * @param storage 数据源DatanodeStorage对象
   */
  public void updateFromStorage(DatanodeStorage storage) {
    state = storage.getState();
    storageType = storage.getStorageType();
  }

  /**
   * 迭代器实现，用于遍历当前存储卷上的所有数据块
   */
  class BlockIterator implements Iterator<BlockInfo> {
    private BlockInfo current;

    BlockIterator(BlockInfo head) {
      this.current = head;
    }

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public BlockInfo next() {
      BlockInfo res = current;
      current =
          current.getNext(current.findStorageInfo(DatanodeStorageInfo.this));
      return res;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Sorry. can't remove.");
    }
  }

  private final DatanodeDescriptor dn;
  private final String storageID;
  private StorageType storageType;
  private State state;

  private long capacity;
  private long dfsUsed;
  private long nonDfsUsed;
  private volatile long remaining;
  private long blockPoolUsed;

  private volatile BlockInfo blockList = null;
  private int numBlocks = 0;

  /** 已接收块报告的计数 */
  private int blockReportCount = 0;

  /** 标识NameNode自该存储启动以来是否已经接收到块报告 */
  private boolean hasReceivedBlockReport = false;

  /**
   * 在NameNode故障切换时被设置为false，接收到块报告后重置为true
   */
  private boolean heartbeatedSinceFailover = false;

  /**
   * 在启动或故障切换后，存储内容被标记为 stale（过期）直到接收到完整块报告。
   * 当存储为stale状态时，其上所有副本都视为过期，不会处理该块的无效化操作，解决故障切换后的数据一致性问题。
   */
  private boolean blockContentsStale = true;

  DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
    this(dn, s.getStorageID(), s.getStorageType(), s.getState());
  }

  DatanodeStorageInfo(DatanodeDescriptor dn, String storageID,
      StorageType storageType, State state) {
    this.dn = dn;
    this.storageID = storageID;
    this.storageType = storageType;
    this.state = state;
  }

  /**
   * 获取已接收块报告的计数
   * @return 块报告计数
   */
  public int getBlockReportCount() {
    return blockReportCount;
  }

  /**
   * 判断是否已经接收到该存储的块报告
   * @return 是否已接收块报告
   */
  boolean hasReceivedBlockReport() {
    return hasReceivedBlockReport;
  }

  /**
   * 设置块报告计数
   * @param blockReportCount 新的计数
   */
  void setBlockReportCount(int blockReportCount) {
    this.blockReportCount = blockReportCount;
  }

  /**
   * 判断当前存储的块内容是否为过期 stale 状态
   * @return 是否过期
   */
  public boolean areBlockContentsStale() {
    return blockContentsStale;
  }

  @VisibleForTesting
  /**
   * 设置块内容过期状态，仅用于测试
   * @param value 新的过期状态
   */
  public void setBlockContentsStale(boolean value) {
    blockContentsStale = value;
  }

  /**
   * 在故障切换后标记存储为过期状态
   */
  void markStaleAfterFailover() {
    heartbeatedSinceFailover = false;
    blockContentsStale = true;
  }

  /**
   * 处理接收心跳的存储报告，更新容量使用状态
   * @param report DataNode发送的存储报告
   */
  void receivedHeartbeat(StorageReport report) {
    updateState(report);
    heartbeatedSinceFailover = true;
  }

  /**
   * 处理接收块报告，更新过期状态和计数
   */
  void receivedBlockReport() {
    if (heartbeatedSinceFailover) {
      blockContentsStale = false;
    }
    blockReportCount++;
    hasReceivedBlockReport = true;
  }

  @VisibleForTesting
  /**
   * 设置存储容量使用统计，仅用于测试
   * @param capacity 总容量
   * @param dfsUsed HDFS已用容量
   * @param remaining 剩余容量
   * @param blockPoolUsed 块池已用容量
   */
  public void setUtilizationForTesting(long capacity, long dfsUsed,
                      long remaining, long blockPoolUsed) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
  }

  /**
   * 获取存储当前状态
   * @return 存储状态
   */
  State getState() {
    return this.state;
  }

  /**
   * 设置存储状态
   * @param state 新状态
   */
  void setState(State state) {
    this.state = state;
  }

  /**
   * 设置故障切换后已接收心跳标记
   * @param value 标记值
   */
  void setHeartbeatedSinceFailover(boolean value) {
    heartbeatedSinceFailover = value;
  }

  /**
   * 判断当前存储已失败且仍有块存在
   * @return 是否失败存储包含块
   */
  boolean areBlocksOnFailedStorage() {
    return getState() == State.FAILED && numBlocks != 0;
  }

  @VisibleForTesting
  /**
   * 获取存储ID
   * @return 存储ID
   */
  public String getStorageID() {
    return storageID;
  }

  /**
   * 获取存储类型
   * @return 存储类型
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * 获取存储总容量
   * @return 总容量
   */
  long getCapacity() {
    return capacity;
  }

  /**
   * 获取HDFS已用容量
   * @return HDFS已用容量
   */
  long getDfsUsed() {
    return dfsUsed;
  }

  /**
   * 获取非HDFS已用容量
   * @return 非HDFS已用容量
   */
  long getNonDfsUsed() {
    return nonDfsUsed;
  }

  /**
   * 获取剩余可用容量
   * @return 剩余容量
   */
  long getRemaining() {
    return remaining;
  }

  /**
   * 获取块池已用容量
   * @return 块池已用容量
   */
  long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  /**
   * 向当前存储添加数据块，处理同一DataNode不同存储的块迁移情况
   * @param b 待添加的块元数据
   * @param reportedBlock DataNode上报的块信息
   * @return 添加结果状态
   */
  public AddBlockResult addBlock(BlockInfo b, Block reportedBlock) {
    // 首先检查该块是否已经存在于同一DataNode的其他存储上
    AddBlockResult result = AddBlockResult.ADDED;
    DatanodeStorageInfo otherStorage =
        b.findStorageInfo(getDatanodeDescriptor());

    if (otherStorage != null) {
      if (otherStorage != this) {
        // 块存在于同一DataNode的其他存储，先移除旧关联
        otherStorage.removeBlock(b);
        result = AddBlockResult.REPLACED;
      } else {
        // 块已经关联到当前存储
        return AddBlockResult.ALREADY_EXIST;
      }
    }

    // 将块添加到当前存储链表头部
    b.addStorage(this, reportedBlock);
    insertToList(b);
    return result;
  }

  /**
   * 向当前存储添加数据块，使用块自身作为上报信息
   * @param b 待添加的块元数据
   * @return 添加结果状态
   */
  AddBlockResult addBlock(BlockInfo b) {
    return addBlock(b, b);
  }

  /**
   * 将块插入到当前存储的块链表中
   * @param b 待插入的块
   */
  public void insertToList(BlockInfo b) {
    blockList = b.listInsert(blockList, this);
    numBlocks++;
  }

  /**
   * 从当前存储移除指定块
   * @param b 待移除的块
   * @return 是否移除成功
   */
  boolean removeBlock(BlockInfo b) {
    blockList = b.listRemove(blockList, this);
    if (b.removeStorage(this)) {
      numBlocks--;
      return true;
    } else {
      return false;
    }
  }

  /**
   * 获取当前存储上的块数量
   * @return 块数量
   */
  int numBlocks() {
    return numBlocks;
  }

  /**
   * 获取当前存储上所有块的迭代器
   * @return 块迭代器
   */
  Iterator<BlockInfo> getBlockIterator() {
    return new BlockIterator(blockList);
  }

  /**
   * 将块移动到存储块链表的头部，优化块遍历性能（最近访问的块先被找到）
   * @param b 待移动的块
   * @param curIndex 当前块在链表中的索引
   * @param headIndex 链表头部索引
   * @return 原位置索引
   */
  int moveBlockToHead(BlockInfo b, int curIndex, int headIndex) {
    blockList = b.moveBlockToHead(blockList, this, curIndex, headIndex);
    return curIndex;
  }


  /**
   * 获取块链表头节点，仅用于测试
   * @return 块链表头
   */
  @VisibleForTesting
  BlockInfo getBlockListHeadForTesting(){
    return blockList;
  }

  /**
   * 从存储报告更新存储容量使用统计信息
   * @param r DataNode发送的存储报告
   */
  void updateState(StorageReport r) {
    capacity = r.getCapacity();
    dfsUsed = r.getDfsUsed();
    nonDfsUsed = r.getNonDfsUsed();
    remaining = r.getRemaining();
    blockPoolUsed = r.getBlockPoolUsed();
  }

  /**
   * 获取该存储所属的DataNode描述符
   * @return DataNode描述符
   */
  public DatanodeDescriptor getDatanodeDescriptor() {
    return dn;
  }

  /**
   * 增加指定存储的已调度块计数，用于配额和负载统计
   * @param storages 目标存储数组
   */
  public static void incrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.getDatanodeDescriptor().incrementBlocksScheduled(s.getStorageType());
    }
  }

  /**
   * 减少指定存储的已调度块计数，在块被放弃或删除时调用
   * @param storages 目标存储数组
   */
  public static void decrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.getDatanodeDescriptor().decrementBlocksScheduled(s.getStorageType());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof DatanodeStorageInfo)) {
      return false;
    }
    final DatanodeStorageInfo that = (DatanodeStorageInfo)obj;
    return this.storageID.equals(that.storageID);
  }

  @Override
  public int hashCode() {
    return storageID.hashCode();
  }

  @Override
  public String toString() {
    return "[" + storageType + "]" + storageID + ":" + state + ":" + dn;
  }
  
  /**
   * 转换为StorageReport对象
   * @return 存储报告对象
   */
  StorageReport toStorageReport() {
    return new StorageReport(
        new DatanodeStorage(storageID, state, storageType),
        false, capacity, dfsUsed, remaining, blockPoolUsed,