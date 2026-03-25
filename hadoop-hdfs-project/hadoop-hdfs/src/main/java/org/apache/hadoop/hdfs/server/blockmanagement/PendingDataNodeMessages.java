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

import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.Lists;

/**
 * 文件说明：HDFS备用NameNode节点待处理数据块消息队列容器
 * 
 * 核心职责：在Standby NameNode中，缓存数据节点上报的块消息，这些消息对应的块尚未在命名空间就绪
 * 或命名空间中该块处于过时状态，等待后续处理。当命名空间就绪后，再取出这些消息进行处理。
 */
class PendingDataNodeMessages {
  
  // 按块ID分组存储待处理消息队列
  final Map<Block, Queue<ReportedBlockInfo>> queueByBlockId =
    Maps.newHashMap();
  // 待处理消息总数统计
  private int count = 0;
  
    
  /**
   * 内部类：存储数据节点上报的块信息
   * 封装数据块对应的存储信息、块对象和上报的副本状态
   */
  static class ReportedBlockInfo {
    private final Block block;
    private final DatanodeStorageInfo storageInfo;
    private final ReplicaState reportedState;

    ReportedBlockInfo(DatanodeStorageInfo storageInfo, Block block,
        ReplicaState reportedState) {
      this.storageInfo = storageInfo;
      this.block = block;
      this.reportedState = reportedState;
    }

    Block getBlock() {
      return block;
    }

    ReplicaState getReportedState() {
      return reportedState;
    }
    
    DatanodeStorageInfo getStorageInfo() {
      return storageInfo;
    }

    @Override
    public String toString() {
      return "ReportedBlockInfo [block=" + block + ", dn="
          + storageInfo.getDatanodeDescriptor()
          + ", reportedState=" + reportedState + "]";
    }
  }
  
  /**
   * 移除指定数据节点的所有待处理消息
   * 用于数据节点下线时，清理该节点所有未处理的块上报消息
   * @param dn 待移除消息的数据节点描述符
   */
  void removeAllMessagesForDatanode(DatanodeDescriptor dn) {
    // 遍历所有块的待处理队列
    for (Map.Entry<Block, Queue<ReportedBlockInfo>> entry :
        queueByBlockId.entrySet()) {
      // 创建新队列保存非目标节点的消息
      Queue<ReportedBlockInfo> newQueue = Lists.newLinkedList();
      Queue<ReportedBlockInfo> oldQueue = entry.getValue();
      // 遍历原队列中所有消息
      while (!oldQueue.isEmpty()) {
        ReportedBlockInfo rbi = oldQueue.remove();
        // 保留非目标节点的消息
        if (!rbi.getStorageInfo().getDatanodeDescriptor().equals(dn)) {
          newQueue.add(rbi);
        } else {
          // 目标节点消息移除，统计数减一
          count--;
        }
      }
      // 将过滤后的新队列放回Map
      queueByBlockId.put(entry.getKey(), newQueue);
    }
  }
  
  /**
   * 将数据节点上报的块信息加入待处理队列
   * 处理纠删码块和普通块的ID转换，将消息缓存等待后续处理
   * @param storageInfo 数据节点存储信息
   * @param block 上报的块对象
   * @param reportedState 上报的副本状态
   */
  void enqueueReportedBlock(DatanodeStorageInfo storageInfo, Block block,
      ReplicaState reportedState) {
    // 如果是纠删码块，转换块ID格式
    if (BlockIdManager.isStripedBlockID(block.getBlockId())) {
      Block blkId = new Block(BlockIdManager.convertToStripedID(block
          .getBlockId()));
      getBlockQueue(blkId).add(
          new ReportedBlockInfo(storageInfo, new Block(block), reportedState));
    } else {
      // 普通块直接拷贝对象入队
      block = new Block(block);
      getBlockQueue(block).add(
          new ReportedBlockInfo(storageInfo, block, reportedState));
    }
    // 总计数加一
    count++;
  }

  /**
   * 移除指定存储上指定块的待处理消息
   * 解决旧消息残留导致切换活跃节点后块被错误标记为损坏的问题
   * @param storageInfo 数据节点存储信息
   * @param block 待移除的块对象
   */
  void removeQueuedBlock(DatanodeStorageInfo storageInfo, Block block) {
    // 空参数检查直接返回
    if (storageInfo == null || block == null) {
      return;
    }
    Block blk = new Block(block);
    // 纠删码块转换块ID格式
    if (BlockIdManager.isStripedBlockID(block.getBlockId())) {
      blk = new Block(BlockIdManager.convertToStripedID(block
          .getBlockId()));
    }
    // 获取对应块的待处理队列
    Queue<ReportedBlockInfo> queue = queueByBlockId.get(blk);
    if (queue == null) {
      return;
    }
    // 移除该存储上的所有待处理块消息，只保留最新上报，解决HDFS-17453竞态问题
    int size = queue.size();
    if (queue.removeIf(rbi -> storageInfo.equals(rbi.storageInfo))) {
      // 更新总计数，减去移除的消息数量
      count -= (size - queue.size());
    }
    // 如果队列已经空了，从Map中移除该块条目节省空间
    if (queue.isEmpty()) {
      queueByBlockId.remove(blk);
    }
  }
  
  /**
   * 获取并移除指定块的所有待处理消息队列
   * 当块在命名空间就绪后，取出所有缓存的消息进行处理
   * @param block 目标块对象
   * @return 指定块的待处理消息队列，如果没有则返回null
   */
  Queue<ReportedBlockInfo> takeBlockQueue(Block block) {
    // 从Map中移除并获取队列
    Queue<ReportedBlockInfo> queue = queueByBlockId.remove(block);
    if (queue != null) {
      // 总计数减去队列中消息数量
      count -= queue.size();
    }
    return queue;
  }


  /**
   * 获取指定块对应的待处理消息队列，不存在则创建新队列
   * @param block 目标块对象
   * @return 对应块的待处理消息队列
   */
  private Queue<ReportedBlockInfo> getBlockQueue(Block block) {
    Queue<ReportedBlockInfo> queue = queueByBlockId.get(block);
    if (queue == null) {
      // 队列不存在，创建新链表队列并存入Map
      queue = Lists.newLinkedList();
      queueByBlockId.put(block, queue);
    }
    return queue;
  }
  
  /**
   * 获取当前所有待处理消息的总数
   * @return 待处理消息总数
   */
  int count() {
    return count ;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    // 遍历所有块的待处理队列拼接字符串
    for (Map.Entry<Block, Queue<ReportedBlockInfo>> entry :
      queueByBlockId.entrySet()) {
      sb.append("Block " + entry.getKey() + ":\n");
      for (ReportedBlockInfo rbi : entry.getValue()) {
        sb.append("  ").append(rbi).append("\n");
      }
    }
    return sb.toString();
  }

  /**
   * 获取所有待处理消息，清空队列并重置计数
   * 用于Standby切换为Active时，处理所有缓存的块上报消息
   * @return 所有待处理消息的可迭代集合
   */
  Iterable<ReportedBlockInfo> takeAll() {
    // 根据总计数初始化列表容量
    List<ReportedBlockInfo> rbis = Lists.newArrayListWithCapacity(
        count);
    // 将所有队列中的消息添加到结果列表
    for (Queue<ReportedBlockInfo> q : queueByBlockId.values()) {
      rbis.addAll(q);
    }
    // 清空所有队列，重置计数
    queueByBlockId.clear();
    count = 0;
    return rbis;
  }
}