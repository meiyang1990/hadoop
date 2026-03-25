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

import java.beans.ConstructorProperties;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;

/**
 * 存储类型容量统计类，维护HDFS集群中单个存储类型（如DISK、SSD、PROVIDED等）的容量和节点统计信息
 * 用于聚合多个数据节点同类型存储的容量数据，支持集群层面按存储类型统计资源使用
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class StorageTypeStats {
  private long capacityTotal = 0L;
  private long capacityUsed = 0L;
  private long capacityNonDfsUsed = 0L;
  private long capacityRemaining = 0L;
  private long blockPoolUsed = 0L;
  private int nodesInService = 0;
  private StorageType storageType;

  /**
   * 设置服务中节点的总Xceiver线程数，用于单元测试
   * @param avgXceiverPerDatanode 每个数据节点平均Xceiver线程数
   * @param numNodesInService 服务中数据节点数量
   */
  @VisibleForTesting
  void setDataNodesInServiceXceiverCount(int avgXceiverPerDatanode,
      int numNodesInService) {
    this.nodesInService = numNodesInService;
    this.nodesInServiceXceiverCount = numNodesInService * avgXceiverPerDatanode;
  }

  private int nodesInServiceXceiverCount;

  /**
   * 构造函数，用于JSON序列化反序列化创建存储统计对象
   * @param capacityTotal 总容量
   * @param capacityUsed 已用容量
   * @param capacityNonDfsUsedUsed 非DFS使用容量
   * @param capacityRemaining 剩余容量
   * @param blockPoolUsed 块池使用容量
   * @param nodesInService 提供该存储类型的在线节点数量
   */
  @ConstructorProperties({"capacityTotal", "capacityUsed", "capacityNonDfsUsed",
      "capacityRemaining", "blockPoolUsed", "nodesInService"})
  public StorageTypeStats(
      long capacityTotal, long capacityUsed, long capacityNonDfsUsedUsed,
      long capacityRemaining, long blockPoolUsed, int nodesInService) {
    this.capacityTotal = capacityTotal;
    this.capacityUsed = capacityUsed;
    this.capacityNonDfsUsed = capacityNonDfsUsedUsed;
    this.capacityRemaining = capacityRemaining;
    this.blockPoolUsed = blockPoolUsed;
    this.nodesInService = nodesInService;
  }

  /**
   * 获取该存储类型总容量，对PROVIDED存储做去重处理
   * @return 总容量字节数
   */
  public long getCapacityTotal() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityTotal/nodesInService;
    }
    return capacityTotal;
  }

  /**
   * 获取该存储类型已用容量，对PROVIDED存储做去重处理
   * @return 已用容量字节数
   */
  public long getCapacityUsed() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityUsed/nodesInService;
    }
    return capacityUsed;
  }

  /**
   * 获取该存储类型非DFS使用容量，对PROVIDED存储做去重处理
   * @return 非DFS使用容量字节数
   */
  public long getCapacityNonDfsUsed() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityNonDfsUsed/nodesInService;
    }
    return capacityNonDfsUsed;
  }

  /**
   * 获取该存储类型剩余容量，对PROVIDED存储做去重处理
   * @return 剩余容量字节数
   */
  public long getCapacityRemaining() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return capacityRemaining/nodesInService;
    }
    return capacityRemaining;
  }

  /**
   * 获取该存储类型块池已用容量，对PROVIDED存储做去重处理
   * @return 块池已用容量字节数
   */
  public long getBlockPoolUsed() {
    // for PROVIDED storage, avoid counting the same storage
    // across multiple datanodes
    if (storageType == StorageType.PROVIDED && nodesInService > 0) {
      return blockPoolUsed/nodesInService;
    }
    return blockPoolUsed;
  }

  /**
   * 计算存储容量使用率
   * @return 已用容量占总容量的百分比
   */
  public float getPercentUsed() {
    long used = getCapacityUsed();
    long total = getCapacityTotal();
    return DFSUtilClient.getPercentUsed(used, total);
  }

  /**
   * 计算块池容量使用率
   * @return 块池已用容量占总容量的百分比
   */
  public float getPercentBlockPoolUsed() {
    long poolUsed = getBlockPoolUsed();
    long total = getCapacityTotal();
    return DFSUtilClient.getPercentUsed(poolUsed, total);
  }

  /**
   * 计算存储容量剩余百分比
   * @return 剩余容量占总容量的百分比
   */
  public float getPercentRemaining() {
    long remaining = getCapacityRemaining();
    long total = getCapacityTotal();
    return DFSUtilClient.getPercentUsed(remaining, total);
  }

  /**
   * 获取提供该存储类型的在线节点数量
   * @return 在线数据节点数量
   */
  public int getNodesInService() {
    return nodesInService;
  }

  /**
   * 获取服务中节点的总Xceiver线程数（用于数据传输的服务线程）
   * @return 总Xceiver线程数
   */
  public int getNodesInServiceXceiverCount() {
    return nodesInServiceXceiverCount;
  }

  /**
   * 构造指定存储类型的空统计对象
   * @param storageType 存储类型
   */
  StorageTypeStats(StorageType storageType) {
    this.storageType = storageType;
  }

  /**
   * 拷贝构造函数，基于另一个统计对象创建新对象
   * @param other 要拷贝的源统计对象
   */
  StorageTypeStats(StorageTypeStats other) {
    capacityTotal = other.capacityTotal;
    capacityUsed = other.capacityUsed;
    capacityNonDfsUsed = other.capacityNonDfsUsed;
    capacityRemaining = other.capacityRemaining;
    blockPoolUsed = other.blockPoolUsed;
    nodesInService = other.nodesInService;
  }

  /**
   * 添加一个数据节点存储信息，累加容量统计
   * @param info 数据节点存储信息
   * @param node 所属数据节点描述符
   */
  void addStorage(final DatanodeStorageInfo info,
      final DatanodeDescriptor node) {
    assert storageType == info.getStorageType();
    capacityUsed += info.getDfsUsed();
    capacityNonDfsUsed += info.getNonDfsUsed();
    blockPoolUsed += info.getBlockPoolUsed();
    if (node.isInService()) {
      capacityTotal += info.getCapacity();
      capacityRemaining += info.getRemaining();
    } else {
      // 离线节点仅统计已使用的DFS容量
      capacityTotal += info.getDfsUsed();
    }
  }

  /**
   * 添加一个数据节点，更新在线节点计数和Xceiver线程总数
   * @param node 要添加的数据节点
   */
  void addNode(final DatanodeDescriptor node) {
    if (node.isInService()) {
      nodesInService++;
      nodesInServiceXceiverCount += node.getXceiverCount();
    }
  }

  /**
   * 移除一个数据节点存储信息，累减容量统计
   * @param info 数据节点存储信息
   * @param node 所属数据节点描述符
   */
  void subtractStorage(final DatanodeStorageInfo info,
      final DatanodeDescriptor node) {
    assert storageType == info.getStorageType();
    capacityUsed -= info.getDfsUsed();
    capacityNonDfsUsed -= info.getNonDfsUsed();
    blockPoolUsed -= info.getBlockPoolUsed();
    if (node.isInService()) {
      capacityTotal -= info.getCapacity();
      capacityRemaining -= info.getRemaining();
    } else {
      // 离线节点仅减去已使用的DFS容量
      capacityTotal -= info.getDfsUsed();
    }
  }

  /**
   * 移除一个数据节点，更新在线节点计数和Xceiver线程总数
   * @param node 要移除的数据节点
   */
  void subtractNode(final DatanodeDescriptor node) {
    if (node.isInService()) {
      nodesInService--;
      nodesInServiceXceiverCount -= node.getXceiverCount();
    }
  }
}