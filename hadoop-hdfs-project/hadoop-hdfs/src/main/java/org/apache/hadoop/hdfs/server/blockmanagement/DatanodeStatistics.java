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

import java.util.Map;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

/**
 * Datanode统计信息接口，定义了获取HDFS集群所有DataNode聚合统计信息的方法。
 * 用于提供集群容量、存储使用、心跳等全局统计数据，支撑监控显示和资源调度决策。
 */
public interface DatanodeStatistics {

  /**
   * 获取所有在线DataNode的总存储容量。
   * @return 总容量，单位字节
   */
  public long getCapacityTotal();

  /**
   * 获取所有在线DataNode已使用的存储容量。
   * @return 已使用容量，单位字节
   */
  public long getCapacityUsed();

  /**
   * 获取已使用容量占总容量的百分比。
   * @return 已使用容量百分比
   */
  public float getCapacityUsedPercent();

  /**
   * 获取所有在线DataNode剩余的存储容量。
   * @return 剩余容量，单位字节
   */
  public long getCapacityRemaining();

  /**
   * 获取剩余容量占总容量的百分比。
   * @return 剩余容量百分比
   */
  public float getCapacityRemainingPercent();

  /**
   * 获取块池已使用的存储容量。
   * @return 块池已使用容量，单位字节
   */
  public long getBlockPoolUsed();

  /**
   * 获取块池已使用容量占总容量的百分比。
   * @return 块池已使用容量百分比
   */
  public float getPercentBlockPoolUsed();
  
  /**
   * 获取所有DataNode的总缓存容量。
   * @return 总缓存容量，单位字节
   */
  public long getCacheCapacity();

  /**
   * 获取所有DataNode已使用的缓存容量。
   * @return 已使用缓存容量，单位字节
   */
  public long getCacheUsed();

  /**
   * 获取当前所有DataNode的数据传输线程数总和。
   * @return 数据传输线程总数
   */
  public int getXceiverCount();

  /**
   * 获取在线（非退役/已退役）DataNode的数据传输线程平均数。
   * @return 在线节点平均数据传输线程数
   */
  public int getInServiceXceiverCount();
  
  /**
   * 获取在线（非退役/已退役）DataNode的数量。
   * @return 在线DataNode节点数量
   */
  public int getNumDatanodesInService();

  /**
   * 获取在线节点可写卷的平均数量。
   * @return 在线节点平均可写卷数量
   */
  int getInServiceAvailableVolumeCount();

  /**
   * 获取DataNode上非DFS用途占用的存储容量（比如本地临时文件等）。
   * @return 非DFS使用的容量，单位字节
   */
  public long getCapacityUsedNonDFS();

  /**
   * 获取兼容ClientProtocol的统计数组，块相关统计项设为-1。
   * 兼容旧版本客户端获取集群统计信息的接口。
   * @return 统计数组，格式与ClientProtocol.getStats()保持一致
   */
  public long[] getStats();

  /**
   * 获取过期心跳的数量，即错过心跳上报的节点数量。
   * @return 过期心跳数量
   */
  public int getExpiredHeartbeats();

  /**
   * 获取不同存储层级的统计信息。
   * @return 存储类型到对应统计信息的映射
   */
  Map<StorageType, StorageTypeStats> getStorageTypeStats();

  /**
   * 获取所有DataNode提供的外部存储总容量。
   * @return 外部提供的总容量，单位字节
   */
  public long getProvidedCapacity();
}