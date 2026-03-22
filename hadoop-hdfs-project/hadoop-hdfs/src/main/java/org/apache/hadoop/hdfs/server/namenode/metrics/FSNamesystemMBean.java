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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 文件系统名称节点FSNamesystem的JMX MBean接口，用于通过JMX暴露FSNamesystem运行状态指标
 * 遵循JMX命名规范，对外提供HDFS元数据、存储容量、数据节点状态、块复制等核心统计信息
 */
@InterfaceAudience.Private
public interface FSNamesystemMBean {

  /**
   * 获取文件系统当前运行状态
   * @return 状态名称：安全模式Safemode 或 正常运行Operational
   */
  public String getFSState();
  
  
  /**
   * 获取系统中已分配的数据块总数
   * @return 已分配块数量
   */
  public long getBlocksTotal();

  /**
   * 获取HDFS集群总存储容量
   * @return 总容量，单位字节
   */
  public long getCapacityTotal();


  /**
   * 获取HDFS集群剩余未使用存储容量
   * @return 剩余容量，单位字节
   */
  public long getCapacityRemaining();
 
  /**
   * 获取HDFS集群已使用存储容量
   * @return 已使用容量，单位字节
   */
  public long getCapacityUsed();

  /**
   * 获取提供存储（第三方缓存存储）的总容量
   * @return 提供存储总容量，单位字节
   */
  public long getProvidedCapacityTotal();

  /**
   * 获取系统中文件和目录总数量
   * @return 文件和目录总数
   */
  public long getFilesTotal();
 
  /**
   * 获取待重建块的聚合总数（已废弃）
   * @deprecated 请使用 {@link #getPendingReconstructionBlocks()} 替代
   */
  @Deprecated
  public long getPendingReplicationBlocks();

  /**
   * 获取待重建块的聚合总数
   * @return 等待复制重建的块数量
   */
  public long getPendingReconstructionBlocks();

  /**
   * 获取低冗余块的聚合总数（已废弃）
   * @deprecated 请使用 {@link #getLowRedundancyBlocks()} 替代
   */
  @Deprecated
  public long getUnderReplicatedBlocks();

  /**
   * 获取低冗余块的聚合总数
   * @return 副本数低于配置要求的块数量
   */
  public long getLowRedundancyBlocks();

  /**
   * 获取已调度等待复制的块数量
   * @return 已调度待复制的块数量
   */
  public long getScheduledReplicationBlocks();

  /**
   * 获取FSNamesystem的总负载
   * @return FSNamesystem总负载值
   */
  public int getTotalLoad();

  /**
   * 获取存活数据节点数量
   * @return 存活DataNode数量
   */
  public int getNumLiveDataNodes();
  
  /**
   * 获取死亡数据节点数量
   * @return 死亡DataNode数量
   */
  public int getNumDeadDataNodes();
  
  /**
   * 获取状态超时（ stale ）数据节点数量
   * @return 状态超时DataNode数量
   */
  public int getNumStaleDataNodes();

  /**
   * 获取已完成退役且仍存活的数据节点数量
   * @return 已退役存活DataNode数量
   */
  public int getNumDecomLiveDataNodes();

  /**
   * 获取已完成退役且死亡的数据节点数量
   * @return 已退役死亡DataNode数量
   */
  public int getNumDecomDeadDataNodes();

  /**
   * 获取正常提供服务的存活数据节点数量
   * 计算公式：NumInServiceDataNodes = NumLiveDataNodes - NumDecomLiveDataNodes - NumInMaintenanceLiveDataNodes
   * @return 正常服务的存活DataNode数量
   */
  int getNumInServiceLiveDataNodes();

  /**
   * 获取所有存活数据节点中失败磁盘卷的总数
   * @return 失败磁盘卷总数量
   */
  int getVolumeFailuresTotal();

  /**
   * 获取因磁盘卷故障损失的总容量估算值
   * @return 损失容量估算值，单位字节
   */
  long getEstimatedCapacityLostTotal();

  /**
   * 获取正在进行退役操作的数据节点数量
   * @return 正在退役的DataNode数量
   */
  public int getNumDecommissioningDataNodes();

  /**
   * 获取快照统计信息
   * @return 快照统计信息字符串
   */
  public String getSnapshotStats();

  /**
   * 获取文件系统支持的最大inode数量
   * @return 最大inode数量
   */
  public long getMaxObjects();

  /**
   * 获取等待删除的块数量
   * @return 待删除块数量
   */
  long getPendingDeletionBlocks();

  /**
   * 获取块删除任务开始时间戳
   * @return 块删除开始时间（毫秒时间戳）
   */
  long getBlockDeletionStartTime();

  /**
   * 获取内容过期存储的数量
   * @return 内容过期存储数量
   */
  public int getNumStaleStorages();

  /**
   * 获取不同时间窗口内RPC操作排名靠前用户的统计结果
   * @return 包含Top用户统计的JSON字符串
   */
  public String getTopUserOpCounts();

  /**
   * 获取系统中加密区域的数量
   * @return 加密区域数量
   */
  int getNumEncryptionZones();

  /**
   * 获取FSNamesystem锁的等待队列长度
   * 数值越大说明越多线程在等待获取FSNamesystem锁，反映元数据操作竞争程度
   * @return 等待获取FSNamesystem锁的线程数量
   */
  int getFsLockQueueLength();

  /**
   * 获取FSEditLog日志同步操作总次数
   * @return 同步操作总次数
   */
  long getTotalSyncCount();

  /**
   * 获取FSEditLog日志同步操作总耗时统计
   * @return 同步操作总耗时统计字符串
   */
  String getTotalSyncTimes();

  /**
   * 获取处于维护模式的存活数据节点数量
   * @return 维护模式存活DataNode数量
   */
  int getNumInMaintenanceLiveDataNodes();

  /**
   * 获取处于维护模式的死亡数据节点数量
   * @return 维护模式死亡DataNode数量
   */
  int getNumInMaintenanceDeadDataNodes();

  /**
   * 获取正在进入维护模式的数据节点数量
   * @return 正在进入维护模式的DataNode数量
   */
  int getNumEnteringMaintenanceDataNodes();

  /**
   * 获取当前内存中保存的代理令牌总数
   * @return 代理令牌数量
   */
  long getCurrentTokensCount();

  /**
   * 获取存储策略满足器待处理的路径数量
   * @return SPS待处理路径数量
   */
  int getPendingSPSPaths();

  /**
   * 获取块重建队列初始化进度
   * @return 初始化进度，范围0到1
   */
  float getReconstructionQueuesInitProgress();
}