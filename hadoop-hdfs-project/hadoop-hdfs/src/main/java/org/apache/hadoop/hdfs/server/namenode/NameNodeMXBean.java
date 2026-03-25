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

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;

/**
 * NameNode的JMX管理接口，用于暴露NameNode运行状态指标供监控系统通过JMX获取。
 * 终端用户不应直接实现此接口，应通过标准JMX API访问其中暴露的监控信息。
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface NameNodeMXBean {

  /**
   * 获取Hadoop版本号。
   * 
   * @return Hadoop版本字符串
   */
  String getVersion();

  /**
   * 获取当前NameNode运行的软件版本号。
   *
   * @return 表示版本号的字符串
   */
  String getSoftwareVersion();

  /**
   * 获取所有DataNode已使用的存储空间总和。
   * 
   * @return DataNode已使用空间（字节）
   */
  long getUsed();
  
  /**
   * 获取所有DataNode剩余可用存储空间总和。
   * 
   * @return DataNode剩余空间（字节）
   */
  long getFree();
  
  /**
   * 获取所有DataNode总存储空间（包含非DFS占用空间）。
   * 
   * @return DataNode总原始空间（字节）
   */
  long getTotal();

  /**
   * 获取第三方提供存储的总容量（字节）。
   *
   * @return 提供存储的总容量（字节）
   */
  long getProvidedCapacity();

  /**
   * 获取当前安全模式状态。
   * 
   * @return 安全模式状态描述
   */
  String getSafemode();
  
  /**
   * 检查系统升级是否已完成。
   * 
   * @return true表示升级已完成，false表示升级未完成
   */
  boolean isUpgradeFinalized();

  /**
   * 获取滚动升级信息。
   *
   * @return 如果滚动升级正在进行则返回升级信息，否则（无升级或升级已完成）返回null
   */
  RollingUpgradeInfo.Bean getRollingUpgradeStatus();

  /**
   * 获取DataNode上非DFS用途（如本地临时文件）占用的存储空间总和。
   * 
   * @return 集群非DFS占用空间（字节）
   */
  long getNonDfsUsedSpace();
  
  /**
   * 获取已用空间占总容量的百分比。
   * 
   * @return 集群已用空间百分比
   */
  float getPercentUsed();
  
  /**
   * 获取剩余空间占总容量的百分比。
   * 
   * @return 集群剩余空间百分比
   */
  float getPercentRemaining();

  /**
   * 获取DataNode已使用的缓存容量（字节）。
   *
   * @return DataNode已使用缓存（字节）
   */
  long getCacheUsed();

  /**
   * 获取DataNode总缓存容量（字节）。
   *
   * @return DataNode总缓存容量（字节）
   */
  long getCacheCapacity();
  
  /**
   * 获取当前NameNode所属块池已使用的总空间。
   *
   * @return 当前NameNode块池已使用空间（字节）
   */
  long getBlockPoolUsedSpace();
  
  /**
   * 获取块池已用空间占总容量的百分比。
   *
   * @return 块池已用空间占总容量的百分比
   */
  float getPercentBlockPoolUsed();
    
  /**
   * 获取集群总块数量。
   * 
   * @return 集群所有块总数
   */
  long getTotalBlocks();
  
  /**
   * 获取集群缺失块总数。
   * 
   * @return 集群缺失块总数
   */
  long getNumberOfMissingBlocks();
  
  /**
   * 获取集群中副本数为1的缺失块总数。
   *
   * @return 集群中副本数为1的缺失块总数
   */
  long getNumberOfMissingBlocksWithReplicationFactorOne();


  /**
   * 获取集群中分布不合理块总数。
   *
   * @return 集群中分布不合理块总数
   */
  long getNumberOfBadlyDistributedBlocks();

  /**
   * 获取集群中最高丢失风险的低冗余副本块总数。
   *
   * @return 集群中最高丢失风险的低冗余副本块总数
   */
  long getHighestPriorityLowRedundancyReplicatedBlocks();

  /**
   * 获取集群中最高丢失风险的低冗余纠删码块总数。
   *
   * @return 集群中最高丢失风险的低冗余纠删码块总数
   */
  long getHighestPriorityLowRedundancyECBlocks();

  /**
   * 获取系统中支持快照的目录总数。
   *
   * @return 系统中可快照目录总数
   */
  long getNumberOfSnapshottableDirs();

  /**
   * 获取NameNode当前线程数。
   * 
   * @return NameNode线程数
   */
  int getThreads();

  /**
   * 获取集群中存活节点信息。
   * 
   * @return 存活节点信息字符串
   */
  String getLiveNodes();
  
  /**
   * 获取集群中宕机节点信息。
   * 
   * @return 宕机节点信息字符串
   */
  String getDeadNodes();
  
  /**
   * 获取集群中正在退役节点信息。
   * 
   * @return 正在退役节点信息字符串
   */
  String getDecomNodes();

  /**
   * 获取正在进入维护状态节点信息。
   *
   * @return 进入维护状态节点信息字符串
   */
  String getEnteringMaintenanceNodes();

  /**
   * 获取集群ID。
   * 
   * @return 集群ID字符串
   */
  String getClusterId();
  
  /**
   * 获取块池ID。
   * 
   * @return 块池ID字符串
   */
  String getBlockPoolId();

  /**
   * 获取NameNode存储镜像和编辑日志目录的状态信息。
   * 
   * @return 名称目录状态信息，JSON格式字符串
   */
  String getNameDirStatuses();

  /**
   * 获取DataNode磁盘使用率的最大值、中位数、最小值和标准差。
   *
   * @return DataNode使用率信息，JSON格式字符串
   */
  String getNodeUsage();

  /**
   * 获取NameNode编辑日志Journal的状态信息。
   *
   * @return NameNode Journal状态信息，JSON格式字符串
   */
  String getNameJournalStatus();
  
  /**
   * 获取Journal事务ID信息，包含最后应用的事务ID和最近一次检查点的事务ID。
   *
   * @return 事务ID信息字符串
   */
  String getJournalTransactionInfo();

  /**
   * 获取NameNode启动时间戳（毫秒）。
   *
   * @return NameNode启动时间（毫秒）
   */
  long getNNStartedTimeInMillis();

  /**
   * 获取编译信息，包含编译日期、编译用户、分支信息。
   *
   * @return 编译信息，JSON格式字符串
   */
  String getCompileInfo();

  /**
   * 获取损坏文件列表。
   *
   * @return 损坏文件列表，JSON格式字符串
   */
  String getCorruptFiles();

  /**
   * 获取损坏文件列表长度。
   *
   * @return 损坏文件总数
   */
  int getCorruptFilesCount();

  /**
   * 获取存活DataNode的不同版本数量。
   * 
   * @return 存活DataNode不同版本的数量
   */
  int getDistinctVersionCount();

  /**
   * 获取每个版本对应的存活DataNode数量。
   * 
   * @return 版本号到存活节点数的映射表
   */
  Map<String, Integer> getDistinctVersions();
  
  /**
   * 获取NameNode目录总大小。
   *
   * @return NameNode目录大小信息字符串
   */
  String getNameDirSize();

  /**
   * 验证集群拓扑是否支持当前所有启用的纠删码策略。
   *
   * @return 验证结果字符串
   */
  String getVerifyECWithTopologyResult();

}