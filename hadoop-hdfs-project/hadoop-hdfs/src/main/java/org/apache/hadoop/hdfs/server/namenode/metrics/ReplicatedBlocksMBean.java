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
 * HDFS NameNode中复制块（CONTIGUOUS类型）状态指标的JMX MBean接口
 * 用于通过JMX暴露复制块相关统计信息，供监控系统采集查看
 * <p>
 * 所有块的聚合状态请查看
 * @see FSNamesystemMBean
 * NameNode运行时活动统计请查看
 * @see org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics
 */
@InterfaceAudience.Private
public interface ReplicatedBlocksMBean {
  /**
   * 获取低冗余复制块数量（副本数低于要求值的块）
   * @return 低冗余复制块总数
   */
  long getLowRedundancyReplicatedBlocks();

  /**
   * 获取损坏复制块数量
   * @return 损坏复制块总数
   */
  long getCorruptReplicatedBlocks();

  /**
   * 获取缺失复制块数量（所有副本都不可用的块）
   * @return 缺失复制块总数
   */
  long getMissingReplicatedBlocks();

  /**
   * 获取副本因子为1的缺失复制块数量
   * @return 副本因子为1的缺失复制块总数
   */
  long getMissingReplicationOneBlocks();

  /**
   * 获取分布不良复制块数量（副本分布不符合机架感知策略的块）
   * @return 分布不良复制块总数
   */
  long getBadlyDistributedBlocks();

  /**
   * 获取即将提交的复制块总字节数（正在写入尚未完成的块）
   * @return 即将提交的复制块总字节数
   */
  long getBytesInFutureReplicatedBlocks();

  /**
   * 获取待删除复制块数量
   * @return 待删除复制块总数
   */
  long getPendingDeletionReplicatedBlocks();

  /**
   * 获取集群中复制块总数
   * @return 复制块总数量
   */
  long getTotalReplicatedBlocks();
}