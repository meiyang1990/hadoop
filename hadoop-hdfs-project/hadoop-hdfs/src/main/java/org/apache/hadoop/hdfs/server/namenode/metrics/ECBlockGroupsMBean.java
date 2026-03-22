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
 * 纠删码块组相关监控指标MBean接口，用于通过JMX暴露NameNode中纠删码条带化块的统计信息
 * <p>
 * 本接口定义了获取FSNamesystem中所有纠删码块组状态指标的方法，供JMX发布监控指标使用。
 * 全块聚合状态在 {@link FSNamesystemMBean} 中报告，NameNode运行时统计在
 * {@link org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics} 中报告。
 */
@InterfaceAudience.Private
public interface ECBlockGroupsMBean {
  /**
   * 获取冗余度不足的纠删码块组数量
   * @return 冗余度低于要求的纠删码块组总数
   */
  long getLowRedundancyECBlockGroups();

  /**
   * 获取损坏的纠删码块组数量
   * @return 已损坏的纠删码块组总数
   */
  long getCorruptECBlockGroups();

  /**
   * 获取丢失数据块的纠删码块组数量
   * @return 数据块丢失的纠删码块组总数
   */
  long getMissingECBlockGroups();

  /**
   * 获取处于未来状态（即将完成写入）的纠删码块组总字节数
   * @return 未来状态纠删码块组的总字节数
   */
  long getBytesInFutureECBlockGroups();

  /**
   * 获取待删除的纠删码块数量
   * @return 等待删除的纠删码块总数
   */
  long getPendingDeletionECBlocks();

  /**
   * 获取集群中总的纠删码块组数量
   * @return 所有纠删码块组的总数
   */
  long getTotalECBlockGroups();

  /**
   * 获取当前集群启用的所有纠删码策略，以逗号分隔
   * @return 逗号分隔的已启用纠删码策略名称字符串
   */
  String getEnabledEcPolicies();
}