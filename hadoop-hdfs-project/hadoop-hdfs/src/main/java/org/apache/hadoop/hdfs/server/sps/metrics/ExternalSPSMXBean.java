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
package org.apache.hadoop.hdfs.server.sps.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
org.apache.hadoop.classification.InterfaceStability;

/**
 * External SPS（外部存储策略满足器）的JMX管理接口，用于暴露监控指标。
 * 终端用户不应该自行实现该接口，应通过JMX API获取对应监控信息。
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface ExternalSPSMXBean {

  /**
   * 获取需要存储移动任务的处理队列长度。
   * 该指标反映当前积压等待处理的移动任务数量，用于监控系统负载。
   *
   * @return 需要存储移动任务的处理队列长度
   */
  int getProcessingQueueSize();

  /**
   * 获取已完成移动的块总数。
   * 该指标反映SPS累计完成的数据块存储移动任务量，用于统计处理进度。
   *
   * @return 已完成移动的数据块总数
   */
  int getMovementFinishedBlocksCount();

  /**
   * 获取已尝试处理的移动任务总数。
   * 该指标包含成功和失败的尝试，用于统计总处理吞吐量和计算成功率。
   *
   * @return 已尝试处理的移动任务总数
   */
  int getAttemptedItemsCount();
}