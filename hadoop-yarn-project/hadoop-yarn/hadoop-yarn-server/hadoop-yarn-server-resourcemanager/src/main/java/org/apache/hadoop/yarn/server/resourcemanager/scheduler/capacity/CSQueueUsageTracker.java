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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;

/**
 * 容量调度器队列资源使用跟踪器，负责维护队列核心指标、资源使用统计和配额信息。
 * 跟踪队列中容器数量、应用提交时间、各类资源使用情况以及资源配额配置。
 */
public class CSQueueUsageTracker {
  private final CSQueueMetrics metrics;
  private int numContainers;

  /**
   * The timestamp of the last submitted application to this queue.
   * Only applies to dynamic queues.
   */
  private long lastSubmittedTimestamp;

  /**
   * Tracks resource usage by label like used-resource / pending-resource.
   */
  private final ResourceUsage queueUsage;

  private final QueueResourceQuotas queueResourceQuotas;

  /**
   * 构造容量调度器队列使用跟踪器，初始化资源统计和配额对象。
   * @param metrics 队列指标收集器
   */
  public CSQueueUsageTracker(CSQueueMetrics metrics) {
    this.metrics = metrics;
    this.queueUsage = new ResourceUsage();
    this.queueResourceQuotas = new QueueResourceQuotas();
  }

  /**
   * 获取当前队列运行的容器总数。
   * @return 容器数量
   */
  public int getNumContainers() {
    return numContainers;
  }

  /**
   * 原子增加队列容器计数。
   */
  public synchronized void increaseNumContainers() {
    numContainers++;
  }

  /**
   * 原子减少队列容器计数。
   */
  public synchronized void decreaseNumContainers() {
    numContainers--;
  }

  /**
   * 获取队列指标收集器。
   * @return 队列指标对象
   */
  public CSQueueMetrics getMetrics() {
    return metrics;
  }

  /**
   * 获取队列最近一次提交应用的时间戳（仅动态队列有效）。
   * @return 最近提交时间戳
   */
  public long getLastSubmittedTimestamp() {
    return lastSubmittedTimestamp;
  }

  /**
   * 设置队列最近一次提交应用的时间戳。
   * @param lastSubmittedTimestamp 时间戳
   */
  public void setLastSubmittedTimestamp(long lastSubmittedTimestamp) {
    this.lastSubmittedTimestamp = lastSubmittedTimestamp;
  }

  /**
   * 获取队列按节点标签划分的资源使用统计对象。
   * @return 资源使用对象，包含已使用、待分配等资源统计
   */
  public ResourceUsage getQueueUsage() {
    return queueUsage;
  }

  /**
   * 获取队列资源配额配置对象。
   * @return 资源配额对象，包含最小、最大资源配额等配置
   */
  public QueueResourceQuotas getQueueResourceQuotas() {
    return queueResourceQuotas;
  }

}