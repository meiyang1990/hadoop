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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;

/**
 * YARN资源调度器分区（节点标签）队列指标实现
 * 扩展QueueMetrics，支持按节点标签分区统计队列调度指标
 */
@Metrics(context = "yarn")
public class PartitionQueueMetrics extends QueueMetrics {

  // 当前指标所属的分区（节点标签）名称
  private String partition;

  /**
   * 构造分区队列指标实例
   * @param ms 指标系统实例
   * @param queueName 队列名称
   * @param parent 父队列
   * @param enableUserMetrics 是否开启用户级别指标
   * @param conf 配置对象
   * @param partition 分区（节点标签）名称
   */
  protected PartitionQueueMetrics(MetricsSystem ms, String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf,
      String partition) {
    super(ms, queueName, parent, enableUserMetrics, conf);
    this.partition = partition;
    // 如果存在父队列，关联父队列对应分区的指标
    if (getParentQueue() != null) {
      String newQueueName = (getParentQueue() instanceof CSQueue)
          ? ((CSQueue) getParentQueue()).getQueuePath()
          : getParentQueue().getQueueName();
      // 构建父分区指标名称：分区名+分隔符+父队列路径/名称
      String parentMetricName =
          partition + METRIC_NAME_DELIMITER + newQueueName;
      // 从缓存获取父分区指标并设置关联
      setParent(getQueueMetrics().get(parentMetricName));
      storedPartitionMetrics = null;
    }
  }

  /**
   * Partition * Queue * User Metrics
   *
   * Computes Metrics at Partition (Node Label) * Queue * User Level.
   *
   * Sample JMX O/P Structure:
   *
   * PartitionQueueMetrics (labelX)
   *  QueueMetrics (A)
   *    usermetrics
   *  QueueMetrics (A1)
   *    usermetrics
   *    QueueMetrics (A2)
   *      usermetrics
   *    QueueMetrics (B)
   *      usermetrics
   *
   * @return QueueMetrics
   */
  @Override
  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }

    // 默认分区使用特殊JMX名称，避免特殊字符问题
    String partitionJMXStr =
        (partition.equals(DEFAULT_PARTITION)) ? DEFAULT_PARTITION_JMX_STR
            : partition;

    // 从缓存获取用户对应分区的指标
    QueueMetrics metrics = (PartitionQueueMetrics) users.get(userName);
    // 指标不存在则创建并注册到指标系统
    if (metrics == null) {
      metrics = new PartitionQueueMetrics(this.metricsSystem, this.queueName,
          null, false, this.conf, this.partition);
      users.put(userName, metrics);
      // 注册指标，添加分区、队列、用户标签用于JMX展示
      metricsSystem.register(
          pSourceName(partitionJMXStr).append(qSourceName(queueName))
              .append(",user=").append(userName).toString(),
          "Metrics for user '" + userName + "' in queue '" + queueName + "'",
          ((PartitionQueueMetrics) metrics.tag(PARTITION_INFO, partitionJMXStr)
              .tag(QUEUE_INFO, queueName)).tag(USER_INFO, userName));
    }
    return metrics;
  }
}