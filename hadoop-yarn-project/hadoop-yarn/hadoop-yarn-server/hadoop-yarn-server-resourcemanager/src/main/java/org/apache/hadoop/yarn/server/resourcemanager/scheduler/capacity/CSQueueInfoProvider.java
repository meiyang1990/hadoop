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

import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 容量调度器队列信息提供者，负责从容量调度器队列对象转换生成标准API层队列信息对象。
 * 封装队列信息、统计数据、按标签分组的配置信息的转换逻辑，供REST API和客户端查询使用。
 */
public final class CSQueueInfoProvider {

  private static final RecordFactory RECORD_FACTORY =
          RecordFactoryProvider.getRecordFactory(null);

  private CSQueueInfoProvider() {
  }

  /**
   * 从容量调度器队列对象生成标准队列信息对象。
   * @param csQueue 容量调度器抽象队列对象
   * @return 填充完成的标准API队列信息对象
   */
  public static QueueInfo getQueueInfo(AbstractCSQueue csQueue) {
    QueueInfo queueInfo = RECORD_FACTORY.newRecordInstance(QueueInfo.class);
    // 设置调度器类型为容量调度器
    queueInfo.setSchedulerType("CapacityScheduler");
    // 设置队列叶子名称（路径最后一段）
    queueInfo.setQueueName(csQueue.getQueuePathObject().getLeafName());
    // 设置队列完整路径
    queueInfo.setQueuePath(csQueue.getQueuePathObject().getFullPath());
    // 设置队列可访问节点标签
    queueInfo.setAccessibleNodeLabels(csQueue.getAccessibleNodeLabels());
    // 设置队列配置容量
    queueInfo.setCapacity(csQueue.getCapacity());
    // 设置队列最大容量限制
    queueInfo.setMaximumCapacity(csQueue.getMaximumCapacity());
    // 设置队列运行状态
    queueInfo.setQueueState(csQueue.getState());
    // 设置队列默认节点标签表达式
    queueInfo.setDefaultNodeLabelExpression(csQueue.getDefaultNodeLabelExpression());
    // 设置当前已使用容量百分比
    queueInfo.setCurrentCapacity(csQueue.getUsedCapacity());
    // 设置队列运行统计信息
    queueInfo.setQueueStatistics(getQueueStatistics(csQueue));
    // 设置是否禁用抢占
    queueInfo.setPreemptionDisabled(csQueue.getPreemptionDisabled());
    // 设置是否禁用队列内抢占
    queueInfo.setIntraQueuePreemptionDisabled(
            csQueue.getIntraQueuePreemptionDisabled());
    // 设置按节点标签分组的队列配置信息
    queueInfo.setQueueConfigurations(getQueueConfigurations(csQueue));
    // 设置队列权重
    queueInfo.setWeight(csQueue.getQueueCapacities().getWeight());
    // 设置队列最大并行应用数限制
    queueInfo.setMaxParallelApps(csQueue.getMaxParallelApps());
    return queueInfo;
  }

  /**
   * 从队列监控指标生成标准队列统计信息对象。
   * @param csQueue 容量调度器抽象队列对象
   * @return 填充完成的标准队列统计信息对象
   */
  private static QueueStatistics getQueueStatistics(AbstractCSQueue csQueue) {
    QueueStatistics stats = RECORD_FACTORY.newRecordInstance(
            QueueStatistics.class);
    // 获取队列监控指标对象
    CSQueueMetrics queueMetrics = csQueue.getMetrics();
    // 设置已提交应用数
    stats.setNumAppsSubmitted(queueMetrics.getAppsSubmitted());
    // 设置运行中应用数
    stats.setNumAppsRunning(queueMetrics.getAppsRunning());
    // 设置等待中应用数
    stats.setNumAppsPending(queueMetrics.getAppsPending());
    // 设置已完成应用数
    stats.setNumAppsCompleted(queueMetrics.getAppsCompleted());
    // 设置已杀死应用数
    stats.setNumAppsKilled(queueMetrics.getAppsKilled());
    // 设置失败应用数
    stats.setNumAppsFailed(queueMetrics.getAppsFailed());
    // 设置活跃用户数
    stats.setNumActiveUsers(queueMetrics.getActiveUsers());
    // 设置可用内存大小（MB）
    stats.setAvailableMemoryMB(queueMetrics.getAvailableMB());
    // 设置已分配内存大小（MB）
    stats.setAllocatedMemoryMB(queueMetrics.getAllocatedMB());
    // 设置待分配内存大小（MB）
    stats.setPendingMemoryMB(queueMetrics.getPendingMB());
    // 设置预留内存大小（MB）
    stats.setReservedMemoryMB(queueMetrics.getReservedMB());
    // 设置可用虚拟CPU核数
    stats.setAvailableVCores(queueMetrics.getAvailableVirtualCores());
    // 设置已分配虚拟CPU核数
    stats.setAllocatedVCores(queueMetrics.getAllocatedVirtualCores());
    // 设置待分配虚拟CPU核数
    stats.setPendingVCores(queueMetrics.getPendingVirtualCores());
    // 设置预留虚拟CPU核数
    stats.setReservedVCores(queueMetrics.getReservedVirtualCores());
    // 设置待分配容器数
    stats.setPendingContainers(queueMetrics.getPendingContainers());
    // 设置已分配容器数
    stats.setAllocatedContainers(queueMetrics.getAllocatedContainers());
    // 设置预留容器数
    stats.setReservedContainers(queueMetrics.getReservedContainers());
    return stats;
  }

  /**
   * 获取按节点标签分组的队列配置信息。
   * @param csQueue 容量调度器抽象队列对象
   * @return 节点标签到对应队列配置的映射
   */
  private static Map<String, QueueConfigurations> getQueueConfigurations(AbstractCSQueue csQueue) {
    Map<String, QueueConfigurations> queueConfigurations = new HashMap<>();
    // 获取队列关联的所有节点标签
    Set<String> nodeLabels = csQueue.getNodeLabelsForQueue();
    // 获取队列资源配额对象
    QueueResourceQuotas queueResourceQuotas = csQueue.getQueueResourceQuotas();
    // 遍历每个节点标签，生成对应配置
    for (String nodeLabel : nodeLabels) {
      QueueConfigurations queueConfiguration =
              RECORD_FACTORY.newRecordInstance(QueueConfigurations.class);
      // 获取队列容量对象
      QueueCapacities queueCapacities = csQueue.getQueueCapacities();
      // 获取当前标签下的队列配置容量
      float capacity = queueCapacities.getCapacity(nodeLabel);
      // 获取当前标签下的绝对容量（相对于集群总容量）
      float absoluteCapacity = queueCapacities.getAbsoluteCapacity(nodeLabel);
      // 获取当前标签下的最大容量
      float maxCapacity = queueCapacities.getMaximumCapacity(nodeLabel);
      // 获取当前标签下的绝对最大容量
      float absMaxCapacity =
              queueCapacities.getAbsoluteMaximumCapacity(nodeLabel);
      // 获取当前标签下的ApplicationMaster资源最大占比
      float maxAMPercentage =
              queueCapacities.getMaxAMResourcePercentage(nodeLabel);
      // 设置容量相关配置
      queueConfiguration.setCapacity(capacity);
      queueConfiguration.setAbsoluteCapacity(absoluteCapacity);
      queueConfiguration.setMaxCapacity(maxCapacity);
      queueConfiguration.setAbsoluteMaxCapacity(absMaxCapacity);
      queueConfiguration.setMaxAMPercentage(maxAMPercentage);
      // 设置配置指定的最小资源配额
      queueConfiguration.setConfiguredMinCapacity(
              queueResourceQuotas.getConfiguredMinResource(nodeLabel));
      // 设置配置指定的最大资源配额
      queueConfiguration.setConfiguredMaxCapacity(
              queueResourceQuotas.getConfiguredMaxResource(nodeLabel));
      // 设置生效的最小资源配额（受父队列限制）
      queueConfiguration.setEffectiveMinCapacity(
              queueResourceQuotas.getEffectiveMinResource(nodeLabel));
      // 设置生效的最大资源配额（受父队列限制）
      queueConfiguration.setEffectiveMaxCapacity(
              queueResourceQuotas.getEffectiveMaxResource(nodeLabel));
      // 将配置存入映射
      queueConfigurations.put(nodeLabel, queueConfiguration);
    }
    return queueConfigurations;
  }
}