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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 容量调度器抢占工具类，提供抢占流程中通用的资源计算、容器选择、资源扣除等辅助方法
 */
public class CapacitySchedulerPreemptionUtils {
  /**
   * 按节点分区计算叶子队列需要通过抢占获取的资源量
   * @param context 抢占上下文
   * @param queueName 目标叶子队列名称
   * @param clusterResource 集群总资源
   * @return 按分区组织的需要抢占获取的资源映射
   */
  public static Map<String, Resource> getResToObtainByPartitionForLeafQueue(
      CapacitySchedulerPreemptionContext context, String queueName,
      Resource clusterResource) {
    Map<String, Resource> resToObtainByPartition = new HashMap<>();
    // 遍历队列的所有分区，计算队列间抢占后需要获取的资源
    for (TempQueuePerPartition qT : context.getQueuePartitions(queueName)) {
      if (qT.preemptionDisabled) {
        continue;
      }

      // 仅当实际需要抢占的资源大于0时才加入结果
      if (Resources.greaterThan(context.getResourceCalculator(),
          clusterResource, qT.getActuallyToBePreempted(), Resources.none())) {
        resToObtainByPartition.put(qT.partition,
            Resources.clone(qT.getActuallyToBePreempted()));
      }
    }

    return resToObtainByPartition;
  }

  /**
   * 检查容器是否已经被选为抢占候选
   * @param container 待检查容器
   * @param selectedCandidates 已选中的候选容器集合
   * @return 是否已选中
   */
  public static boolean isContainerAlreadySelected(RMContainer container,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates) {
    if (null == selectedCandidates) {
      return false;
    }

    Set<RMContainer> containers = selectedCandidates
        .get(container.getApplicationAttemptId());
    if (containers == null) {
      return false;
    }
    return containers.contains(container);
  }

  /**
   * 根据已经选中的抢占容器，扣除对应队列和应用的可抢占资源量
   * @param context 抢占上下文
   * @param selectedCandidates 已选中的抢占候选容器
   */
  public static void deductPreemptableResourcesBasedSelectedCandidates(
      CapacitySchedulerPreemptionContext context,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates) {
    // 遍历所有应用的选中容器
    for (Set<RMContainer> containers : selectedCandidates.values()) {
      for (RMContainer c : containers) {
        // 获取容器所在调度节点
        SchedulerNode schedulerNode = context.getScheduler()
            .getSchedulerNode(c.getAllocatedNode());
        if (null == schedulerNode) {
          continue;
        }

        // 获取分区和队列信息
        String partition = schedulerNode.getPartition();
        String queue = c.getQueueName();
        TempQueuePerPartition tq = context.getQueueByPartition(
            context.getScheduler().normalizeQueueName(queue),
            partition);

        // 获取容器资源（优先使用预留资源）
        Resource res = c.getReservedResource();
        if (null == res) {
          res = c.getAllocatedResource();
        }

        if (null != res) {
          // 扣除队列维度的待抢占资源
          tq.deductActuallyToBePreempted(context.getResourceCalculator(),
              tq.totalPartitionResource, res);
          Collection<TempAppPerPartition> tas = tq.getApps();
          if (null == tas || tas.isEmpty()) {
            continue;
          }

          // 扣除应用维度的待抢占资源
          deductPreemptableResourcePerApp(context, tq.totalPartitionResource,
              tas, res);
        }
      }
    }
  }

  /**
   * 扣除所有应用的待抢占资源
   * @param context 抢占上下文
   * @param totalPartitionResource 分区总资源
   * @param tas 应用分区信息集合
   * @param res 需要扣除的资源量
   */
  private static void deductPreemptableResourcePerApp(
      CapacitySchedulerPreemptionContext context,
      Resource totalPartitionResource, Collection<TempAppPerPartition> tas,
      Resource res) {
    for (TempAppPerPartition ta : tas) {
      ta.deductActuallyToBePreempted(context.getResourceCalculator(),
          totalPartitionResource, res);
    }
  }

  /**
   * 尝试抢占指定容器，并从待获取资源中扣除对应资源
   *
   * @param rc 资源计算器
   * @param context 抢占上下文
   * @param resourceToObtainByPartitions 按分区组织的待获取资源映射
   * @param rmContainer 待抢占容器
   * @param clusterResource 集群总资源
   * @param preemptMap 已选中待抢占容器集合
   * @param curCandidates 当前轮次候选容器集合
   * @param totalPreemptionAllowed 本轮允许抢占的总资源上限
   * @param conservativeDRF 是否使用保守DRF抢占策略
   * @return 是否成功抢占该容器
   */
  public static boolean tryPreemptContainerAndDeductResToObtain(
      ResourceCalculator rc, CapacitySchedulerPreemptionContext context,
      Map<String, Resource> resourceToObtainByPartitions,
      RMContainer rmContainer, Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      Resource totalPreemptionAllowed, boolean conservativeDRF) {
    ApplicationAttemptId attemptId = rmContainer.getApplicationAttemptId();

    // 避免重复统计同一个容器
    if (preemptMapContains(preemptMap, attemptId, rmContainer)) {
      return false;
    }

    // 获取容器所在节点分区
    String nodePartition = getPartitionByNodeId(context,
        rmContainer.getAllocatedNode());
    Resource toObtainByPartition = resourceToObtainByPartitions
        .get(nodePartition);
    if (null == toObtainByPartition) {
      return false;
    }

    // 将0值资源设为-1，避免影响后续抢占判断
    for (ResourceInformation ri : toObtainByPartition.getResources()) {
      if (ri.getValue() == 0) {
        ri.setValue(-1);
      }
    }

    // 检查仍有需要获取的资源，且容器资源未超过本轮抢占上限
    if (rc.isAnyMajorResourceAboveZero(toObtainByPartition) && Resources.fitsIn(
        rc, rmContainer.getAllocatedResource(), totalPreemptionAllowed)) {
      boolean doPreempt;

      // 计算抢占该容器后剩余待获取资源
      Resource toObtainAfterPreemption = Resources.subtract(toObtainByPartition,
          rmContainer.getAllocatedResource());

      if (conservativeDRF) {
        // 保守策略：只要任意主资源小于等于0就停止抢占（队列内抢占默认行为）
        doPreempt = !rc.isAnyMajorResourceZeroOrNegative(toObtainByPartition);
      } else {
        // 激进策略：只要抢占后总需求比抢占前小就继续（队列间抢占默认行为）
        doPreempt = Resources.lessThan(rc, clusterResource,
            Resources
                .componentwiseMax(toObtainAfterPreemption, Resources.none()),
            Resources.componentwiseMax(toObtainByPartition, Resources.none()));
      }

      if (!doPreempt) {
        return false;
      }

      // 扣除待获取资源和总允许抢占资源
      Resources.subtractFrom(toObtainByPartition,
          rmContainer.getAllocatedResource());
      Resources.subtractFrom(totalPreemptionAllowed,
          rmContainer.getAllocatedResource());

      // 如果该分区已无需获取资源，从映射中移除
      if (Resources.lessThanOrEqual(rc, clusterResource, toObtainByPartition,
          Resources.none())) {
        resourceToObtainByPartitions.remove(nodePartition);
      }

      // 将容器加入待抢占集合
      addToPreemptMap(preemptMap, curCandidates, attemptId, rmContainer);
      return true;
    }

    return false;
  }

  /**
   * 根据节点ID获取节点所属分区
   * @param context 抢占上下文
   * @param nodeId 节点ID
   * @return 分区名称
   */
  private static String getPartitionByNodeId(
      CapacitySchedulerPreemptionContext context, NodeId nodeId) {
    return context.getScheduler().getSchedulerNode(nodeId).getPartition();
  }

  /**
   * 将待抢占容器添加到抢占集合和当前候选集合
   * @param preemptMap 全局已选中抢占集合
   * @param curCandidates 当前轮次候选集合
   * @param appAttemptId 应用尝试ID
   * @param containerToPreempt 待抢占容器
   */
  protected static void addToPreemptMap(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      ApplicationAttemptId appAttemptId, RMContainer containerToPreempt) {
    Set<RMContainer> setForToPreempt = preemptMap.get(appAttemptId);
    Set<RMContainer> setForCurCandidates = curCandidates.get(appAttemptId);
    if (null == setForToPreempt) {
      setForToPreempt = new HashSet<>();
      preemptMap.put(appAttemptId, setForToPreempt);
    }
    setForToPreempt.add(containerToPreempt);

    if (null == setForCurCandidates) {
      setForCurCandidates = new HashSet<>();
      curCandidates.put(appAttemptId, setForCurCandidates);
    }
    setForCurCandidates.add(containerToPreempt);
  }

  /**
   * 检查抢占集合是否已包含该容器
   * @param preemptMap 抢占集合
   * @param attemptId 应用尝试ID
   * @param rmContainer 容器
   * @return 是否已包含
   */
  private static boolean preemptMapContains(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      ApplicationAttemptId attemptId, RMContainer rmContainer) {
    Set<RMContainer> rmContainers = preemptMap.get(attemptId);
    if (null == rmContainers) {
      return false;
    }
    return rmContainers.contains(rmContainer);
  }
}