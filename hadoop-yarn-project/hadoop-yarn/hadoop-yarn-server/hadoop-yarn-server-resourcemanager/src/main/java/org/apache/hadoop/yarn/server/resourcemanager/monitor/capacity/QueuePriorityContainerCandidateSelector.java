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

import org.apache.hadoop.thirdparty.com.google.common.collect.HashBasedTable;
import org.apache.hadoop.thirdparty.com.google.common.collect.Table;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 基于队列优先级的抢占候选容器选择器，实现高优先级队列对低优先级队列资源的抢占
 * 针对预留容器场景，通过抢占低优先级容器资源满足高优先级队列的预留容器分配需求
 */
public class QueuePriorityContainerCandidateSelector
    extends PreemptionCandidatesSelector {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueuePriorityContainerCandidateSelector.class);

  // 预留容器抢占最小等待超时时间
  private long minTimeout;

  // 是否允许移动预留容器到更好的节点位置
  private boolean allowMoveReservation;

  // 系统中所有可能需要抢占的预留容器列表
  private List<RMContainer> reservedContainers;

  // 优先级有向关系表，行表示高优先级队列，列表示低优先级队列，存在记录表示高优先级可抢占低优先级
  // 例如：a->b表示队列a优先级高于队列b，a可以抢占b的资源
  private Table<String, String, Boolean> priorityDigraph =
      HashBasedTable.create();

  private Resource clusterResource;
  private Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates;
  private Resource totalPreemptionAllowed;

  // 临时调度节点缓存，每轮抢占重新刷新
  private Map<NodeId, TempSchedulerNode> tempSchedulerNodeMap = new HashMap<>();

  // 本轮已修改过的节点集合，已修改节点不再尝试移动预留容器
  private Set<NodeId> touchedNodes;

  // 各队列在各分区标记需要从其他队列抢占的资源总量
  // <队列名, 分区, 待抢占资源总量>
  private Table<String, String, Resource> toPreemptedFromOtherQueues =
      HashBasedTable.create();

  // 容器比较器，按优先级排序，优先级相同按创建时间排序，高优先级、早创建的容器排在前面
  private final Comparator<RMContainer>
      CONTAINER_CREATION_TIME_COMPARATOR = new Comparator<RMContainer>() {
    @Override
    public int compare(RMContainer o1, RMContainer o2) {
      if (preemptionAllowed(o1.getQueueName(), o2.getQueueName())) {
        return -1;
      } else if (preemptionAllowed(o2.getQueueName(), o1.getQueueName())) {
        return 1;
      }

      // 两个队列不能互相抢占，按创建时间排序
      return Long.compare(o1.getCreationTime(), o2.getCreationTime());
    }
  };

  /**
   * 构造函数，从抢占上下文初始化配置参数
   * @param preemptionContext 容量调度器抢占上下文
   */
  QueuePriorityContainerCandidateSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    super(preemptionContext);

    // 初始化参数
    CapacitySchedulerConfiguration csc =
        preemptionContext.getScheduler().getConfiguration();

    minTimeout = csc.getPUOrderingPolicyUnderUtilizedPreemptionDelay();
    allowMoveReservation =
        csc.getPUOrderingPolicyUnderUtilizedPreemptionMoveReservation();
  }

  /**
   * 获取队列从当前队列到根队列的路径列表
   * @param tq 起始队列分区对象
   * @return 到根队列的路径列表
   */
  private List<TempQueuePerPartition> getPathToRoot(TempQueuePerPartition tq) {
    List<TempQueuePerPartition> list = new ArrayList<>();
    while (tq != null) {
      list.add(tq);
      tq = tq.parent;
    }
    return list;
  }

  /**
   * 初始化队列优先级抢占有向图，计算所有叶子队列之间的优先级抢占关系
   */
  private void initializePriorityDigraph() {
    LOG.debug("Initializing priority preemption directed graph:");
    // 遍历所有叶子队列组合
    for (String q1 : preemptionContext.getLeafQueueNames()) {
      for (String q2 : preemptionContext.getLeafQueueNames()) {
        // 只计算一次组合，避免重复处理全排列
        if (q1.compareTo(q2) < 0) {
          TempQueuePerPartition tq1 = preemptionContext.getQueueByPartition(q1,
              RMNodeLabelsManager.NO_LABEL);
          TempQueuePerPartition tq2 = preemptionContext.getQueueByPartition(q2,
              RMNodeLabelsManager.NO_LABEL);

          List<TempQueuePerPartition> path1 = getPathToRoot(tq1);
          List<TempQueuePerPartition> path2 = getPathToRoot(tq2);

          // 找到最近公共祖先(LCA)下方的直接祖先节点
          int i = path1.size() - 1;
          int j = path2.size() - 1;
          while (path1.get(i).queueName.equals(path2.get(j).queueName)) {
            i--;
            j--;
          }

          // 比较两个直接祖先的优先级
          int p1 = path1.get(i).relativePriority;
          int p2 = path2.get(j).relativePriority;
          if (p1 < p2) {
            priorityDigraph.put(q2, q1, true);
            LOG.debug("- Added priority ordering edge: {} >> {}", q2, q1);
          } else if (p2 < p1) {
            priorityDigraph.put(q1, q2, true);
            LOG.debug("- Added priority ordering edge: {} >> {}", q1, q2);
          }
        }
      }
    }
  }

  /**
   * 判断是否允许需求队列抢占待抢占队列的资源
   * @param demandingQueue 需求队列（发起抢占的队列）
   * @param toBePreemptedQueue 待抢占队列（被抢占的队列）
   * @return true允许抢占，false不允许
   */
  private boolean preemptionAllowed(String demandingQueue,
      String toBePreemptedQueue) {
    return priorityDigraph.contains(demandingQueue,
        toBePreemptedQueue);
  }

  /**
   * 判断是否能在指定节点上抢占到足够满足需求的资源
   * @param requiredResource 需求资源总量
   * @param demandingQueue 需求队列
   * @param schedulerNode 目标节点
   * @param lookingForNewReservationPlacement 是否在尝试移动预留容器到该节点
   * @param newlySelectedContainers 存储新选中的待抢占容器
   * @return true可抢占到足够资源，false不可
   */
  private boolean canPreemptEnoughResourceForAsked(Resource requiredResource,
      String demandingQueue, FiCaSchedulerNode schedulerNode,
      boolean lookingForNewReservationPlacement,
      List<RMContainer> newlySelectedContainers) {
    // 已修改过的节点不重复检查
    if (touchedNodes.contains(schedulerNode.getNodeID())) {
      return false;
    }

    // 从缓存获取节点信息，不存在则创建
    TempSchedulerNode node = tempSchedulerNodeMap.get(schedulerNode.getNodeID());
    if (null == node) {
      node = TempSchedulerNode.fromSchedulerNode(schedulerNode);
      tempSchedulerNodeMap.put(schedulerNode.getNodeID(), node);
    }

    // 节点已被预留且尝试移动预留容器时，跳过该节点
    if (null != schedulerNode.getReservedContainer()
        && lookingForNewReservationPlacement) {
      return false;
    }

    // 计算还缺多少资源：缺额 = 需求资源 - (节点总资源 - 节点已分配资源)
    Resource lacking = Resources.subtract(requiredResource, Resources
        .subtract(node.getTotalResource(), node.getAllocatedResource()));

    // 获取节点上所有运行中容器并排序
    List<RMContainer> runningContainers = node.getRunningContainers();
    Collections.sort(runningContainers, CONTAINER_CREATION_TIME_COMPARATOR);

    // 先扣除已经被选中待抢占的容器资源
    for (RMContainer runningContainer : runningContainers) {
      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(
          runningContainer, selectedCandidates)) {
        Resources.subtractFrom(lacking,
            runningContainer.getAllocatedResource());
      }
    }

    // 如果已经满足缺额，直接返回成功
    if (Resources.fitsIn(rc, lacking, Resources.none())) {
      return true;
    }

    // 初始化剩余允许抢占资源和已选中资源
    Resource allowed = Resources.clone(totalPreemptionAllowed);
    Resource selected = Resources.createResource(0);

    // 遍历所有容器尝试抢占
    for (RMContainer runningContainer : runningContainers) {
      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(
          runningContainer, selectedCandidates)) {
        // 已选中容器跳过
        continue;
      }

      // 只允许抢占低优先级队列的容器
      if (!preemptionAllowed(demandingQueue,
          runningContainer.getQueueName())) {
        continue;
      }

      // 不抢占AM容器
      if (runningContainer.isAMContainer()) {
        continue;
      }

      // 不超过总抢占限额
      if (Resources.greaterThanOrEqual(rc, clusterResource, allowed,
          runningContainer.getAllocatedResource())) {
        Resources.subtractFrom(allowed,
            runningContainer.getAllocatedResource());
        Resources.subtractFrom(lacking,
            runningContainer.getAllocatedResource());
        Resources.addTo(selected, runningContainer.getAllocatedResource());

        if (null != newlySelectedContainers) {
          newlySelectedContainers.add(runningContainer);
        }
      }

      // 缺额已满足，返回成功
      if (Resources.fitsIn(rc, lacking, Resources.none())) {
        return true;
      }
    }

    // 遍历完仍不满足缺额，返回失败
    return false;
  }

  /**
   * 移动预留容器到新节点的前置检查
   * @param reservedContainer 待移动预留容器
   * @param newNode 目标新节点
   * @return true通过检查可移动，false不允许移动
   */
  private boolean preChecksForMovingReservedContainerToNode(
      RMContainer reservedContainer, FiCaSchedulerNode newNode) {
    // 容器更新请求不允许移动
    if (reservedContainer.getReservedSchedulerKey().getContainerToUpdate()
        != null) {
      return false;
    }

    // 检查硬位置限制，硬位置请求不允许移动
    FiCaSchedulerApp app =
        preemptionContext.getScheduler().getApplicationAttempt(
            reservedContainer.getApplicationAttemptId());
    if (!app.getAppSchedulingInfo().canDelayTo(
        reservedContainer.getAllocatedSchedulerKey(), ResourceRequest.ANY)) {
      return false;
    }

    // 检查节点分区匹配请求标签
    if (!StringUtils.equals(reservedContainer.getNodeLabelExpression(),
        newNode.getPartition())) {
      return false;
    }

    return true;
  }

  /**
   * 尝试将预留容器移动到有足够可抢占资源的更好节点
   * @param reservedContainer 待移动预留容器
   * @param allSchedulerNodes 所有节点列表
   */
  private void tryToMakeBetterReservationPlacement(
      RMContainer reservedContainer,
      List<FiCaSchedulerNode> allSchedulerNodes) {
    for (FiCaSchedulerNode targetNode : allSchedulerNodes) {
      // 前置检查不通过则跳过
      if (!preChecksForMovingReservedContainerToNode(reservedContainer,
          targetNode)) {
        continue;
      }

      // 检查目标节点能否抢占到足够资源
      if (canPreemptEnoughResourceForAsked(
          reservedContainer.getReservedResource(),
          reservedContainer.getQueueName(), targetNode, true, null)) {
        NodeId fromNode = reservedContainer.getNodeId();

        // 调用调度器移动预留容器
        if (preemptionContext.getScheduler().moveReservedContainer(
            reservedContainer, targetNode)) {
          LOG.info("Successfully moved reserved container=" + reservedContainer
              .getContainerId() + " from targetNode=" + fromNode
              + " to targetNode=" + targetNode.getNodeID());
          touchedNodes.add(targetNode.getNodeID());
        }
      }
    }
  }

  /**
   * 判断队列是否已满足资源需求，满足的队列不允许抢占其他队列资源
   * @param demandingQueue 需求队列
   * @param partition 分区
   * @return true队列已满足，false未满足
   */
  private boolean isQueueSatisfied(String demandingQueue,
      String partition) {
    TempQueuePerPartition tq = preemptionContext.getQueueByPartition(
        demandingQueue, partition);
    if (null == tq) {
      return false;
    }

    Resource guaranteed = tq.getGuaranteed();
    Resource usedDeductReservd = Resources.subtract(tq.getUsed(),
        tq.getReserved());
    Resource markedToPreemptFromOtherQueue = toPreemptedFromOtherQueues.get(
        demandingQueue, partition);
    if (null == markedToPreemptFromOtherQueue) {
      markedToPreemptFromOtherQueue = Resources.none();
    }

    // 判断：已用(扣除预留) + 待抢占资源 >= 保障资源 即认为满足
    boolean flag = Resources.greaterThanOrEqual(rc, clusterResource,
        Resources.add(usedDeductReservd, markedToPreemptFromOtherQueue),
        guaranteed);
    return flag;
  }

  /**
   * 增加队列指定分区的待抢占资源总量
   * @param queue 队列名
   * @param partition 分区
   * @param allocated 新增待抢占资源
   */
  private void incToPreempt(String queue, String partition,
      Resource allocated) {
    Resource total = toPreemptedFromOtherQueues.get(queue, partition);
    if (null == total) {
      total = Resources.createResource(0);
      toPreemptedFromOtherQueues.put(queue, partition, total);
    }

    Resources.addTo(total, allocated);
  }

  @Override
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource,
      Resource totalPreemptedResourceAllowed) {
    Map<ApplicationAttemptId, Set<RMContainer>> curCandidates = new HashMap<>();
    // 清空并重新初始化优先级有向图
    // TODO (wangda): only do this when queue refreshed.
    priorityDigraph.clear();
    initializePriorityDigraph();

    // 没有可抢占关系直接返回空
    if (priorityDigraph.isEmpty()) {
      return curCandidates;
    }

    // 保存全局参数供其他方法使用
    this.selectedCandidates = selectedCandidates;
    this.clusterResource = clusterResource;
    this.totalPreemptionAllowed = totalPreemptedResourceAllowed;

    // 清空待抢占资源表
    toPreemptedFromOtherQueues.clear();

    // 初始化预留容器列表
    reservedContainers = new ArrayList<>();

    // 清空临时节点缓存和已修改节点集合
    tempSchedulerNodeMap.clear();
    touchedNodes = new HashSet<>();

    // 收集所有节点上的预留容器
    List<FiCaSchedulerNode> allSchedulerNodes =
        preemptionContext.getScheduler().getAllNodes();
    for (FiCaSchedulerNode node : allSchedulerNodes) {
      RMContainer reservedContainer = node.getReservedContainer();
      if (null != reservedContainer) {
        // 只添加存在可抢占关系队列的预留容器
        if (priorityDigraph.containsRow(
            reservedContainer.getQueueName())) {
          reservedContainers.add(reservedContainer);
        }
      }
    }

    // 对预留容器按优先级和创建