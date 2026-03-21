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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.IntraQueuePreemptionOrderPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.AbstractComparatorOrderingPolicy;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 队列内抢占候选容器选择器，识别队列内资源分配异常，基于优先级和用户限额进行资源再分配，解决分配不均衡问题。
 * 属于YARN容量调度器抢占机制的一部分，负责处理同一个队列内部不同应用/用户之间的资源抢占。
 */
public class IntraQueueCandidatesSelector extends PreemptionCandidatesSelector {

  @SuppressWarnings("serial")
  /**
   * 按应用优先级比较，优先级高的排在前面，优先级相同则按应用ID排序
   */
  static class TAPriorityComparator
      implements
        Serializable,
        Comparator<TempAppPerPartition> {

    @Override
    public int compare(TempAppPerPartition ta1, TempAppPerPartition ta2) {
      Priority p1 = Priority.newInstance(ta1.getPriority());
      Priority p2 = Priority.newInstance(ta2.getPriority());

      if (!p1.equals(p2)) {
        return p1.compareTo(p2);
      }
      return ta1.getApplicationId().compareTo(ta2.getApplicationId());
    }
  }

  /*
   * Order first by amount used from least to most. Then order from oldest to
   * youngest if amount used is the same.
   */
  /**
   * 公平排序比较器，按用户已使用资源量从小到大排序，资源量相同则按应用ID排序
   * 用于用户限额优先的抢占场景，让使用资源多的用户更容易被抢占
   */
  static class TAFairOrderingComparator
      implements Comparator<TempAppPerPartition> {

    private ResourceCalculator rc;
    private Resource clusterRes;

    TAFairOrderingComparator(ResourceCalculator rc, Resource clusterRes) {
      this.rc = rc;
      this.clusterRes = clusterRes;
    }

    @Override
    public int compare(TempAppPerPartition ta1, TempAppPerPartition ta2) {
      if (ta1.getUser().equals(ta2.getUser())) {
        AbstractComparatorOrderingPolicy<FiCaSchedulerApp> acop =
            (AbstractComparatorOrderingPolicy<FiCaSchedulerApp>)
            ta1.getFiCaSchedulerApp().getCSLeafQueue().getOrderingPolicy();
        return acop.getComparator()
                  .compare(ta1.getFiCaSchedulerApp(), ta2.getFiCaSchedulerApp());
      } else {
        Resource usedByUser1 = ta1.getTempUserPerPartition().getUsedDeductAM();
        Resource usedByUser2 = ta2.getTempUserPerPartition().getUsedDeductAM();
        if (Resources.equals(usedByUser1, usedByUser2)) {
          return ta1.getApplicationId().compareTo(ta2.getApplicationId());
        }
        if (Resources.lessThan(rc, clusterRes, usedByUser1, usedByUser2)) {
          return -1;
        } else {
          return 1;
        }
      }
    }
  }

  IntraQueuePreemptionComputePlugin fifoPreemptionComputePlugin = null;
  final CapacitySchedulerPreemptionContext context;

  private static final Logger LOG =
      LoggerFactory.getLogger(IntraQueueCandidatesSelector.class);

  /**
   * 构造函数，初始化队列内抢占候选选择器
   * @param preemptionContext 抢占上下文，包含预计算的队列、应用资源信息
   */
  IntraQueueCandidatesSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    super(preemptionContext);
    fifoPreemptionComputePlugin = new FifoIntraQueuePreemptionPlugin(rc,
        preemptionContext);
    context = preemptionContext;
  }

  @Override
  /**
   * 选择需要被抢占的容器候选集合
   * @param selectedCandidates 已被其他选择器选中的抢占容器
   * @param clusterResource 集群总资源
   * @param totalPreemptedResourceAllowed 本轮允许抢占的总资源上限
   * @return 本轮选中的需要被抢占的容器集合
   */
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource, Resource totalPreemptedResourceAllowed) {
    Map<ApplicationAttemptId, Set<RMContainer>> curCandidates = new HashMap<>();
    // 1. 逐个队列计算队列内的资源分配异常和抢占需求
    computeIntraQueuePreemptionDemand(
        clusterResource, totalPreemptedResourceAllowed, selectedCandidates);

    // 2. 根据已被选中的候选容器，扣减可抢占资源配额
    CapacitySchedulerPreemptionUtils
        .deductPreemptableResourcesBasedSelectedCandidates(preemptionContext,
            selectedCandidates);

    // 3. 遍历所有资源分区选择待抢占容器
    for (String partition : preemptionContext.getAllPartitions()) {
      LinkedHashSet<String> queueNames = preemptionContext
          .getUnderServedQueuesPerPartition(partition);

      // 跳过未映射标签的队列
      if (null == queueNames) {
        continue;
      }

      // 4. 按资源不足程度从高到低遍历队列
      for (String queueName : queueNames) {
        AbstractLeafQueue leafQueue = preemptionContext.getQueueByPartition(queueName,
            RMNodeLabelsManager.NO_LABEL).leafQueue;

        // 跳过非叶子队列
        if (null == leafQueue) {
          continue;
        }

        // 如果该队列关闭了队列内抢占，直接跳过
        if (leafQueue.getIntraQueuePreemptionDisabled()) {
          continue;
        }

        // 5. 计算每个分区需要获取的资源量
        Map<String, Resource> resToObtainByPartition = fifoPreemptionComputePlugin
            .getResourceDemandFromAppsPerQueue(queueName, partition);

        // 获取符合抢占条件的应用列表
        Collection<FiCaSchedulerApp> apps = fifoPreemptionComputePlugin
            .getPreemptableApps(queueName, partition);

        // 6. 初始化每个用户的滚动资源使用量，确保抢占后不会低于用户限额
        Map<String, Resource> rollingResourceUsagePerUser = new HashMap<>();
        initializeUsageAndUserLimitForCompute(clusterResource, partition,
            leafQueue, rollingResourceUsagePerUser);

        // 7. 遍历应用选择待抢占容器，获取队列读锁保证安全
        leafQueue.getReadLock().lock();
        try {
          for (FiCaSchedulerApp app : apps) {
            preemptFromLeastStarvedApp(app, selectedCandidates,
                curCandidates, clusterResource, totalPreemptedResourceAllowed,
                resToObtainByPartition, rollingResourceUsagePerUser);
          }
        } finally {
          leafQueue.getReadLock().unlock();
        }
      }
    }

    return curCandidates;
  }

  /**
   * 初始化用户滚动资源使用量，用于计算抢占后是否低于用户限额
   * @param clusterResource 集群总资源
   * @param partition 资源分区
   * @param leafQueue 叶子队列
   * @param rollingResourceUsagePerUser 输出参数，存储每个用户的初始资源使用量
   */
  private void initializeUsageAndUserLimitForCompute(Resource clusterResource,
      String partition, AbstractLeafQueue leafQueue,
      Map<String, Resource> rollingResourceUsagePerUser) {
    for (String user : leafQueue.getAllUsers()) {
      // 克隆用户当前已使用资源作为初始滚动计算值
      rollingResourceUsagePerUser.put(user, Resources.clone(
          leafQueue.getUser(user).getResourceUsage().getUsed(partition)));
      LOG.debug("Rolling resource usage for user:{} is : {}", user,
          rollingResourceUsagePerUser.get(user));
    }
  }

  /**
   * 从资源过剩的应用中选择容器进行抢占，优先选择满足需求的容器
   * @param app 当前待检查的应用
   * @param selectedCandidates 已被选中的抢占容器集合
   * @param curCandidates 本轮新增选中的容器集合
   * @param clusterResource 集群总资源
   * @param totalPreemptedResourceAllowed 本轮允许抢占总资源上限
   * @param resToObtainByPartition 各分区还需要获取的资源量
   * @param rollingResourceUsagePerUser 各用户滚动资源使用量记录
   */
  private void preemptFromLeastStarvedApp(FiCaSchedulerApp app,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      Resource clusterResource, Resource totalPreemptedResourceAllowed,
      Map<String, Resource> resToObtainByPartition,
      Map<String, Resource> rollingResourceUsagePerUser) {

    // ToDo: Reuse reservation selector here.

    List<RMContainer> liveContainers = new ArrayList<>(app.getLiveContainers());
    sortContainers(liveContainers);
    LOG.debug("totalPreemptedResourceAllowed for preemption at this"
        + " round is :{}", totalPreemptedResourceAllowed);

    Resource rollingUsedResourcePerUser = rollingResourceUsagePerUser
        .get(app.getUser());
    for (RMContainer c : liveContainers) {

      // 如果已经满足所有抢占需求，直接返回
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      // 跳过已被其他选择器选中的容器
      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
          selectedCandidates)) {
        continue;
      }

      // 跳过已被标记为可杀死的容器
      if (null != preemptionContext.getKillableContainers() && preemptionContext
          .getKillableContainers().contains(c.getContainerId())) {
        continue;
      }

      // 当前不抢占AM容器
      if (c.isAMContainer()) {
        continue;
      }

      // 如果抢占该容器会导致用户资源低于限额，则跳过该容器及后续容器
      if (fifoPreemptionComputePlugin.skipContainerBasedOnIntraQueuePolicy(app,
          clusterResource, rollingUsedResourcePerUser, c)) {
        LOG.debug("Skipping container: {} with resource:{} as UserLimit for"
            + " user:{} with resource usage: {} is going under UL",
            c.getContainerId(), c.getAllocatedResource(), app.getUser(),
            rollingUsedResourcePerUser);

        break;
      }

      // 尝试将该容器加入抢占候选，扣减对应分区的资源需求
      boolean ret = CapacitySchedulerPreemptionUtils
          .tryPreemptContainerAndDeductResToObtain(rc, preemptionContext,
              resToObtainByPartition, c, clusterResource, selectedCandidates,
              curCandidates, totalPreemptedResourceAllowed,
              preemptionContext.getInQueuePreemptionConservativeDRF());

      // 选中容器后，更新用户滚动资源使用量
      if (ret && preemptionContext.getIntraQueuePreemptionOrderPolicy()
          .equals(IntraQueuePreemptionOrderPolicy.USERLIMIT_FIRST)) {
        Resources.subtractFrom(rollingUsedResourcePerUser,
            c.getAllocatedResource());
      }
    }
  }

  /**
   * 计算所有队列内的抢占需求，计算每个应用的理想分配资源，确定需要抢占的总资源量
   * @param clusterResource 集群总资源
   * @param totalPreemptedResourceAllowed 本轮允许抢占总资源上限
   * @param selectedCandidates 已被选中的抢占容器集合
   */
  private void computeIntraQueuePreemptionDemand(Resource clusterResource,
      Resource totalPreemptedResourceAllowed,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates) {

    // 1. 遍历所有资源分区计算抢占需求
    for (String partition : context.getAllPartitions()) {
      LinkedHashSet<String> queueNames = context
          .getUnderServedQueuesPerPartition(partition);

      if (null == queueNames) {
        continue;
      }

      // 2. 遍历分区下所有资源不足的队列
      for (String queueName : queueNames) {
        TempQueuePerPartition tq = context.getQueueByPartition(queueName,
            partition);
        AbstractLeafQueue leafQueue = tq.leafQueue;

        // 跳过非叶子队列
        if (null == leafQueue) {
          continue;
        }

        // 3. 计算队列可重新分配的资源 = 已使用资源 - 已经计划抢占的资源
        Resource queueReassignableResource = Resources.subtract(tq.getUsed(),
            tq.getActuallyToBePreempted());

        // 4. 队列使用率低于最小阈值时，不触发队列内抢占
        if (leafQueue.getQueueCapacities().getUsedCapacity(partition) < context
            .getMinimumThresholdForIntraQueuePreemption()) {
          continue;
        }

        // 5. 基于队列可分配资源，计算所有应用的理想资源分配，确定抢占需求
        fifoPreemptionComputePlugin.computeAppsIdealAllocation(clusterResource,
            tq, selectedCandidates, totalPreemptedResourceAllowed,
            queueReassignableResource,
            context.getMaxAllowableLimitForIntraQueuePreemption());
      }
    }
  }
}