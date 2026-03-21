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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * FIFO抢占候选容器选择器，按照先进先出原则选择需要被抢占的容器
 * 优先抢占后提交应用的容器，保障先提交应用的资源分配，满足队列资源均衡需求
 */
public class FifoCandidatesSelector
    extends PreemptionCandidatesSelector {
  private static final Logger LOG =
      LoggerFactory.getLogger(FifoCandidatesSelector.class);
  private PreemptableResourceCalculator preemptableAmountCalculator;
  private boolean allowQueuesBalanceAfterAllQueuesSatisfied;

  /**
   * 构造FIFO抢占候选选择器
   * @param preemptionContext 容量调度抢占上下文
   * @param includeReservedResource 是否包含预留资源参与可抢占计算
   * @param allowQueuesBalanceAfterAllQueuesSatisfied 所有队列满足最低保障后是否允许继续抢占均衡
   */
  FifoCandidatesSelector(CapacitySchedulerPreemptionContext preemptionContext,
      boolean includeReservedResource,
      boolean allowQueuesBalanceAfterAllQueuesSatisfied) {
    super(preemptionContext);

    this.allowQueuesBalanceAfterAllQueuesSatisfied =
        allowQueuesBalanceAfterAllQueuesSatisfied;
    preemptableAmountCalculator = new PreemptableResourceCalculator(
        preemptionContext, includeReservedResource,
        allowQueuesBalanceAfterAllQueuesSatisfied);
  }

  @Override
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource, Resource totalPreemptionAllowed) {
    Map<ApplicationAttemptId, Set<RMContainer>> curCandidates = new HashMap<>();
    // 计算各队列需要抢占的资源总量
    preemptableAmountCalculator.computeIdealAllocation(clusterResource,
        totalPreemptionAllowed);

    // 高优先级选择器已经选中的容器，需要从可抢占资源中扣除已选资源
    CapacitySchedulerPreemptionUtils
        .deductPreemptableResourcesBasedSelectedCandidates(preemptionContext,
            selectedCandidates);

    List<RMContainer> skippedAMContainerlist = new ArrayList<>();

    // 遍历所有叶子队列选择可抢占容器
    for (String queueName : preemptionContext.getLeafQueueNames()) {
      // 检查该队列是否禁用抢占
      if (preemptionContext.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).preemptionDisabled) {
        LOG.debug("skipping from queue={} because it's a"
            + " non-preemptable queue", queueName);
        continue;
      }

      // 获取队列信息，计算各分区需要抢占的资源
      AbstractLeafQueue leafQueue = preemptionContext.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).leafQueue;

      Map<String, Resource> resToObtainByPartition =
          CapacitySchedulerPreemptionUtils
              .getResToObtainByPartitionForLeafQueue(preemptionContext,
                  queueName, clusterResource);

      // 获取队列读锁，保证遍历容器过程中队列结构不变化
      leafQueue.getReadLock().lock();
      try {
        // 优先处理忽略分区排他性的容器，确保这类容器优先被选为抢占候选
        Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityContainers =
            leafQueue.getIgnoreExclusivityRMContainers();
        for (String partition : resToObtainByPartition.keySet()) {
          if (ignorePartitionExclusivityContainers.containsKey(partition)) {
            TreeSet<RMContainer> rmContainers =
                ignorePartitionExclusivityContainers.get(partition);
            // 逆序遍历，后提交的容器优先被抢占
            for (RMContainer c : rmContainers.descendingSet()) {
              if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
                  selectedCandidates)) {
                // 跳过已被选中的容器
                continue;
              }
              boolean preempted = CapacitySchedulerPreemptionUtils
                  .tryPreemptContainerAndDeductResToObtain(rc,
                      preemptionContext, resToObtainByPartition, c,
                      clusterResource, selectedCandidates, curCandidates,
                      totalPreemptionAllowed,
                      preemptionContext.getCrossQueuePreemptionConservativeDRF()
                      );
              if (!preempted) {
                continue;
              }
            }
          }
        }

        // 处理普通容器
        Resource skippedAMSize = Resource.newInstance(0, 0);
        Iterator<FiCaSchedulerApp> desc =
            leafQueue.getOrderingPolicy().getPreemptionIterator();
        while (desc.hasNext()) {
          FiCaSchedulerApp fc = desc.next();
          // 所有分区已获取足够可抢占资源，无需继续
          if (resToObtainByPartition.isEmpty()) {
            break;
          }

          // 从当前应用中选择容器抢占
          preemptFrom(fc, clusterResource, resToObtainByPartition,
              skippedAMContainerlist, skippedAMSize, selectedCandidates,
              curCandidates, totalPreemptionAllowed);
        }

        // 如果仍需要更多资源，尝试抢占AM容器，保留队列最大AM容量限制
        Resource maxAMCapacityForThisQueue = Resources
            .multiply(
                leafQueue.getEffectiveCapacity(RMNodeLabelsManager.NO_LABEL),
                leafQueue.getMaxAMResourcePerQueuePercent());

        preemptAMContainers(clusterResource, selectedCandidates, curCandidates,
            skippedAMContainerlist, resToObtainByPartition, skippedAMSize,
            maxAMCapacityForThisQueue, totalPreemptionAllowed);
      } finally {
        // 释放队列读锁
        leafQueue.getReadLock().unlock();
      }
    }

    return curCandidates;
  }

  /**
   * 仍需要更多抢占资源时，重新扫描之前跳过的AM容器进行抢占
   * 保证抢占后仍保留队列配置的最大AM容量资源不被抢占
   *
   * @param clusterResource 集群总资源
   * @param preemptMap 已选中的抢占容器集合
   * @param skippedAMContainerlist 之前跳过的AM容器列表
   * @param skippedAMSize 跳过AM容器总资源大小
   * @param maxAMCapacityForThisQueue 队列需要保留的最大AM容量
   * @param resToObtainByPartition 各分区仍需要抢占的资源
   * @param totalPreemptionAllowed 允许抢占的总资源
   */
  private void preemptAMContainers(Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      List<RMContainer> skippedAMContainerlist,
      Map<String, Resource> resToObtainByPartition, Resource skippedAMSize,
      Resource maxAMCapacityForThisQueue, Resource totalPreemptionAllowed) {
    for (RMContainer c : skippedAMContainerlist) {
      // 已获取足够资源，停止抢占
      if (resToObtainByPartition.isEmpty()) {
        break;
      }
      // 剩余保留资源已低于最大AM容量限制，停止抢占
      if (Resources.lessThanOrEqual(rc, clusterResource, skippedAMSize,
          maxAMCapacityForThisQueue)) {
        break;
      }

      boolean preempted = CapacitySchedulerPreemptionUtils
          .tryPreemptContainerAndDeductResToObtain(rc, preemptionContext,
              resToObtainByPartition, c, clusterResource, preemptMap,
              curCandidates, totalPreemptionAllowed,
              preemptionContext.getCrossQueuePreemptionConservativeDRF());
      if (preempted) {
        Resources.subtractFrom(skippedAMSize, c.getAllocatedResource());
      }
    }
    skippedAMContainerlist.clear();
  }

  /**
   * 从指定应用中选择容器进行抢占，先处理预留容器，再处理普通容器
   * AM容器暂时跳过，后续统一处理
   */
  private void preemptFrom(FiCaSchedulerApp app,
      Resource clusterResource, Map<String, Resource> resToObtainByPartition,
      List<RMContainer> skippedAMContainerlist, Resource skippedAMSize,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedContainers,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      Resource totalPreemptionAllowed) {
    ApplicationAttemptId appId = app.getApplicationAttemptId();

    // 优先抢占预留容器
    List<RMContainer> reservedContainers =
        new ArrayList<>(app.getReservedContainers());
    for (RMContainer c : reservedContainers) {
      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
          selectedContainers)) {
        continue;
      }
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      // 尝试抢占该容器
     CapacitySchedulerPreemptionUtils
          .tryPreemptContainerAndDeductResToObtain(rc, preemptionContext,
              resToObtainByPartition, c, clusterResource, selectedContainers,
              curCandidates, totalPreemptionAllowed,
              preemptionContext.getCrossQueuePreemptionConservativeDRF());

      // 非观察模式下，发送事件杀死该预留容器
      if (!preemptionContext.isObserveOnly()) {
        preemptionContext.getRMContext().getDispatcher().getEventHandler()
            .handle(new ContainerPreemptEvent(appId, c,
                SchedulerEventType.KILL_RESERVED_CONTAINER));
      }
    }

    // 如果还需要更多资源，遍历所有活跃容器选择抢占
    List<RMContainer> liveContainers =
        new ArrayList<>(app.getLiveContainers());

    sortContainers(liveContainers);

    for (RMContainer c : liveContainers) {
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
          selectedContainers)) {
        continue;
      }

      // 跳过已经标记为可杀死的容器
      if (null != preemptionContext.getKillableContainers() && preemptionContext
          .getKillableContainers().contains(c.getContainerId())) {
        continue;
      }

      // AM容器暂时跳过，后续统一抢占
      if (c.isAMContainer()) {
        skippedAMContainerlist.add(c);
        Resources.addTo(skippedAMSize, c.getAllocatedResource());
        continue;
      }

      // 尝试抢占该容器
      CapacitySchedulerPreemptionUtils
          .tryPreemptContainerAndDeductResToObtain(rc, preemptionContext,
              resToObtainByPartition, c, clusterResource, selectedContainers,
              curCandidates, totalPreemptionAllowed,
              preemptionContext.getCrossQueuePreemptionConservativeDRF());
    }
  }

  public boolean getAllowQueuesBalanceAfterAllQueuesSatisfied() {
    return allowQueuesBalanceAfterAllQueuesSatisfied;
  }
}