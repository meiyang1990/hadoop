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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 文件说明：容量调度器抢占资源计算器，计算每个队列需要抢占的资源量，供PreemptionCandidatesSelector使用
 */
public class PreemptableResourceCalculator
    extends
      AbstractPreemptableResourceCalculator {
  private static final Logger LOG =
      LoggerFactory.getLogger(PreemptableResourceCalculator.class);

  /**
   * 构造函数
   *
   * @param preemptionContext 抢占上下文
   * @param isReservedPreemptionCandidatesSelector 是否为预留资源抢占候选选择器，由不同实现设置，详见TempQueuePerPartition#offer
   * @param allowQueuesBalanceAfterAllQueuesSatisfied 
   *         当所有请求队列都已满足保障容量时，是否允许从超配额队列抢占资源实现队列间平衡
   *         示例：root下有10个队列，每个保障容量都是10%。假设只有两个队列使用资源，queueA用了10%，queueB用了90%。
   *         所有队列都满足了保障容量，但分配不公平。该配置用于开启/关闭该场景下的抢占，默认关闭
   */
  public PreemptableResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector,
      boolean allowQueuesBalanceAfterAllQueuesSatisfied) {
    super(preemptionContext, isReservedPreemptionCandidatesSelector,
        allowQueuesBalanceAfterAllQueuesSatisfied);
  }

  /**
   * 为同一层级的队列集合计算理想资源分配，递归分配直到资源分配完成或所有需求满足
   *
   * @param rc 资源计算器
   * @param queues 待分配容量的临时队列列表，作为输出参数，会被修改
   * @param totalPreemptionAllowed 允许抢占的总资源量上限
   * @param tot_guarant 当前队列池可分配的总容量
   */
  protected void computeIdealResourceDistribution(ResourceCalculator rc,
      List<TempQueuePerPartition> queues, Resource totalPreemptionAllowed,
      Resource tot_guarant) {

    // 保存当前待分配的活跃队列列表，随需求满足逐步减少
    List<TempQueuePerPartition> qAlloc = new ArrayList<>(queues);
    // 保存剩余未分配资源，初始化为当前队列池总容量
    Resource unassigned = Resources.clone(tot_guarant);

    // 按是否有非零保障容量分组队列
    Set<TempQueuePerPartition> nonZeroGuarQueues = new HashSet<>();
    Set<TempQueuePerPartition> zeroGuarQueues = new HashSet<>();

    for (TempQueuePerPartition q : qAlloc) {
      if (Resources.greaterThan(rc, tot_guarant,
          q.getGuaranteed(), Resources.none())) {
        nonZeroGuarQueues.add(q);
      } else {
        zeroGuarQueues.add(q);
      }
    }

    // 首先基于保障容量计算不动点分配
    computeFixpointAllocation(tot_guarant, new HashSet<>(nonZeroGuarQueues),
        unassigned, false);

    // 如果还有剩余未分配容量，均匀分配给零保障队列
    if (!zeroGuarQueues.isEmpty()
        && Resources.greaterThan(rc, tot_guarant, unassigned, Resources.none())) {
      computeFixpointAllocation(tot_guarant, zeroGuarQueues, unassigned,
          true);
    }

    // 根据理想分配和当前使用量，计算总共需要抢占的资源量
    Resource totPreemptionNeeded = Resource.newInstance(0, 0);
    for (TempQueuePerPartition t:queues) {
      if (Resources.greaterThan(rc, tot_guarant,
          t.getUsed(), t.idealAssigned)) {
        Resources.addTo(totPreemptionNeeded, Resources
            .subtract(t.getUsed(), t.idealAssigned));
      }
    }

    /**
     * 如果需要抢占的资源超过允许上限，计算缩放因子(0<f<1)，按比例缩放每个队列的抢占量
     */
    float scalingFactor = 1.0F;
    if (Resources.greaterThan(rc,
        tot_guarant, totPreemptionNeeded, totalPreemptionAllowed)) {
      scalingFactor = Resources.divide(rc, tot_guarant, totalPreemptionAllowed,
          totPreemptionNeeded);
    }

    // 根据理想抢占量和缩放因子，为每个队列设置实际需要抢占的资源量
    for (TempQueuePerPartition t : queues) {
      t.assignPreemption(scalingFactor, rc, tot_guarant);
    }
  }

  /**
   * 递归计算队列层次结构每层的理想资源分配，确保只有父队列也超容量时才会抢占子队列资源
   *
   * @param root 当前层级的根临时队列
   * @param totalPreemptionAllowed 允许抢占的总资源量上限
   */
  protected void recursivelyComputeIdealAssignment(
      TempQueuePerPartition root, Resource totalPreemptionAllowed) {
    if (root.getChildren() != null &&
        root.getChildren().size() > 0) {
      // 计算当前层级的理想分配
      computeIdealResourceDistribution(rc, root.getChildren(),
          totalPreemptionAllowed, root.idealAssigned);
      // 递归计算子层级，生成叶子队列列表
      for (TempQueuePerPartition t : root.getChildren()) {
        recursivelyComputeIdealAssignment(t, totalPreemptionAllowed);
      }
    }
  }

  /**
   * 按分区计算每个叶子队列需要抢占获取的资源量
   * @param leafQueueNames 叶子队列名称集合
   * @param clusterResource 集群总资源
   */
  private void calculateResToObtainByPartitionForLeafQueues(
      Set<String> leafQueueNames, Resource clusterResource) {
    // 遍历所有叶子队列
    for (String queueName : leafQueueNames) {
      // 检查队列是否禁用抢占
      if (context.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).preemptionDisabled) {
        LOG.debug("skipping from queue={} because it's a non-preemptable"
            + " queue", queueName);
        continue;
      }

      // 遍历队列的所有分区，计算需要抢占的资源
      for (TempQueuePerPartition qT : context.getQueuePartitions(queueName)) {
        // 仅当使用率超过最大忽略超额阈值时才进行抢占
        if (Resources.greaterThan(rc, clusterResource,
            qT.getUsed(), Resources
                .multiply(qT.getGuaranteed(),
                    1.0 + context.getMaxIgnoreOverCapacity()))) {
          /*
           * 使用自然终止系数减缓抢占速度，因为部分容器会自然结束无需抢占
           * 例如：计算得出队列需要抢占20GB，系数设为0.1，则实际只选择2GB容器进行抢占
           * 该优化仅对非预留抢占候选选择器生效，对于需要抢占大容器的场景，部分抢占没有意义
           */
          Resource resToObtain = qT.toBePreempted;
          if (!isReservedPreemptionCandidatesSelector) {
            if (Resources.greaterThan(rc, clusterResource, resToObtain,
                Resource.newInstance(0, 0))) {
              resToObtain = Resources.multiplyAndNormalizeUp(rc, qT.toBePreempted,
                  context.getNaturalTerminationFactor(), Resource.newInstance(1, 1));
            }
          }

          // 仅当需要抢占资源大于0时记录日志
          if (Resources.greaterThan(rc, clusterResource, resToObtain,
              Resources.none())) {
            LOG.debug("Queue={} partition={} resource-to-obtain={}",
                queueName, qT.partition, resToObtain);
          }
          qT.setActuallyToBePreempted(Resources.clone(resToObtain));
        } else {
          // 未超过阈值，不需要抢占
          qT.setActuallyToBePreempted(Resources.none());
        }
        LOG.debug("{}", qT);
      }
    }
  }

  /**
   * 递归更新队列可抢占额外资源，从叶子到根汇总
   * @param cur 当前处理的临时队列
   */
  private void updatePreemptableExtras(TempQueuePerPartition cur) {
    if (cur.children == null || cur.children.isEmpty()) {
      cur.updatePreemptableExtras(rc);
    } else {
      for (TempQueuePerPartition child : cur.children) {
        updatePreemptableExtras(child);
      }
      cur.updatePreemptableExtras(rc);
    }
  }

  /**
   * 计算全集群所有队列的理想资源分配，确定各队列实际需要抢占的资源量
   * @param clusterResource 集群总资源
   * @param totalPreemptionAllowed 允许抢占的总资源量上限
   */
  public void computeIdealAllocation(Resource clusterResource,
      Resource totalPreemptionAllowed) {
    for (String partition : context.getAllPartitions()) {
      TempQueuePerPartition tRoot = context.getQueueByPartition(
          CapacitySchedulerConfiguration.ROOT, partition);
      // 更新根队列可抢占额外资源
      updatePreemptableExtras(tRoot);

      // 初始化根队列理想分配为保障容量，递归计算整个队列树的理想分配
      tRoot.initializeRootIdealWithGuarangeed();
      recursivelyComputeIdealAssignment(tRoot, totalPreemptionAllowed);
    }

    // 根据理想分配，计算每个叶子队列各分区实际需要抢占获取的资源
    calculateResToObtainByPartitionForLeafQueues(context.getLeafQueueNames(),
        clusterResource);
  }
}