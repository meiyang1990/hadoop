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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 为预留容器抢占资源选择待抢占容器的选择器
 * 功能：为等待分配的预留容器查找可抢占的容器候选，满足预留容器的资源需求
 */
public class ReservedContainerCandidatesSelector
    extends PreemptionCandidatesSelector {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReservedContainerCandidatesSelector.class);

  private PreemptableResourceCalculator preemptableAmountCalculator;

  /**
   * 临时数据结构，保存一个节点上的抢占信息，包括抢占成本和选中的待抢占容器
   */
  private static class NodeForPreemption {
    private float preemptionCost;
    private FiCaSchedulerNode schedulerNode;
    private List<RMContainer> selectedContainers;

    public NodeForPreemption(float preemptionCost,
        FiCaSchedulerNode schedulerNode, List<RMContainer> selectedContainers) {
      this.preemptionCost = preemptionCost;
      this.schedulerNode = schedulerNode;
      this.selectedContainers = selectedContainers;
    }
  }

  /**
   * 构造预留容器抢占候选选择器
   * @param preemptionContext 容量调度抢占上下文
   */
  ReservedContainerCandidatesSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    super(preemptionContext);
    preemptableAmountCalculator = new PreemptableResourceCalculator(
        preemptionContext, true, false);
  }

  @Override
  /**
   * 选择满足预留容器资源需求的待抢占容器候选
   * @param selectedCandidates 已选中的待抢占容器集合
   * @param clusterResource 集群总资源
   * @param totalPreemptedResourceAllowed 允许抢占的总资源配额
   * @return 本次新增的待抢占容器集合
   */
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource,
      Resource totalPreemptedResourceAllowed) {
    Map<ApplicationAttemptId, Set<RMContainer>> curCandidates = new HashMap<>();
    // 计算各队列需要抢占获取的资源量
    preemptableAmountCalculator.computeIdealAllocation(clusterResource,
        totalPreemptedResourceAllowed);

    // 按队列、分区组织需要抢占获取的资源量
    Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition =
        new HashMap<>();
    for (String leafQueue : preemptionContext.getLeafQueueNames()) {
      queueToPreemptableResourceByPartition.put(leafQueue,
          CapacitySchedulerPreemptionUtils
              .getResToObtainByPartitionForLeafQueue(preemptionContext,
                  leafQueue, clusterResource));
    }

    // 获取可满足预留容器需求的节点，按抢占成本排序
    List<NodeForPreemption> nodesForPreemption = getNodesForPreemption(
        queueToPreemptableResourceByPartition, selectedCandidates,
        totalPreemptedResourceAllowed);

    // 遍历所有符合条件的节点，收集待抢占容器
    for (NodeForPreemption nfp : nodesForPreemption) {
      RMContainer reservedContainer = nfp.schedulerNode.getReservedContainer();
      if (null == reservedContainer) {
        continue;
      }

      NodeForPreemption preemptionResult = getPreemptionCandidatesOnNode(
          nfp.schedulerNode, queueToPreemptableResourceByPartition,
          selectedCandidates, totalPreemptedResourceAllowed, false);
      if (null != preemptionResult) {
        for (RMContainer c : preemptionResult.selectedContainers) {
          // 添加到待抢占映射表
          CapacitySchedulerPreemptionUtils.addToPreemptMap(selectedCandidates,
              curCandidates, c.getApplicationAttemptId(), c);

          LOG.debug("{} Marked container={} from queue={} to be preemption"
              + " candidates", this.getClass().getName(), c.getContainerId(),
              c.getQueueName());
        }
      }
    }

    return curCandidates;
  }

  /**
   * 从按队列-分区组织的抢占资源表中获取指定队列指定分区的可抢占资源量
   * @param queueName 队列名称
   * @param partitionName 分区名称
   * @param queueToPreemptableResourceByPartition 按队列-分区组织的可抢占资源表
   * @return 指定队列分区的可抢占资源量，不存在则返回null
   */
  private Resource getPreemptableResource(String queueName,
      String partitionName,
      Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition) {
    Map<String, Resource> partitionToPreemptable =
        queueToPreemptableResourceByPartition.get(queueName);
    if (null == partitionToPreemptable) {
      return null;
    }

    Resource preemptable = partitionToPreemptable.get(partitionName);
    return preemptable;
  }

  /**
   * 尝试从指定队列分区抢占指定大小的资源，检查配额是否足够
   * @param queueName 目标队列名称
   * @param partitionName 分区名称
   * @param queueToPreemptableResourceByPartition 可抢占资源配额表
   * @param required 需要抢占的资源量
   * @param totalPreemptionAllowed 全局允许抢占总资源配额
   * @param readOnly 是否仅检查不扣减配额
   * @return 是否可以抢占该资源
   */
  private boolean tryToPreemptFromQueue(String queueName, String partitionName,
      Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition,
      Resource required, Resource totalPreemptionAllowed, boolean readOnly) {
    Resource preemptable = getPreemptableResource(queueName, partitionName,
        queueToPreemptableResourceByPartition);
    if (null == preemptable) {
      return false;
    }

    if (!Resources.fitsIn(rc, required, preemptable)) {
      return false;
    }

    if (!Resources.fitsIn(rc, required, totalPreemptionAllowed)) {
      return false;
    }

    if (!readOnly) {
      Resources.subtractFrom(preemptable, required);
      Resources.subtractFrom(totalPreemptionAllowed, required);
    }
    return true;
  }


  /**
   * 检查指定节点是否可以通过抢占容器满足预留容器的资源需求，收集待抢占容器
   * @param node 目标节点
   * @param queueToPreemptableResourceByPartition 按队列-分区组织的可抢占资源表
   * @param selectedCandidates 已选中的待抢占容器集合
   * @param totalPreemptionAllowed 全局允许抢占总资源配额
   * @param readOnly 是否仅检查不修改配额
   * @return 若可以满足需求返回包含选中容器的NodeForPreemption，否则返回null
   */
  private NodeForPreemption getPreemptionCandidatesOnNode(
      FiCaSchedulerNode node,
      Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource totalPreemptionAllowed, boolean readOnly) {
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer == null) {
      return null;
    }
    Resource available = Resources.clone(node.getUnallocatedResource());
    Resource totalSelected = Resources.createResource(0);
    List<RMContainer> sortedRunningContainers =
        node.getCopiedListOfRunningContainers();
    List<RMContainer> selectedContainers = new ArrayList<>();
    Map<ContainerId, RMContainer> killableContainers =
        node.getKillableContainers();

    // 按容器ID降序排序，优先抢占新启动的容器
    Collections.sort(sortedRunningContainers, new Comparator<RMContainer>() {
      @Override public int compare(RMContainer o1, RMContainer o2) {
        return -1 * o1.getContainerId().compareTo(o2.getContainerId());
      }
    });

    // 标记是否可以通过抢占满足预留容器需求
    boolean canAllocateReservedContainer = false;

    // 当前节点可用资源 = 空闲资源 + 已标记可杀死资源
    Resource cur = Resources.add(available, node.getTotalKillableResources());
    String partition = node.getPartition();

    // 如果现有可用+可杀死资源已经能满足需求，不需要抢占新容器
    if (Resources.fitsIn(rc, reservedContainer.getReservedResource(), cur)) {
      return null;
    }

    // AM容器抢占额外成本，这里策略是不抢占AM，所以恒为0
    float amPreemptionCost = 0f;

    // 遍历所有运行容器，收集可抢占容器
    for (RMContainer c : sortedRunningContainers) {
      String containerQueueName = c.getQueueName();

      // 跳过已经标记为可杀死的容器
      if (killableContainers.containsKey(c.getContainerId())) {
        continue;
      }

      // 安全策略：永远不抢占ApplicationMaster容器
      if (c.isAMContainer()) {
        LOG.debug("Skip selecting AM container on host={} AM container={}",
            node.getNodeID(), c.getContainerId());

        continue;
      }

      // 检查队列和全局配额是否允许抢占该容器
      boolean canPreempt = tryToPreemptFromQueue(containerQueueName, partition,
          queueToPreemptableResourceByPartition, c.getAllocatedResource(),
          totalPreemptionAllowed, readOnly);

      // 若允许抢占，添加到选中列表，更新资源统计
      if (canPreempt) {
        if (!CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
            selectedCandidates)) {
          if (!readOnly) {
            selectedContainers.add(c);
          }
          Resources.addTo(totalSelected, c.getAllocatedResource());
        }
        Resources.addTo(cur, c.getAllocatedResource());
        // 检查累计资源是否已经满足预留容器需求
        if (Resources.fitsIn(rc,
            reservedContainer.getReservedResource(), cur)) {
          canAllocateReservedContainer = true;
          break;
        }
      }
    }

    // 无法满足预留容器需求，回滚配额变更返回null
    if (!canAllocateReservedContainer) {
      if (!readOnly) {
        // 回滚队列抢占配额
        for (RMContainer c : selectedContainers) {
          Resource res = getPreemptableResource(c.getQueueName(), partition,
              queueToPreemptableResourceByPartition);
          if (null == res) {
            // 容器可能在抢占过程中移动到了其他队列，忽略该错误
            continue;
          }
          Resources.addTo(res, c.getAllocatedResource());
        }
      }
      return null;
    }

    // 计算抢占成本：已选资源/需求资源，越接近1成本越低
    float ratio = Resources.ratio(rc, totalSelected,
        reservedContainer.getReservedResource());

    // 构造抢占节点信息返回
    NodeForPreemption nfp = new NodeForPreemption(ratio + amPreemptionCost,
        node, selectedContainers);

    return nfp;
  }

  /**
   * 获取所有包含预留容器且可通过抢占满足资源需求的节点，按抢占成本排序
   * @param queueToPreemptableResourceByPartition 按队列-分区组织的可抢占资源表
   * @param selectedCandidates 已选中的待抢占容器集合
   * @param totalPreemptionAllowed 全局允许抢占总资源配额
   * @return 按抢占成本升序排序的可抢占节点列表
   */
  private List<NodeForPreemption> getNodesForPreemption(
      Map<String, Map<String, Resource>> queueToPreemptableResourceByPartition,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource totalPreemptionAllowed) {
    List<NodeForPreemption> nfps = new ArrayList<>();

    // 遍历所有节点，筛选出有预留容器且可以抢占满足需求的节点
    for (FiCaSchedulerNode node : preemptionContext.getScheduler()
        .getAllNodes()) {
      if (node.getReservedContainer() != null) {
        NodeForPreemption nfp = getPreemptionCandidatesOnNode(node,
            queueToPreemptableResourceByPartition, selectedCandidates,
            totalPreemptionAllowed, true);
        if (null != nfp) {
          nfps.add(nfp);
        }
      }
    }

    // 按抢占成本升序排序，成本低的优先抢占
    Collections.sort(nfps, new Comparator<NodeForPreemption>() {
      @Override
      public int compare(NodeForPreemption o1, NodeForPreemption o2) {
        return Float.compare(o1.preemptionCost, o2.preemptionCost);
      }
    });

    return nfps;
  }
}