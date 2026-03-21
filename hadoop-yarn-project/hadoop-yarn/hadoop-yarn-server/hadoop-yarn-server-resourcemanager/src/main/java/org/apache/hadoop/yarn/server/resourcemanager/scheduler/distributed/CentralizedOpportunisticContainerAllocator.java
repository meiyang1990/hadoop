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


package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 集中式机会容器分配器，基于集群所有节点分配机会容器，遵循ResourceManager限制
 * 调整容器大小，尽可能均匀地将容器分布到不同节点。
 * 属于YARN分布式机会调度的核心服务端组件，负责从ResourceManager集中完成容器分配。
 */
public class CentralizedOpportunisticContainerAllocator extends
    OpportunisticContainerAllocator {

  private static final Logger LOG =
      LoggerFactory.getLogger(CentralizedOpportunisticContainerAllocator.class);

  // 节点队列负载监视器，用于根据负载选择合适的分配节点
  private NodeQueueLoadMonitor nodeQueueLoadMonitor;
  // 机会调度指标收集器
  private OpportunisticSchedulerMetrics metrics =
      OpportunisticSchedulerMetrics.getMetrics();

  /**
   * 构造集中式机会容器分配器，使用默认参数。
   * @param tokenSecretManager 容器令牌密钥管理器，用于生成容器令牌
   */
  public CentralizedOpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager) {
    super(tokenSecretManager);
  }

  /**
   * 构造集中式机会容器分配器，指定全量参数。
   * @param tokenSecretManager 容器令牌密钥管理器，用于生成容器令牌
   * @param maxAllocationsPerAMHeartbeat 单次AM心跳最多分配容器数量
   * @param nodeQueueLoadMonitor 节点队列负载监视器
   */
  public CentralizedOpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager,
      int maxAllocationsPerAMHeartbeat,
      NodeQueueLoadMonitor nodeQueueLoadMonitor) {
    super(tokenSecretManager, maxAllocationsPerAMHeartbeat);
    this.nodeQueueLoadMonitor = nodeQueueLoadMonitor;
  }

  @VisibleForTesting
  void setNodeQueueLoadMonitor(NodeQueueLoadMonitor nodeQueueLoadMonitor) {
    this.nodeQueueLoadMonitor = nodeQueueLoadMonitor;
  }

  @Override
  public List<Container> allocateContainers(
      ResourceBlacklistRequest blackList, List<ResourceRequest> oppResourceReqs,
      ApplicationAttemptId applicationAttemptId,
      OpportunisticContainerContext opportContext, long rmIdentifier,
      String appSubmitter) throws YarnException {

    // 更新节点黑名单
    updateBlacklist(blackList, opportContext);

    // 将新的机会容器请求加入待分配队列
    opportContext.addToOutstandingReqs(oppResourceReqs);

    // 获取当前黑名单集合
    Set<String> nodeBlackList = new HashSet<>(opportContext.getBlacklist());
    // 存储已分配容器结果
    List<Container> allocatedContainers = new ArrayList<>();
    // 获取单次AM心跳最大分配数量限制
    int maxAllocationsPerAMHeartbeat = getMaxAllocationsPerAMHeartbeat();
    // 存储按调度键分类的分配结果
    List<Map<Resource, List<Allocation>>> allocations = new ArrayList<>();

    // 按优先级降序遍历待分配请求
    for (SchedulerRequestKey schedulerKey :
        opportContext.getOutstandingOpReqs().descendingKeySet()) {
      // 计算本次心跳剩余可分配容器数量
      int remAllocs = -1;
      if (maxAllocationsPerAMHeartbeat > 0) {
        remAllocs =
            maxAllocationsPerAMHeartbeat - getTotalAllocations(allocations);
        // 已达到最大分配数量，停止分配
        if (remAllocs <= 0) {
          LOG.info("Not allocating more containers as we have reached max "
                  + "allocations per AM heartbeat {}",
              maxAllocationsPerAMHeartbeat);
          break;
        }
      }
      // 为当前优先级分配容器
      Map<Resource, List<Allocation>> allocation = allocatePerSchedulerKey(
          rmIdentifier, opportContext, schedulerKey, applicationAttemptId,
          appSubmitter, nodeBlackList, remAllocs);
      if (allocation.size() > 0) {
        allocations.add(allocation);
      }
    }
    // 将分配结果匹配到待分配请求，更新待分配队列并返回结果
    matchAllocation(allocations, allocatedContainers, opportContext);
    return allocatedContainers;
  }

  // 按调度键（优先级）分配容器
  private Map<Resource, List<Allocation>> allocatePerSchedulerKey(
      long rmIdentifier, OpportunisticContainerContext appContext,
      SchedulerRequestKey schedKey, ApplicationAttemptId appAttId,
      String userName, Set<String> blackList, int maxAllocations)
      throws YarnException {
    Map<Resource, List<Allocation>> allocations = new HashMap<>();
    int totalAllocated = 0;
    // 遍历当前优先级下所有待分配资源请求
    for (EnrichedResourceRequest enrichedAsk :
        appContext.getOutstandingOpReqs().get(schedKey).values()) {
      // 计算当前请求剩余可分配数量
      int remainingAllocs = -1;
      if (maxAllocations > 0) {
        remainingAllocs = maxAllocations - totalAllocated;
        // 已达到心跳分配上限，停止分配
        if (remainingAllocs <= 0) {
          LOG.info("Not allocating more containers as max allocations per AM "
              + "heartbeat {} has reached", getMaxAllocationsPerAMHeartbeat());
          break;
        }
      }

      // 为当前资源请求分配容器，累计已分配数量
      totalAllocated += allocateContainersPerRequest(rmIdentifier,
          appContext.getAppParams(),
          appContext.getContainerIdGenerator(), blackList,
          appAttId, userName, allocations, enrichedAsk,
          remainingAllocs);
      ResourceRequest anyAsk = enrichedAsk.getRequest();
      // 打印分配日志
      if (!allocations.isEmpty()) {
        LOG.info("Opportunistic allocation requested for [priority={}, "
                + "allocationRequestId={}, num_containers={}, capability={}] "
                + "allocated = {}", anyAsk.getPriority(),
            anyAsk.getAllocationRequestId(), anyAsk.getNumContainers(),
            anyAsk.getCapability(), allocations.keySet());
      }
    }
    return allocations;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  // 为单个资源请求分配容器，按照节点本地 -> 机架本地 -> 任意节点的顺序分配
  private int allocateContainersPerRequest(long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist,
      ApplicationAttemptId id,
      String userName, Map<Resource, List<Allocation>> allocations,
      EnrichedResourceRequest enrichedAsk, int maxAllocations)
      throws YarnException {
    ResourceRequest anyAsk = enrichedAsk.getRequest();
    int totalAllocated = 0;
    // 计算本次需要分配的容器总数
    int maxToAllocate = anyAsk.getNumContainers()
        - (allocations.isEmpty() ? 0 :
        allocations.get(anyAsk.getCapability()).size());
    // 受限于心跳最大分配数，取较小值
    if (maxAllocations >= 0) {
      maxToAllocate = Math.min(maxAllocations, maxToAllocate);
    }

    // 优先分配节点本地容器
    if (maxToAllocate > 0) {
      Map<String, AtomicInteger> nodeLocations = enrichedAsk.getNodeMap();
      // 遍历请求的所有节点位置
      for (Map.Entry<String, AtomicInteger> nodeLocation :
          nodeLocations.entrySet()) {
        int numContainers = nodeLocation.getValue().get();
        numContainers = Math.min(numContainers, maxToAllocate);
        // 在指定节点分配容器
        List<Container> allocatedContainers =
            allocateNodeLocal(enrichedAsk, nodeLocation.getKey(),
                numContainers, rmIdentifier, appParams, idCounter, blacklist,
                id, userName, allocations);
        // 更新计数
        totalAllocated += allocatedContainers.size();
        maxToAllocate -= allocatedContainers.size();
        // 已分配完需要的数量，退出
        if (maxToAllocate <= 0) {
          break;
        }
      }
    }

    // 节点本地分配后仍有剩余，尝试分配机架本地容器
    if (maxToAllocate > 0) {
      Map<String, AtomicInteger> rackLocations = enrichedAsk.getRackMap();
      // 遍历请求的所有机架位置
      for (Map.Entry<String, AtomicInteger> rack : rackLocations.entrySet()) {
        int numContainers = rack.getValue().get();
        numContainers = Math.min(numContainers, maxToAllocate);
        // 在指定机架分配容器
        List<Container> allocatedContainers =
            allocateRackLocal(enrichedAsk, rack.getKey(), numContainers,
                rmIdentifier, appParams, idCounter, blacklist, id,
                userName, allocations);
        // 更新计数
        totalAllocated += allocatedContainers.size();
        maxToAllocate -= allocatedContainers.size();
        // 已分配完需要的数量，退出
        if (maxToAllocate <= 0) {
          break;
        }
      }
    }

    // 机架本地分配后仍有剩余，在任意节点分配容器
    if (maxToAllocate > 0) {
      List<Container> allocatedContainers = allocateAny(enrichedAsk,
          maxToAllocate, rmIdentifier, appParams, idCounter, blacklist,
          id, userName, allocations);
      totalAllocated += allocatedContainers.size();
    }
    return totalAllocated;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  // 分配节点本机会容器，在指定节点分配指定数量机会容器
  private List<Container> allocateNodeLocal(
      EnrichedResourceRequest enrichedAsk,
      String nodeLocation,
      int toAllocate, long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist,
      ApplicationAttemptId id,
      String userName, Map<Resource, List<Allocation>> allocations)
      throws YarnException {
    List<Container> allocatedContainers = new ArrayList<>();
    final ResourceRequest resourceRequest = enrichedAsk.getRequest();
    // 循环分配直到达到需要数量或没有可用节点
    while (toAllocate > 0) {
      // 从负载监视器选择指定节点，满足资源要求且不在黑名单
      RMNode node = nodeQueueLoadMonitor.selectLocalNode(nodeLocation,
          blacklist, resourceRequest.getCapability());
      if (node != null) {
        toAllocate--;
        // 创建容器实例并加入分配结果
        Container container = createContainer(rmIdentifier, appParams,
            idCounter, id, userName, allocations, nodeLocation,
            resourceRequest, convertToRemoteNode(node));
        allocatedContainers.add(container);
        LOG.info("Allocated [{}] as opportunistic at location [{}]",
            container.getId(), nodeLocation);
        // 增加节点本地机会容器分配指标计数
        metrics.incrNodeLocalOppContainers();
      } else {
        // 没有可用节点，退出循环
        break;
      }
    }
    return allocatedContainers;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  // 分配机架本地机会容器，在指定机架内选择合适节点分配容器
  private List<Container> allocateRackLocal(EnrichedResourceRequest enrichedAsk,
      String rackLocation, int toAllocate, long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist,
      ApplicationAttemptId id,
      String userName, Map<Resource, List<Allocation>> allocations)
      throws YarnException {
    List<Container> allocatedContainers = new ArrayList<>();
    final ResourceRequest resourceRequest = enrichedAsk.getRequest();
    // 循环分配直到达到需要数量或没有可用节点
    while (toAllocate > 0) {
      // 从负载监视器选择指定机架内负载最低的可用节点
      RMNode node = nodeQueueLoadMonitor.selectRackLocalNode(rackLocation,
          blacklist, resourceRequest.getCapability());
      if (node != null) {
        toAllocate--;
        // 创建容器实例并加入分配结果
        Container container = createContainer(rmIdentifier, appParams,
            idCounter, id, userName, allocations, rackLocation,
            resourceRequest, convertToRemoteNode(node));
        allocatedContainers.add(container);
        // 增加机架本地机会容器分配指标计数
        metrics.incrRackLocalOppContainers();
        LOG.info("Allocated [{}] as opportunistic at location [{}]",
            container.getId(), rackLocation);
      } else {
        // 没有可用节点，退出循环
        break;
      }
    }
    return allocatedContainers;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  // 在集群任意节点分配机会容器，选择全局负载最低的可用节点
  private List<Container> allocateAny(EnrichedResourceRequest enrichedAsk,
      int toAllocate, long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist,
      ApplicationAttemptId id,
      String userName, Map<Resource, List<Allocation>> allocations)
      throws YarnException {
    List<Container> allocatedContainers = new ArrayList<>();
    final ResourceRequest resourceRequest = enrichedAsk.getRequest();
    // 循环分配直到达到需要数量或没有可用节点
    while (toAllocate > 0) {
      // 从负载监视器选择全局负载最低的可用节点
      RMNode node = nodeQueueLoadMonitor.selectAnyNode(
          blacklist, resourceRequest.getCapability());
      if (node != null) {
        toAllocate--;
        // 创建容器实例并加入分配结果
        Container container = createContainer(rmIdentifier, appParams,
            idCounter, id, userName, allocations, ResourceRequest.ANY,
            resourceRequest, convertToRemoteNode(node));
        allocatedContainers.add(container);
        // 增加跨交换机机会容器分配指标计数
        metrics.incrOffSwitchOppContainers();
        LOG.info("Allocated [{}] as opportunistic at location [{}]",
            container.getId(), ResourceRequest.ANY);
      } else {
        // 没有可用节点，退出循环
        break;
      }
    }
    return allocatedContainers;
  }

  // 将RMNode转换为RemoteNode，用于返回给AM的分配结果
  private RemoteNode convertToRemoteNode(RMNode rmNode) {
    if (rmNode != null) {
      RemoteNode rNode = RemoteNode.newInstance(rmNode.getNodeID(),
          rmNode.getHttpAddress());
      rNode.setRackName(rmNode.getRackName());
      return rNode;
    }
    return null;
  }
}