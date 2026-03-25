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

package org.apache.hadoop.yarn.server.scheduler;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * 分布式机会容器分配器，在给定节点列表上分配机会容器，根据ResourceManager限制调整容器大小，
 * 并尽可能均匀地将容器分布到各个节点上。用于YARN分布式机会调度场景，提升集群资源利用率。
 * </p>
 */
public class DistributedOpportunisticContainerAllocator
    extends OpportunisticContainerAllocator {

  // 节点本地区分标记
  private static final int NODE_LOCAL_LOOP = 0;
  // 机架本地区分标记
  private static final int RACK_LOCAL_LOOP = 1;
  // 跨交换机（非本地）区分标记
  private static final int OFF_SWITCH_LOOP = 2;

  private static final Logger LOG =
      LoggerFactory.getLogger(DistributedOpportunisticContainerAllocator.class);

  /**
   * 创建分布式机会容器分配器实例。
   * @param tokenSecretManager 容器令牌密钥管理器
   */
  public DistributedOpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager) {
    super(tokenSecretManager);
  }

  /**
   * 创建分布式机会容器分配器实例，指定单次心跳最大分配数量。
   * @param tokenSecretManager 容器令牌密钥管理器
   * @param maxAllocationsPerAMHeartbeat 单次AM心跳最大可分配容器数量
   */
  public DistributedOpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager,
      int maxAllocationsPerAMHeartbeat) {
    super(tokenSecretManager, maxAllocationsPerAMHeartbeat);
  }

  @Override
  public List<Container> allocateContainers(ResourceBlacklistRequest blackList,
      List<ResourceRequest> oppResourceReqs,
      ApplicationAttemptId applicationAttemptId,
      OpportunisticContainerContext opportContext, long rmIdentifier,
      String appSubmitter) throws YarnException {

    // 更新黑名单
    updateBlacklist(blackList, opportContext);

    // 将机会调度请求添加到待处理请求队列
    opportContext.addToOutstandingReqs(oppResourceReqs);
    Set<String> nodeBlackList = new HashSet<>(opportContext.getBlacklist());
    // 记录本轮已分配容器的节点，避免同一节点分配过多
    Set<String> allocatedNodes = new HashSet<>();
    List<Container> allocatedContainers = new ArrayList<>();

    // 循环处理待分配请求直到无法继续分配
    boolean continueLoop = true;
    while (continueLoop) {
      continueLoop = false;
      List<Map<Resource, List<Allocation>>> allocations = new ArrayList<>();
      // 按优先级从高到低处理待分配请求
      for (SchedulerRequestKey schedulerKey :
          opportContext.getOutstandingOpReqs().descendingKeySet()) {
        // 计算本轮心跳剩余可分配容器数量
        int remAllocs = -1;
        int maxAllocationsPerAMHeartbeat = getMaxAllocationsPerAMHeartbeat();
        if (maxAllocationsPerAMHeartbeat > 0) {
          remAllocs =
              maxAllocationsPerAMHeartbeat - allocatedContainers.size()
                  - getTotalAllocations(allocations);
          // 已达到单次心跳分配上限，停止分配
          if (remAllocs <= 0) {
            LOG.info("Not allocating more containers as we have reached max "
                    + "allocations per AM heartbeat {}",
                maxAllocationsPerAMHeartbeat);
            break;
          }
        }
        // 为当前优先级分配容器
        Map<Resource, List<Allocation>> allocation = allocate(
            rmIdentifier, opportContext, schedulerKey, applicationAttemptId,
            appSubmitter, nodeBlackList, allocatedNodes, remAllocs);
        if (allocation.size() > 0) {
          allocations.add(allocation);
          // 本次分配成功，继续循环尝试分配更多
          continueLoop = true;
        }
      }
      // 匹配分配结果，从待处理请求中扣除已分配容器
      matchAllocation(allocations, allocatedContainers, opportContext);
    }

    return allocatedContainers;
  }

  /**
   * 按优先级和资源规格分配机会容器。
   * @param rmIdentifier ResourceManager标识
   * @param appContext 应用分配上下文
   * @param schedKey 调度请求key（优先级+分区）
   * @param appAttId 应用尝试ID
   * @param userName 提交应用用户名
   * @param blackList 节点黑名单
   * @param allocatedNodes 已分配节点集合
   * @param maxAllocations 最大可分配数量
   * @return 分配结果，key为请求资源规格，value为分配列表
   * @throws YarnException 分配异常
   */
  private Map<Resource, List<Allocation>> allocate(long rmIdentifier,
      OpportunisticContainerContext appContext, SchedulerRequestKey schedKey,
      ApplicationAttemptId appAttId, String userName, Set<String> blackList,
      Set<String> allocatedNodes, int maxAllocations)
      throws YarnException {
    Map<Resource, List<Allocation>> containers = new HashMap<>();
    // 遍历当前优先级下所有资源请求
    for (EnrichedResourceRequest enrichedAsk :
        appContext.getOutstandingOpReqs().get(schedKey).values()) {
      // 计算剩余可分配数量
      int remainingAllocs = -1;
      if (maxAllocations > 0) {
        int totalAllocated = 0;
        for (List<Allocation> allocs : containers.values()) {
          totalAllocated += allocs.size();
        }
        remainingAllocs = maxAllocations - totalAllocated;
        if (remainingAllocs <= 0) {
          LOG.info("Not allocating more containers as max allocations per AM "
              + "heartbeat {} has reached", getMaxAllocationsPerAMHeartbeat());
          break;
        }
      }
      // 执行实际容器分配
      allocateContainersInternal(rmIdentifier, appContext.getAppParams(),
          appContext.getContainerIdGenerator(), blackList, allocatedNodes,
          appAttId, appContext.getNodeMap(), userName, containers, enrichedAsk,
          remainingAllocs);
      ResourceRequest anyAsk = enrichedAsk.getRequest();
      if (!containers.isEmpty()) {
        LOG.info("Opportunistic allocation requested for [priority={}, "
                + "allocationRequestId={}, num_containers={}, capability={}] "
                + "allocated = {}", anyAsk.getPriority(),
            anyAsk.getAllocationRequestId(), anyAsk.getNumContainers(),
            anyAsk.getCapability(), containers.keySet());
      }
    }
    return containers;
  }

  /**
   * 内部实际分配容器逻辑，遵循节点本地 -> 机架本地 -> 跨交换机的分配顺序。
   * @param rmIdentifier ResourceManager标识
   * @param appParams 分配参数
   * @param idCounter 容器ID生成器
   * @param blacklist 节点黑名单
   * @param allocatedNodes 已分配节点集合
   * @param id 应用尝试ID
   * @param allNodes 所有可用节点映射
   * @param userName 提交应用用户名
   * @param allocations 输出分配结果
   * @param enrichedAsk  enriched资源请求
   * @param maxAllocations 最大可分配数量
   * @throws YarnException 分配异常
   */
  private void allocateContainersInternal(long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      Set<String> blacklist, Set<String> allocatedNodes,
      ApplicationAttemptId id, Map<String, RemoteNode> allNodes,
      String userName, Map<Resource, List<Allocation>> allocations,
      EnrichedResourceRequest enrichedAsk, int maxAllocations)
      throws YarnException {
    if (allNodes.size() == 0) {
      LOG.info("No nodes currently available to " +
          "allocate OPPORTUNISTIC containers.");
      return;
    }
    ResourceRequest anyAsk = enrichedAsk.getRequest();
    // 计算本次需要分配的容器数量
    int toAllocate = anyAsk.getNumContainers()
        - (allocations.isEmpty() ? 0 :
        allocations.get(anyAsk.getCapability()).size());
    // 限制每轮分配最大数量，避免单次分配过多
    toAllocate = Math.min(toAllocate,
        appParams.getMaxAllocationsPerSchedulerKeyPerRound());
    if (maxAllocations >= 0) {
      toAllocate = Math.min(maxAllocations, toAllocate);
    }
    int numAllocated = 0;
    // 根据请求位置信息确定初始循环层级：有节点请求从节点本地开始，否则从跨交换机开始
    int loopIndex = OFF_SWITCH_LOOP;
    if (enrichedAsk.getNodeMap().size() > 0) {
      loopIndex = NODE_LOCAL_LOOP;
    }
    // 循环分配直到满足需要分配数量
    while (numAllocated < toAllocate) {
      // 根据当前层级查找候选节点
      Collection<RemoteNode> nodeCandidates =
          findNodeCandidates(loopIndex, allNodes, blacklist, allocatedNodes,
              enrichedAsk);
      // 遍历候选节点尝试分配
      for (RemoteNode rNode : nodeCandidates) {
        String rNodeHost = rNode.getNodeId().getHost();
        // 跳过黑名单节点
        if (blacklist.contains(rNodeHost)) {
          LOG.info("Nodes for scheduling has a blacklisted node" +
              " [" + rNodeHost + "]..");
          continue;
        }
        String location = ResourceRequest.ANY;
        // 节点本地位匹配检查
        if (loopIndex == NODE_LOCAL_LOOP) {
          if (enrichedAsk.getNodeMap().containsKey(rNodeHost)) {
            location = rNodeHost;
          } else {
            continue;
          }
        } else if (allocatedNodes.contains(rNodeHost)) {
          // 非本地位，避免同一节点分配多个机会容器
          LOG.info("Opportunistic container has already been allocated on {}.",
              rNodeHost);
          continue;
        }
        // 机架本地位匹配检查
        if (loopIndex == RACK_LOCAL_LOOP) {
          if (enrichedAsk.getRackMap().containsKey(
              rNode.getRackName())) {
            location = rNode.getRackName();
          } else {
            continue;
          }
        }
        // 创建容器实例
        Container container = createContainer(rmIdentifier, appParams,
            idCounter, id, userName, allocations, location,
            anyAsk, rNode);
        numAllocated++;
        // 更新调度metrics
        updateMetrics(loopIndex);
        allocatedNodes.add(rNodeHost);
        LOG.info("Allocated [" + container.getId() + "] as opportunistic at " +
            "location [" + location + "]");
        if (numAllocated >= toAllocate) {
          break;
        }
      }
      // 分配失败后升级层级：节点本地 -> 机架本地 -> 跨交换机
      if (loopIndex == NODE_LOCAL_LOOP &&
          enrichedAsk.getRackMap().size() > 0) {
        loopIndex = RACK_LOCAL_LOOP;
      } else {
        loopIndex++;
      }
      // 所有层级都分配失败，结束分配
      if (loopIndex > OFF_SWITCH_LOOP && numAllocated == 0) {
        LOG.warn("Unable to allocate any opportunistic containers.");
        break;
      }
    }
  }


  /**
   * 根据分配层级更新对应位置的调度指标。
   * @param loopIndex 当前分配层级
   */
  private void updateMetrics(int loopIndex) {
    OpportunisticSchedulerMetrics metrics =
        OpportunisticSchedulerMetrics.getMetrics();
    if (loopIndex == NODE_LOCAL_LOOP) {
      metrics.incrNodeLocalOppContainers();
    } else if (loopIndex == RACK_LOCAL_LOOP) {
      metrics.incrRackLocalOppContainers();
    } else {
      metrics.incrOffSwitchOppContainers();
    }
  }

  /**
   * 根据当前分配层级查找符合分区要求的候选节点列表。
   * @param loopIndex 当前分配层级
   * @param allNodes 所有可用节点映射
   * @param blackList 节点黑名单
   * @param allocatedNodes 已分配节点集合
   * @param enrichedRR  enriched资源请求
   * @return 候选节点列表
   */
  private Collection<RemoteNode> findNodeCandidates(int loopIndex,
      Map<String, RemoteNode> allNodes, Set<String> blackList,
      Set<String> allocatedNodes, EnrichedResourceRequest enrichedRR) {
    LinkedList<RemoteNode> retList = new LinkedList<>();
    String partition = getRequestPartition(enrichedRR);
    // 跨交换机层级，收集所有同分区可用节点
    if (loopIndex > 1) {
      for (RemoteNode remoteNode : allNodes.values()) {
        if (StringUtils.equals(partition, getRemoteNodePartition(remoteNode))) {
          retList.add(remoteNode);
        }
      }
      return retList;
    } else {
      // 节点本地或机架本地层级，收集对应位置的候选节点
      int numContainers = enrichedRR.getRequest().getNumContainers();
      while (numContainers > 0) {
        if (loopIndex == 0) {
          // 收集节点本地候选节点
          numContainers = collectNodeLocalCandidates(
              allNodes, enrichedRR, retList, numContainers);
        } else {
          // 收集机架本地候选节点
          numContainers =
              collectRackLocalCandidates(allNodes, enrichedRR, retList,
                  blackList, allocatedNodes, numContainers);
        }
        // 如果本次循环没有收集到新节点，停止循环
        if (numContainers == enrichedRR.getRequest().getNumContainers()) {
          break;
        }
      }
      return retList;
    }
  }

  /**
   * 收集机架本地候选节点，优先放置在未分配过机会容器的节点。
   * @param allNodes 所有可用节点映射
   * @param enrichedRR enriched资源请求
   * @param retList 输出候选节点列表
   * @param blackList 节点黑名单
   * @param allocatedNodes 已分配节点集合
   * @param numContainers 需要收集的节点数量
   * @return 剩余还需要收集的节点数量
   */
  private int collectRackLocalCandidates(Map<String, RemoteNode> allNodes,
      EnrichedResourceRequest enrichedRR, LinkedList<RemoteNode> retList,
      Set<String> blackList, Set<String> allocatedNodes, int numContainers) {
    String partition = getRequestPartition(enrichedRR);
    for (RemoteNode rNode : allNodes.values()) {
      if (StringUtils.equals(partition, getRemoteNodePartition(rNode)) &&
          enrichedRR.getRackMap().containsKey(rNode.getRackName())) {
        String rHost = rNode.getNodeId().getHost();
        if (blackList.contains(rHost)) {
          continue;
        }
        // 已分配节点放到队尾，未分配放到队头优先分配，实现尽量分散
        if (allocatedNodes.contains(rHost)) {
          retList.addLast(rNode);
        } else {
          retList.addFirst(rNode);
          numContainers--;
        }
      }
      if (numContainers == 0) {
        break;
      }
    }
    return numContainers;
  }

  /**
   * 收集节点本地候选节点。
   * @param allNodes 所有可用节点映射
   * @param enrichedRR enriched资源请求
   * @param retList 输出候选节点列表
   * @param numContainers 需要收集的节点数量
   * @return 剩余还需要收集的节点数量
   */
  private int collectNodeLocalCandidates(Map<String, RemoteNode> allNodes,
      EnrichedResourceRequest enrichedRR, List<Remote