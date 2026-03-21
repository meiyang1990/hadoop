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

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;

import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.security.BaseContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * 机会容器分配的基础抽象类，提供机会容器分配所需的通用功能。
 * 机会容器是YARN中利用集群空闲资源调度的低优先级容器，可提升集群资源利用率。
 * </p>
 */
public abstract class OpportunisticContainerAllocator {

  private int maxAllocationsPerAMHeartbeat = -1;

  /**
   * 封装构建容器所需的应用级参数。
   */
  public static class AllocationParams {
    private Resource maxResource;
    private Resource minResource;
    private Resource incrementResource;
    private int containerTokenExpiryInterval;
    private int maxAllocationsPerSchedulerKeyPerRound = 1;

    /**
     * 返回最大资源规格。
     * @return 最大资源规格
     */
    public Resource getMaxResource() {
      return maxResource;
    }

    /**
     * 设置最大资源规格。
     * @param maxResource 最大资源规格
     */
    public void setMaxResource(Resource maxResource) {
      this.maxResource = maxResource;
    }

    /**
     * 获取最小资源规格。
     * @return 最小资源规格
     */
    public Resource getMinResource() {
      return minResource;
    }

    /**
     * 设置最小资源规格。
     * @param minResource 最小资源规格
     */
    public void setMinResource(Resource minResource) {
      this.minResource = minResource;
    }

    /**
     * 获取资源增量步长。
     * @return 资源增量步长
     */
    public Resource getIncrementResource() {
      return incrementResource;
    }

    /**
     * 设置资源增量步长。
     * @param incrementResource 资源增量步长
     */
    public void setIncrementResource(Resource incrementResource) {
      this.incrementResource = incrementResource;
    }

    /**
     * 获取容器令牌过期间隔。
     * @return 容器令牌过期间隔
     */
    public int getContainerTokenExpiryInterval() {
      return containerTokenExpiryInterval;
    }

    /**
     * 设置容器令牌过期时间（毫秒）。
     * @param containerTokenExpiryInterval 容器令牌过期时间（毫秒）
     */
    public void setContainerTokenExpiryInterval(
        int containerTokenExpiryInterval) {
      this.containerTokenExpiryInterval = containerTokenExpiryInterval;
    }

    /**
     * 获取每轮分配每个调度键的最大分配数。
     * @return 每轮分配每个调度键的最大分配数
     */
    public int getMaxAllocationsPerSchedulerKeyPerRound() {
      return maxAllocationsPerSchedulerKeyPerRound;
    }

    /**
     * 设置每轮分配每个调度键的最大分配数。
     * @param maxAllocationsPerSchedulerKeyPerRound 最大分配数
     */
    public void setMaxAllocationsPerSchedulerKeyPerRound(
        int maxAllocationsPerSchedulerKeyPerRound) {
      this.maxAllocationsPerSchedulerKeyPerRound =
          maxAllocationsPerSchedulerKeyPerRound;
    }
  }

  /**
   * 容器ID生成器，为分配的机会容器生成唯一ID。
   */
  public static class ContainerIdGenerator {

    protected volatile AtomicLong containerIdCounter = new AtomicLong(1);

    /**
     * 将生成器计数器重置为指定起始值。
     * @param containerIdStart 起始容器ID值
     */
    public void resetContainerIdCounter(long containerIdStart) {
      this.containerIdCounter.set(containerIdStart);
    }

    /**
     * 生成新的容器ID序号，默认实现对原子计数器自增。子类可重写该行为。
     * @return 新的容器ID序号
     */
    public long generateContainerId() {
      return this.containerIdCounter.incrementAndGet();
    }
  }

  /**
   * 按执行类型分区的资源请求集合，分别保存保障型和机会型资源请求。
   */
  public static class PartitionedResourceRequests {
    private List<ResourceRequest> guaranteed = new ArrayList<>();
    private List<ResourceRequest> opportunistic = new ArrayList<>();

    public List<ResourceRequest> getGuaranteed() {
      return guaranteed;
    }

    public List<ResourceRequest> getOpportunistic() {
      return opportunistic;
    }
  }

  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DominantResourceCalculator();

  private final BaseContainerTokenSecretManager tokenSecretManager;

  /**
   * 封装一次分配结果，包含已分配容器和分配位置资源名称。
   */
  public static class Allocation {
    private final Container container;
    private final String resourceName;

    /**
     * 创建分配结果实例。
     * @param container 已分配容器
     * @param resourceName 分配位置资源名称
     */
    public Allocation(Container container, String resourceName) {
      this.container = container;
      this.resourceName = resourceName;
    }

    /**
     * 获取已分配容器。
     * @return 已分配容器
     */
    public Container getContainer() {
      return container;
    }

    /**
     * 获取分配位置资源名称。
     * @return 分配位置资源名称
     */
    public String getResourceName() {
      return resourceName;
    }
  }

  /**
   * 增强型资源请求，按节点和机架分别统计可分配位置信息。
   */
  public static class EnrichedResourceRequest {
    private final Map<String, AtomicInteger> nodeLocations = new HashMap<>();
    private final Map<String, AtomicInteger> rackLocations = new HashMap<>();
    private final ResourceRequest request;
    private final long timestamp;

    public EnrichedResourceRequest(ResourceRequest request) {
      this.request = request;
      timestamp = Time.monotonicNow();
    }

    public long getTimestamp() {
      return timestamp;
    }

    public ResourceRequest getRequest() {
      return request;
    }

    /**
     * 添加请求位置并设置请求数量。
     * @param location 位置名称
     * @param count 请求数量
     */
    public void addLocation(String location, int count) {
      Map<String, AtomicInteger> m = rackLocations;
      if (!location.startsWith("/")) {
        m = nodeLocations;
      }
      if (count == 0) {
        m.remove(location);
      } else {
        m.put(location, new AtomicInteger(count));
      }
    }

    /**
     * 减少一个该位置的请求计数。
     * @param location 位置名称
     */
    public void removeLocation(String location) {
      Map<String, AtomicInteger> m = rackLocations;
      AtomicInteger count = m.get(location);
      if (count == null) {
        m = nodeLocations;
        count = m.get(location);
      }

      if (count != null) {
        if (count.decrementAndGet() == 0) {
          m.remove(location);
        }
      }
    }

    public Map<String, AtomicInteger> getNodeMap() {
      return nodeLocations;
    }

    public Map<String, AtomicInteger> getRackMap() {
      return rackLocations;
    }
  }

  /**
   * 创建机会容器分配器实例。
   * @param tokenSecretManager 容器令牌密钥管理器
   */
  public OpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager) {
    this.tokenSecretManager = tokenSecretManager;
  }

  /**
   * 创建机会容器分配器实例，指定单次心跳最大分配数。
   * @param tokenSecretManager 容器令牌密钥管理器
   * @param maxAllocationsPerAMHeartbeat 单次AM心跳最大分配容器数量
   */
  public OpportunisticContainerAllocator(
      BaseContainerTokenSecretManager tokenSecretManager,
      int maxAllocationsPerAMHeartbeat) {
    this.tokenSecretManager = tokenSecretManager;
    this.maxAllocationsPerAMHeartbeat = maxAllocationsPerAMHeartbeat;
  }

  public void setMaxAllocationsPerAMHeartbeat(
      int maxAllocationsPerAMHeartbeat) {
    this.maxAllocationsPerAMHeartbeat = maxAllocationsPerAMHeartbeat;
  }

  /**
   * 获取单次AM心跳最大分配容器数量。
   * @return 单次AM心跳最大分配容器数量
   */
  public int getMaxAllocationsPerAMHeartbeat() {
    return this.maxAllocationsPerAMHeartbeat;
  }

  /**
   * 分配机会容器，由具体子类实现分配逻辑。
   * @param blackList 资源黑名单请求
   * @param oppResourceReqs 机会型资源请求列表
   * @param applicationAttemptId 应用尝试ID
   * @param opportContext 应用特定的机会容器分配上下文
   * @param rmIdentifier RM标识
   * @param appSubmitter 应用提交者用户名
   * @return 已分配容器列表
   * @throws YarnException Yarn异常
   */
  public abstract List<Container> allocateContainers(
      ResourceBlacklistRequest blackList,
      List<ResourceRequest> oppResourceReqs,
      ApplicationAttemptId applicationAttemptId,
      OpportunisticContainerContext opportContext, long rmIdentifier,
      String appSubmitter) throws YarnException;


  /**
   * 更新应用黑名单，处理新增和移除的节点。
   * @param blackList 黑名单更新请求
   * @param oppContext 机会容器分配上下文
   */
  protected void updateBlacklist(ResourceBlacklistRequest blackList,
      OpportunisticContainerContext oppContext) {
    if (blackList != null) {
      oppContext.getBlacklist().removeAll(blackList.getBlacklistRemovals());
      oppContext.getBlacklist().addAll(blackList.getBlacklistAdditions());
    }
  }

  /**
   * 将已分配容器匹配到未完成请求，并收集已分配容器结果。
   * @param allocations 按资源规格分组的分配结果
   * @param allocatedContainers 收集已分配容器的列表
   * @param oppContext 机会容器分配上下文
   */
  protected void matchAllocation(List<Map<Resource,
      List<Allocation>>> allocations, List<Container> allocatedContainers,
      OpportunisticContainerContext oppContext) {
    for (Map<Resource, List<Allocation>> allocation : allocations) {
      for (Map.Entry<Resource, List<Allocation>> e : allocation.entrySet()) {
        oppContext.matchAllocationToOutstandingRequest(
            e.getKey(), e.getValue());
        for (Allocation alloc : e.getValue()) {
          allocatedContainers.add(alloc.getContainer());
        }
      }
    }
  }

  /**
   * 计算所有分配结果的总容器数量。
   * @param allocations 按资源规格分组的分配结果
   * @return 总分配容器数
   */
  protected int getTotalAllocations(
      List<Map<Resource, List<Allocation>>> allocations) {
    int totalAllocs = 0;
    for (Map<Resource, List<Allocation>> allocation : allocations) {
      for (List<Allocation> allocs : allocation.values()) {
        totalAllocs += allocs.size();
      }
    }
    return totalAllocs;
  }

  @SuppressWarnings("checkstyle:parameternumber")
  protected Container createContainer(long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      ApplicationAttemptId id, String userName,
      Map<Resource, List<Allocation>> allocations, String location,
      ResourceRequest anyAsk, RemoteNode rNode) throws YarnException {
    // 构建容器实例
    Container container = buildContainer(rmIdentifier, appParams,
        idCounter, anyAsk, id, userName, rNode);
    // 获取对应资源规格的分配列表
    List<Allocation> allocList = allocations.get(anyAsk.getCapability());
    if (allocList == null) {
      allocList = new ArrayList<>();
      allocations.put(anyAsk.getCapability(), allocList);
    }
    // 添加本次分配到列表
    allocList.add(new Allocation(container, location));
    return container;
  }

  private Container buildContainer(long rmIdentifier,
      AllocationParams appParams, ContainerIdGenerator idCounter,
      ResourceRequest rr, ApplicationAttemptId id, String userName,
      RemoteNode node) throws YarnException {
    // 生成容器ID
    ContainerId cId =
        ContainerId.newContainerId(id, idCounter.generateContainerId());

    // 规范化资源请求，对齐集群资源最小单元和边界
    Resource capability = normalizeCapability(appParams, rr);

    // 创建并返回容器实例
    return createContainer(
        rmIdentifier, appParams.getContainerTokenExpiryInterval(),
        SchedulerRequestKey.create(rr), userName, node, cId, capability);
  }

  @SuppressWarnings("checkstyle:parameternumber")
  private Container createContainer(long rmIdentifier, long tokenExpiry,
      SchedulerRequestKey schedulerKey, String userName, RemoteNode node,
      ContainerId cId, Resource capability) {
    // 获取当前时间计算令牌过期时间
    long currTime = System.currentTimeMillis();
    // 创建容器令牌标识符
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(
            cId, 0, node.getNodeId().toString(), userName,
            capability, currTime + tokenExpiry,
            tokenSecretManager.getCurrentKey().getKeyId(), rmIdentifier,
            schedulerKey.getPriority(), currTime,
            null, getRemoteNodePartition(node), ContainerType.TASK,
            ExecutionType.OPPORTUNISTIC, schedulerKey.getAllocationRequestId());
    // 生成容器令牌密码
    byte[] pwd =
        tokenSecretManager.createPassword(containerTokenIdentifier);
    // 创建容器令牌
    Token containerToken = newContainerToken(node.getNodeId(), pwd,
        containerTokenIdentifier);
    // 构建并返回容器对象
    Container container = BuilderUtils.newContainer(
        cId, node.getNodeId(), node.getHttpAddress(),
        capability, schedulerKey.getPriority(), containerToken,
        containerTokenIdentifier.getExecutionType(),
        schedulerKey.getAllocationRequestId());
    return container;
  }

  /**
   * 规范化资源规格，使其符合应用配置的最小/最大/增量要求。
   * @param appParams 应用分配参数
   * @param ask 原始请求资源规格
   * @return 规范化后的资源规格
   */
  private Resource normalizeCapability(AllocationParams appParams,
      ResourceRequest ask) {
    return Resources.normalize(RESOURCE_CALCULATOR,
        ask.getCapability(), appParams.minResource, appParams.maxResource,
        appParams.incrementResource);
  }

  /**
   * 创建容器令牌，设置正确的服务地址信息。
   * @param nodeId 节点ID
   * @param password 令牌密码
   * @param tokenIdentifier 令牌标识符
   * @return 新创建的容器令牌
   */
  private static Token newContainerToken(NodeId nodeId, byte[] password,
      ContainerTokenIdentifier tokenIdentifier) {
    // 创建节点网络地址
    InetSocketAddress addr = NetUtils.createSocketAddrForHost(nodeId.getHost(),
        nodeId.getPort());
    // 构建令牌并设置服务标识
    Token containerToken = Token.newInstance(tokenIdentifier.getBytes(),