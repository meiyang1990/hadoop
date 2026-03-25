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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest.ResourceRequestComparator;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 文件说明：MapReduce ApplicationMaster向YARN ResourceManager请求容器的基类，
 * 维护所有容器请求的数据结构，处理节点黑名单机制，管理资源请求分配与释放流程。
 * 是MapReduce AppMaster与RM通信中容器请求管理的核心组件。
 */
public abstract class RMContainerRequestor extends RMCommunicator {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(RMContainerRequestor.class);
  private static final ResourceRequestComparator RESOURCE_REQUEST_COMPARATOR =
      new ResourceRequestComparator();

  protected int lastResponseID;
  private Resource availableResources;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  // 多级索引资源请求表：优先级 -> 资源名(主机/机架/ANY) -> 资源能力 -> 资源请求
  private final Map<Priority, Map<String, Map<Resource, ResourceRequest>>>
  remoteRequestsTable =
      new TreeMap<Priority, Map<String, Map<Resource, ResourceRequest>>>();

  // 使用自定义比较器，保证仅容器数量不同的ResourceRequest不会被当作重复对象
  private final Set<ResourceRequest> ask = new TreeSet<ResourceRequest>(
      RESOURCE_REQUEST_COMPARATOR);
  private final Set<ContainerId> release = new TreeSet<ContainerId>();
  // 待释放容器集合：只有RM确认容器完成后才会移除，区别于每次allocate请求携带的release集合
  protected Set<ContainerId> pendingRelease = new TreeSet<ContainerId>();

  private final Map<ResourceRequest,ResourceRequest> requestLimits =
      new TreeMap<ResourceRequest,ResourceRequest>(RESOURCE_REQUEST_COMPARATOR);
  private final Set<ResourceRequest> requestLimitsToUpdate =
      new TreeSet<ResourceRequest>(RESOURCE_REQUEST_COMPARATOR);

  private boolean nodeBlacklistingEnabled;
  private int blacklistDisablePercent;
  private AtomicBoolean ignoreBlacklisting = new AtomicBoolean(false);
  private int blacklistedNodeCount = 0;
  private int lastClusterNmCount = 0;
  private int clusterNmCount = 0;
  private int maxTaskFailuresPerNode;
  private final Map<String, Integer> nodeFailures = new HashMap<String, Integer>();
  private final Set<String> blacklistedNodes = Collections
      .newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final Set<String> blacklistAdditions = Collections
      .newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final Set<String> blacklistRemovals = Collections
      .newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  /**
   * 构造函数，初始化容器请求器
   * @param clientService MR客户端服务
   * @param context MR应用上下文
   */
  public RMContainerRequestor(ClientService clientService, AppContext context) {
    super(clientService, context);
  }

  /**
   * 容器请求内部数据结构，封装单个任务尝试的容器请求信息
   */
  @Private
  @VisibleForTesting
  static class ContainerRequest {
    final TaskAttemptId attemptID;
    final Resource capability;
    final String[] hosts;
    final String[] racks;
    //final boolean earlierAttemptFailed;
    final Priority priority;
    final String nodeLabelExpression;

    /**
     * 请求创建时间，用于避免对新请求的强制抢占
     */
    final long requestTimeMs;

    public ContainerRequest(ContainerRequestEvent event, Priority priority,
        String nodeLabelExpression) {
      this(event.getAttemptID(), event.getCapability(), event.getHosts(),
          event.getRacks(), priority, nodeLabelExpression);
    }

    public ContainerRequest(ContainerRequestEvent event, Priority priority,
                            long requestTimeMs) {
      this(event.getAttemptID(), event.getCapability(), event.getHosts(),
          event.getRacks(), priority, requestTimeMs,null);
    }

    public ContainerRequest(TaskAttemptId attemptID,
                            Resource capability, String[] hosts, String[] racks,
                            Priority priority, String nodeLabelExpression) {
      this(attemptID, capability, hosts, racks, priority,
          System.currentTimeMillis(), nodeLabelExpression);
    }

    public ContainerRequest(TaskAttemptId attemptID,
        Resource capability, String[] hosts, String[] racks,
        Priority priority, long requestTimeMs,String nodeLabelExpression) {
      this.attemptID = attemptID;
      this.capability = capability;
      this.hosts = hosts;
      this.racks = racks;
      this.priority = priority;
      this.requestTimeMs = requestTimeMs;
      this.nodeLabelExpression = nodeLabelExpression;
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("AttemptId[").append(attemptID).append("]");
      sb.append("Capability[").append(capability).append("]");
      sb.append("Priority[").append(priority).append("]");
      return sb.toString();
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 从配置加载节点黑名单开关
    nodeBlacklistingEnabled = 
      conf.getBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    LOG.info("nodeBlacklistingEnabled:" + nodeBlacklistingEnabled);
    // 加载单节点最大失败次数阈值
    maxTaskFailuresPerNode = 
      conf.getInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 3);
    // 加载禁用黑名单的阈值：黑名单节点占比超过该百分比则自动禁用黑名单
    blacklistDisablePercent =
        conf.getInt(
            MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT,
            MRJobConfig.DEFAULT_MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERCENT);
    LOG.info("maxTaskFailuresPerNode is " + maxTaskFailuresPerNode);
    // 校验百分比配置合法性
    if (blacklistDisablePercent < -1 || blacklistDisablePercent > 100) {
      throw new YarnRuntimeException("Invalid blacklistDisablePercent: "
          + blacklistDisablePercent
          + ". Should be an integer between 0 and 100 or -1 to disabled");
    }
    LOG.info("blacklistDisablePercent is " + blacklistDisablePercent);
  }

  /**
   * 构造并发送容器分配请求到ResourceManager，处理响应更新本地状态
   * @return RM返回的分配响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  protected AllocateResponse makeRemoteRequest() throws YarnException,
      IOException {
    // 应用请求数量限制
    applyRequestLimits();
    // 构造黑名单请求，包含新增和移除的节点
    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(new ArrayList<String>(blacklistAdditions),
            new ArrayList<String>(blacklistRemovals));
    // 构造完整的Allocate请求
    AllocateRequest allocateRequest =
        AllocateRequest.newInstance(lastResponseID,
          super.getApplicationProgress(), new ArrayList<ResourceRequest>(ask),
          new ArrayList<ContainerId>(release), blacklistRequest);
    // 发送请求到RM调度器
    AllocateResponse allocateResponse = scheduler.allocate(allocateRequest);
    // 更新响应ID，后续请求需要携带最新ID
    lastResponseID = allocateResponse.getResponseId();
    // 保存RM返回的可用资源信息
    availableResources = allocateResponse.getAvailableResources();
    // 更新集群NM计数
    lastClusterNmCount = clusterNmCount;
    clusterNmCount = allocateResponse.getNumClusterNodes();
    int numCompletedContainers =
        allocateResponse.getCompletedContainersStatuses().size();

    // 打印请求统计日志
    if (ask.size() > 0 || release.size() > 0) {
      LOG.info("applicationId={}: ask={} release={} newContainers={} finishedContainers={}"
              + " resourceLimit={} knownNMs={}", applicationId, ask.size(), release.size(),
          allocateResponse.getAllocatedContainers().size(), numCompletedContainers,
          availableResources, clusterNmCount);
    }

    // 清空本次请求的询问和释放集合
    ask.clear();
    release.clear();

    // 有容器完成时，需要更新请求限制重新发送有限请求
    if (numCompletedContainers > 0) {
      requestLimitsToUpdate.addAll(requestLimits.keySet());
    }

    // 打印黑名单更新日志
    if (blacklistAdditions.size() > 0 || blacklistRemovals.size() > 0) {
      LOG.info("Update the blacklist for " + applicationId +
          ": blacklistAdditions=" + blacklistAdditions.size() +
          " blacklistRemovals=" +  blacklistRemovals.size());
    }
    // 清空黑名单增量集合
    blacklistAdditions.clear();
    blacklistRemovals.clear();
    return allocateResponse;
  }

  /**
   * 应用最大并发容器请求限制，限制同一优先级下同时请求的容器数量
   */
  private void applyRequestLimits() {
    Iterator<ResourceRequest> iter = requestLimits.values().iterator();
    while (iter.hasNext()) {
      ResourceRequest reqLimit = iter.next();
      int limit = reqLimit.getNumContainers();
      // 从请求表获取对应请求
      Map<String, Map<Resource, ResourceRequest>> remoteRequests =
          remoteRequestsTable.get(reqLimit.getPriority());
      Map<Resource, ResourceRequest> reqMap = (remoteRequests != null)
          ? remoteRequests.get(ResourceRequest.ANY) : null;
      ResourceRequest req = (reqMap != null)
          ? reqMap.get(reqLimit.getCapability()) : null;
      if (req == null) {
        continue;
      }
      // 如果请求已存在，根据限制更新请求容器数量
      if (ask.remove(req) || requestLimitsToUpdate.contains(req)) {
        ResourceRequest newReq = req.getNumContainers() > limit
            ? reqLimit : req;
        ask.add(newReq);
        LOG.info("Applying ask limit of " + newReq.getNumContainers()
            + " for priority:" + reqLimit.getPriority()
            + " and capability:" + reqLimit.getCapability());
      }
      // 无限制则移除该限制记录
      if (limit == Integer.MAX_VALUE) {
        iter.remove();
      }
    }
    requestLimitsToUpdate.clear();
  }

  /**
   * 重新同步时将所有未完成请求重新添加到请求队列
   */
  protected void addOutstandingRequestOnResync() {
    // 遍历所有未完成请求加入ask队列
    for (Map<String, Map<Resource, ResourceRequest>> rr : remoteRequestsTable
        .values()) {
      for (Map<Resource, ResourceRequest> capabalities : rr.values()) {
        for (ResourceRequest request : capabalities.values()) {
          addResourceRequestToAsk(request);
        }
      }
    }
    // 黑名单节点重新上报RM
    if (!ignoreBlacklisting.get()) {
      blacklistAdditions.addAll(blacklistedNodes);
    }
    // 待释放容器重新加入释放列表
    if (!pendingRelease.isEmpty()) {
      release.addAll(pendingRelease);
    }
    // 更新所有请求限制
    requestLimitsToUpdate.addAll(requestLimits.keySet());
  }

  /**
   * 根据黑名单节点占比计算是否需要忽略黑名单机制
   * 注释说明：当黑名单节点占比超过配置阈值，会自动禁用黑名单避免作业无法分配资源
   */
  // May be incorrect if there's multiple NodeManagers running on a single host.
  // knownNodeCount is based on node managers, not hosts. blacklisting is
  // currently based on hosts.
  protected void computeIgnoreBlacklisting() {
    if (!nodeBlacklistingEnabled) {
      return;
    }
    // 只有当黑名单节点数或集群节点数变化时才重新计算
    if (blacklistDisablePercent != -1
        && (blacklistedNodeCount != blacklistedNodes.size() ||
            clusterNmCount != lastClusterNmCount)) {
      blacklistedNodeCount = blacklistedNodes.size();
      if (clusterNmCount == 0) {
        LOG.info("KnownNode Count at 0. Not computing ignoreBlacklisting");
        return;
      }
      // 计算黑名单节点占总节点数百分比
      int val = (int) ((float) blacklistedNodes.size() / clusterNmCount * 100);
      // 占比超过阈值，设置忽略黑名单
      if (val >= blacklistDisablePercent) {
        if (ignoreBlacklisting.compareAndSet(false, true)) {
          LOG.info("Ignore blacklisting set to true. Known: " + clusterNmCount
              + ", Blacklisted: " + blacklistedNodeCount + ", " + val + "%");
          // 通知RM移除所有黑名单节点
          blacklistAdditions.clear();
          blacklistRemovals.addAll(blacklistedNodes);
        }
      } else {
        // 占比低于阈值，恢复黑名单机制
        if (ignoreBlacklisting.compareAndSet(true, false)) {
          LOG.info("Ignore blacklisting set to false. Known: " + clusterNmCount
              + ", Blacklisted: " + blacklistedNodeCount + ", " + val + "%");
          // 通知RM重新添加所有黑名单节点
          blacklistAdditions.addAll(blacklistedNodes);
          blacklistRemovals.clear();
        }
      }
    }
  }
  
  /**
   * 处理节点上容器失败事件，超过失败阈值则将节点加入黑名单
   * @param hostName 失败容器所在主机名
   */
  protected void containerFailedOnHost(String hostName) {
    if (!nodeBlacklistingEnabled) {
      return;
    }
    // 已经在黑名单中直接返回
    if (blacklistedNodes.contains(hostName)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Host " + hostName + " is already blacklisted.");
      }
      return; //already blacklisted
    }
    // 更新失败计数
    Integer failures = nodeFailures.remove(hostName);
    failures = failures == null ? Integer.valueOf(0) : failures;
    failures++;
    LOG.info(failures + " failures on node " + hostName);
    // 失败次数超过阈值，加入黑名单
    if (failures >= maxTaskFailuresPerNode) {
      blacklistedNodes.add(hostName);
      // 如果黑名单未禁用，添加到增量列表通知RM
      if (!ignoreBlacklisting.get()) {
        blacklistAdditions.add(hostName);
      }
      LOG.info("Blacklisted host " + hostName);

      // 移除该主机对应的所有资源请求
      for (Map<String, Map<Resource, ResourceRequest>> remoteRequests 
          : remoteRequestsTable.values()){
        boolean foundAll = true;
        Map<Resource, ResourceRequest> reqMap = remoteRequests.get(hostName);
        if (reqMap != null) {
          // 遍历该主机下所有资源请求
          for (ResourceRequest req : reqMap.values()) {
            // 如果请求尚未发送到RM，直接移除
            if (!ask.remove(req)) {
              foundAll = false;