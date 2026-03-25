// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.EnhancedHeadroom;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.NoActiveSubclustersException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOAD_BASED_SC_SELECTOR_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_LOAD_BASED_SC_SELECTOR_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOAD_BASED_SC_SELECTOR_THRESHOLD;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_LOAD_BASED_SC_SELECTOR_THRESHOLD;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOAD_BASED_SC_SELECTOR_USE_ACTIVE_CORE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_LOAD_BASED_SC_SELECTOR_USE_ACTIVE_CORE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOAD_BASED_SC_SELECTOR_MULTIPLIER;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_LOAD_BASED_SC_SELECTOR_MULTIPLIER;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOAD_BASED_SC_SELECTOR_FAIL_ON_ERROR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_LOAD_BASED_SC_SELECTOR_FAIL_ON_ERROR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_BLACKLIST_SUBCLUSTERS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_FEDERATION_BLACKLIST_SUBCLUSTERS;

/**
 * YARN联邦环境下AMRMProxy策略实现，基于位置性多播分发资源请求，同时结合权重和可用资源进行分发。
 * 实现了{@link FederationAMRMProxyPolicy}接口，核心行为如下：
 *
 * <p>
 * 节点位置性{@link ResourceRequest}：总是转发给对应节点所属的子集群RM，解析失败时默认转发到home子集群。
 * </p>
 *
 * <p>
 * 机架位置性{@link ResourceRequest}：转发给对应机架所属的所有子集群RM，解析失败时默认转发到home子集群。
 * </p>
 *
 * <p>
 * ANY位置请求（对应节点/机架请求）：仅转发给已有位置性请求的子集群集合，每个子集群分配的容器数量与该子集群下位置性请求数量成正比。
 * </p>
 *
 * <p>
 * 无关联位置性请求的ANY请求：根据配置权重和子集群可用资源（headroom）进行分发。headroomAlpha参数控制
 * headroom对分发结果的影响程度：1.0表示完全基于headroom分发，0.0表示完全基于配置权重分发。
 * </p>
 *
 * <p>
 * 零容器ANY请求：转发给所有已知子集群（通常用于取消之前的请求，由于当前是无状态设计需要转发给所有RM）。
 * </p>
 *
 * <p>
 * 不变量：
 * </p>
 *
 * <p>
 * 策略始终排除非活跃RM。
 * </p>
 *
 * <p>
 * 策略始终排除配置权重为0或不在配置中的RM，即使位置性请求明确指向它。
 * </p>
 *
 * <p>
 * （除了分数容器向上取整带来的误差）ANY级别分发后多个RM的请求总和等于用户原始请求，最大误差不超过联邦中子集群的数量。
 * </p>
 */
public class LocalityMulticastAMRMProxyPolicy extends AbstractAMRMProxyPolicy {

  public static final Logger LOG =
      LoggerFactory.getLogger(LocalityMulticastAMRMProxyPolicy.class);

  private static Random rand = new Random();

  private Map<SubClusterId, Float> weights;
  private SubClusterResolver resolver;

  private Configuration conf;
  private Map<SubClusterId, Resource> headroom;
  private Map<SubClusterId, EnhancedHeadroom> enhancedHeadroom;
  private float hrAlpha;
  private FederationStateStoreFacade federationFacade;
  private SubClusterId homeSubcluster;
  private int printRRMax;
  public static final String PRINT_RR_MAX =
      "yarn.nodemanager.amrmproxy.address.splitmerge.printmaxrrcount";
  public static final int DEFAULT_PRINT_RR_MAX = 1000;
  private boolean failOnError = DEFAULT_LOAD_BASED_SC_SELECTOR_FAIL_ON_ERROR;

  /**
   * 格式化打印资源请求列表为单行字符串。
   *
   * @param response 资源请求列表
   * @param max 最大打印数量
   * @return 格式化后的单行字符串
   */
  public static String prettyPrintRequests(List<ResourceRequest> response, int max) {
    StringBuilder builder = new StringBuilder();
    for (ResourceRequest rr : response) {
      builder.append("[id:").append(rr.getAllocationRequestId())
          .append(" loc:")
          .append(rr.getResourceName())
          .append(" num:")
          .append(rr.getNumContainers())
          .append(" pri:")
          .append(((rr.getPriority() != null) ? rr.getPriority().getPriority() : -1))
          .append("], ");
      if (max != -1) {
        if (max-- <= 0) {
          break;
        }
      }
    }
    return builder.toString();
  }

  @Override
  public void reinitialize(
      FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {

    // 保存旧权重引用，初始化失败时回滚
    WeightedPolicyInfo tempPolicy = getPolicyInfo();

    super.reinitialize(policyContext);
    if (!getIsDirty()) {
      return;
    }

    Map<SubClusterId, Float> newWeightsConverted = new HashMap<>();
    boolean allInactive = true;
    WeightedPolicyInfo policy = getPolicyInfo();

    if (policy.getAMRMPolicyWeights() != null
        && policy.getAMRMPolicyWeights().size() > 0) {
      for (Map.Entry<SubClusterIdInfo, Float> e : policy.getAMRMPolicyWeights()
          .entrySet()) {
        if (e.getValue() > 0) {
          allInactive = false;
        }
        newWeightsConverted.put(e.getKey().toId(), e.getValue());
      }
    }
    if (allInactive) {
      // 回滚策略配置并抛出异常
      setPolicyInfo(tempPolicy);
      throw new FederationPolicyInitializationException(
          "The weights used to configure "
              + "this policy are all set to zero! (no ResourceRequest could be "
              + "forwarded with this setting.)");
    }

    if (policyContext.getHomeSubcluster() == null) {
      setPolicyInfo(tempPolicy);
      throw new FederationPolicyInitializationException("The homeSubcluster "
          + "filed in the context must be initialized to use this policy");
    }

    weights = newWeightsConverted;
    resolver = policyContext.getFederationSubclusterResolver();

    // 数据结构仅需初始化一次
    if (headroom == null) {
      headroom = new ConcurrentHashMap<>();
      enhancedHeadroom = new ConcurrentHashMap<>();
    }
    hrAlpha = policy.getHeadroomAlpha();

    this.federationFacade =
        policyContext.getFederationStateStoreFacade();
    this.homeSubcluster = policyContext.getHomeSubcluster();

    this.conf = this.federationFacade.getConf();
    this.printRRMax = this.conf.getInt(PRINT_RR_MAX, DEFAULT_PRINT_RR_MAX);
    this.failOnError = this.conf.getBoolean(LOAD_BASED_SC_SELECTOR_FAIL_ON_ERROR,
        DEFAULT_LOAD_BASED_SC_SELECTOR_FAIL_ON_ERROR);
  }

  @Override
  public void notifyOfResponse(SubClusterId subClusterId,
      AllocateResponse response) throws YarnException {
    // 更新子集群可用资源信息
    if (response.getAvailableResources() != null) {
      headroom.put(subClusterId, response.getAvailableResources());
    }
    // 更新子集群增强可用资源信息
    if (response.getEnhancedHeadroom() != null) {
      this.enhancedHeadroom.put(subClusterId, response.getEnhancedHeadroom());
    }
    LOG.info(
        "Subcluster {} updated with AvailableResource {}, EnhancedHeadRoom {}",
        subClusterId, response.getAvailableResources(),
        response.getEnhancedHeadroom());
  }

  @Override
  public Map<SubClusterId, List<ResourceRequest>> splitResourceRequests(
      List<ResourceRequest> resourceRequests,
      Set<SubClusterId> timedOutSubClusters) throws YarnException {

    // 初始化簿记器，统计每个子集群请求信息，每次调用新建实例支持并发
    AllocationBookkeeper bookkeeper = new AllocationBookkeeper();
    bookkeeper.reinitialize(getActiveSubclusters(), timedOutSubClusters, conf);

    List<ResourceRequest> nonLocalizedRequests = new ArrayList<>();

    SubClusterId targetId = null;
    Set<SubClusterId> targetIds = null;

    // 遍历处理所有资源请求，解析节点/机架位置性请求
    for (ResourceRequest rr : resourceRequests) {
      targetId = null;
      targetIds = null;

      // ANY位置请求，暂存后续统一处理
      if (ResourceRequest.isAnyLocation(rr.getResourceName())) {
        nonLocalizedRequests.add(rr);
        continue;
      }

      // 处理节点位置请求
      try {
        targetId = resolver.getSubClusterForNode(rr.getResourceName());

        // 若开启基于负载的子集群选择，根据负载重路由节点请求
        boolean loadBasedSCSelectorEnabled =
            conf.getBoolean(LOAD_BASED_SC_SELECTOR_ENABLED, DEFAULT_LOAD_BASED_SC_SELECTOR_ENABLED);
        if (loadBasedSCSelectorEnabled) {
          int maxPendingThreshold = conf.getInt(LOAD_BASED_SC_SELECTOR_THRESHOLD,
              DEFAULT_LOAD_BASED_SC_SELECTOR_THRESHOLD);
          targetId = routeNodeRequestIfNeeded(targetId, maxPendingThreshold,
              bookkeeper.getActiveAndEnabledSC());
        }
        LOG.debug("Node request {}", rr.getResourceName());
      } catch (YarnException e) {
        // 解析失败，可能无法区分节点名和机架名，后续统一处理
      }
      if (bookkeeper.isActiveAndEnabled(targetId)) {
        bookkeeper.addLocalizedNodeRR(targetId, rr);
        continue;
      }

      // 处理机架位置请求
      try {
        targetIds = resolver.getSubClustersForRack(rr.getResourceName());
      } catch (YarnException e) {
        // 解析失败，后续统一处理
      }
      if (targetIds != null && targetIds.size() > 0) {
        boolean hasActive = false;
        for (SubClusterId tid : targetIds) {
          if (bookkeeper.isActiveAndEnabled(tid)) {
            bookkeeper.addRackRR(tid, rr);
            hasActive = true;
          }
        }
        if (hasActive) {
          continue;
        }
      }

      // 解析失败的节点/机架请求，从活跃子集群中随机选择一个
      targetId = getSubClusterForUnResolvedRequest(bookkeeper,
          rr.getAllocationRequestId());
      LOG.debug("ERROR resolving sub-cluster for resourceName: {}, picked a "
          + "random subcluster to forward:{}", rr.getResourceName(), targetId);
      if (targetIds != null && targetIds.size() > 0) {
        bookkeeper.addRackRR(targetId, rr);
      } else {
        bookkeeper.addLocalizedNodeRR(targetId, rr);
      }
    }

    // 处理所有非位置性ANY请求
    splitAnyRequests(nonLocalizedRequests, bookkeeper);

    // 获取分发结果并打印日志
    Map<SubClusterId, List<ResourceRequest>> answer = bookkeeper.getAnswer();
    LOG.info("Before split {} RRs: {}", resourceRequests.size(),
        prettyPrintRequests(resourceRequests, this.printRRMax));

    for (Map.Entry<SubClusterId, List<ResourceRequest>> entry : bookkeeper.getAnswer().entrySet()) {
      LOG.info("After split {} has {} RRs: {}", entry.getKey(), entry.getValue().size(),
          prettyPrintRequests(entry.getValue(), this.printRRMax));
    }
    return answer;
  }

  /**
   * 获取未解析请求的目标子集群，供单元测试覆盖。
   *
   * @param bookKeeper 簿记器
   * @param allocationId 分配请求ID
   * @return 目标子集群ID
   */
  protected SubClusterId getSubClusterForUnResolvedRequest(
      AllocationBookkeeper bookKeeper, long allocationId) {
    return bookKeeper.getSubClusterForUnResolvedRequest(allocationId);
  }

  /**
   * 将非位置性资源请求分发到各个子集群。
   */
  private void splitAnyRequests(List<ResourceRequest> originalResourceRequests,
      AllocationBookkeeper allocationBookkeeper) throws YarnException {

    for (ResourceRequest resourceRequest : originalResourceRequests) {

      // 第一步：确定目标子集群集合，若该ANY请求关联了已有位置性请求则使用对应集合，否则使用所有活跃子集群
      Long allocationId = resourceRequest.getAllocationRequestId();
      Set<SubClusterId> targetSubclusters;
      if (allocationBookkeeper.getSubClustersForId(allocationId) != null) {
        targetSubclusters =
            allocationBookkeeper.getSubClustersForId(allocationId);
      } else {
        targetSubclusters = allocationBookkeeper.getActiveAndEnabledSC();
      }

      // 第二步：为每个子集群计算应分配的容器数量并添加结果
      splitIndividualAny(resourceRequest, targetSubclusters,
          allocationBookkeeper);
    }
  }

  /**
   * 拆分单个ANY请求，根据位置性请求数量/权重+可用资源计算每个子集群应分配的容器数。
   */
  private void splitIndividualAny(ResourceRequest originalResourceRequest,
      Set<SubClusterId> targetSubclusters,
      AllocationBookkeeper allocationBookkeeper) throws YarnException {

    long allocationId = originalResourceRequest.getAllocationRequestId();
    int numContainer = originalResourceRequest.getNumContainers();

    // 零容器ANY必须转发给所有之前联系过的RM，通常用于取消之前的请求
    if (numContainer == 0) {
      for (SubClusterId targetId : headroom.keySet()) {
        allocationBookkeeper.addAnyRR(targetId, originalResourceRequest);
      }
      return;
    }

    // 保留迭代顺序，转为列表方便处理
    List<SubClusterId> targetSCs = new ArrayList<>(targetSubclusters);

    // 计算每个子集群的权重
    ArrayList<Float> weightsList = new ArrayList<>();
    for (SubClusterId targetId : targetSCs) {
      // 若关联位置性请求，按位置性请求数量比例计算权重
      if (allocationBookkeeper.getSubClustersForId(allocationId) != null