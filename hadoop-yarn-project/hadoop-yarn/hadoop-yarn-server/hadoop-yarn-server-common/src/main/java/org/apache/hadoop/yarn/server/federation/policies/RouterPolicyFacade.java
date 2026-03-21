// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * RouterPolicyFacade是YARN联邦路由策略子系统的外观类，负责管理所有路由策略的生命周期，
 * 包括从远程状态存储加载配置、处理策略刷新、提供默认降级策略等功能，对外统一暴露路由查询接口。
 */
public class RouterPolicyFacade {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterPolicyFacade.class);

  private final SubClusterResolver subClusterResolver;
  private final FederationStateStoreFacade federationFacade;
  private Map<String, SubClusterPolicyConfiguration> globalConfMap;

  @VisibleForTesting
  Map<String, FederationRouterPolicy> globalPolicyMap;

  /**
   * 构造RouterPolicyFacade实例，初始化默认降级路由策略。
   * 优先从联邦状态存储加载默认策略，加载失败则从本地XML配置创建默认策略，最后将默认策略缓存。
   * 
   * @param conf Hadoop配置对象
   * @param facade 联邦状态存储外观对象，用于查询策略配置
   * @param resolver 子集群解析器
   * @param homeSubcluster 当前Router所在的 home 子集群ID
   * @throws FederationPolicyInitializationException 当默认策略初始化失败时抛出
   */
  public RouterPolicyFacade(Configuration conf,
      FederationStateStoreFacade facade, SubClusterResolver resolver,
      SubClusterId homeSubcluster)
      throws FederationPolicyInitializationException {

    this.federationFacade = facade;
    this.subClusterResolver = resolver;
    this.globalConfMap = new ConcurrentHashMap<>();
    this.globalPolicyMap = new ConcurrentHashMap<>();

    // 尝试从联邦状态存储加载默认策略配置
    String defaultKey = YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY;
    SubClusterPolicyConfiguration configuration = null;
    try {
      configuration = federationFacade.getPolicyConfiguration(defaultKey);
    } catch (YarnException e) {
      LOG.warn("No fallback behavior defined in store, defaulting to XML "
          + "configuration fallback behavior.");
    }

    // 状态存储无默认配置，从本地XML配置读取并构造默认策略配置
    if (configuration == null) {
      String defaultFederationPolicyManager =
          conf.get(YarnConfiguration.FEDERATION_POLICY_MANAGER,
              YarnConfiguration.DEFAULT_FEDERATION_POLICY_MANAGER);
      String defaultPolicyParamString =
          conf.get(YarnConfiguration.FEDERATION_POLICY_MANAGER_PARAMS,
              YarnConfiguration.DEFAULT_FEDERATION_POLICY_MANAGER_PARAMS);
      ByteBuffer defaultPolicyParam = ByteBuffer
          .wrap(defaultPolicyParamString.getBytes(StandardCharsets.UTF_8));

      configuration = SubClusterPolicyConfiguration.newInstance(defaultKey,
          defaultFederationPolicyManager, defaultPolicyParam);
    }

    // 实例化策略管理器并初始化
    FederationPolicyInitializationContext fallbackContext =
        new FederationPolicyInitializationContext(configuration,
            subClusterResolver, federationFacade, homeSubcluster);
    FederationPolicyManager fallbackPolicyManager =
        FederationPolicyUtils.instantiatePolicyManager(configuration.getType());
    fallbackPolicyManager.setQueue(defaultKey);

    // 将默认策略加入缓存，作为全局降级策略
    globalConfMap.put(defaultKey,
        fallbackContext.getSubClusterPolicyConfiguration());
    globalPolicyMap.put(defaultKey,
        fallbackPolicyManager.getRouterPolicy(fallbackContext, null));

  }

  /**
   * 根据应用提交上下文和黑名单，为应用选择目标执行子集群。
   * 内部会自动处理配置变更，按需重新初始化策略。
   *
   * @param appSubmissionContext 应用提交上下文，包含队列等信息
   * @param blackListSubClusters 需要排除的黑名单子集群列表
   * @return 选出来作为应用"home"的目标子集群ID
   * @throws YarnException 策略初始化失败或找不到有效子集群时抛出
   */
  public SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext,
      List<SubClusterId> blackListSubClusters) throws YarnException {

    // 局部引用缓存，避免reset()重分配全局map导致的并发问题
    Map<String, SubClusterPolicyConfiguration> cachedConfs = globalConfMap;
    Map<String, FederationRouterPolicy> policyMap = globalPolicyMap;

    if (appSubmissionContext == null) {
      throw new FederationPolicyException(
          "The ApplicationSubmissionContext cannot be null.");
    }

    String queue = appSubmissionContext.getQueue();

    // 队列未指定时使用默认队列，保证null也能命中默认策略
    if (queue == null) {
      queue = YarnConfiguration.DEFAULT_QUEUE_NAME;
    }

    FederationRouterPolicy policy = getFederationRouterPolicy(cachedConfs, policyMap, queue);
    if (policy == null) {
      // 正常不会发生，缓存的默认策略总会存在
      throw new FederationPolicyException("No FederationRouterPolicy found "
          + "for queue: " + appSubmissionContext.getQueue() + " (for "
          + "application: " + appSubmissionContext.getApplicationId() + ") "
          + "and no default specified.");
    }

    return policy.getHomeSubcluster(appSubmissionContext, blackListSubClusters);
  }

  /**
   * 重新初始化指定队列的路由策略，加载最新配置并更新到缓存。
   *
   * @param policyMap 路由策略缓存map
   * @param cachedConfs 策略配置缓存map
   * @param queue 目标队列名称
   * @param conf 最新的策略配置
   * @throws FederationPolicyInitializationException 策略初始化失败时抛出
   */
  private void singlePolicyReinit(Map<String, FederationRouterPolicy> policyMap,
      Map<String, SubClusterPolicyConfiguration> cachedConfs, String queue,
      SubClusterPolicyConfiguration conf)
      throws FederationPolicyInitializationException {

    FederationPolicyInitializationContext context =
        new FederationPolicyInitializationContext(conf, subClusterResolver,
            federationFacade, null);
    String newType = context.getSubClusterPolicyConfiguration().getType();
    FederationRouterPolicy routerPolicy = policyMap.get(queue);

    // 实例化策略管理器，获取最新路由策略实例
    FederationPolicyManager federationPolicyManager =
        FederationPolicyUtils.instantiatePolicyManager(newType);
    federationPolicyManager.setQueue(queue);
    routerPolicy =
        federationPolicyManager.getRouterPolicy(context, routerPolicy);

    // 保证配置和策略的更新原子性
    synchronized (this) {
      policyMap.put(queue, routerPolicy);
      cachedConfs.put(queue, conf);
    }
  }

  /**
   * 清空所有缓存的策略和配置，只保留默认降级策略。
   * 当系统出现大量队列 churn 时调用，清理过期无用缓存。
   * 该方法是线程安全的，会同步更新全局缓存。
   */
  public synchronized void reset() {

    // 保留默认降级策略
    SubClusterPolicyConfiguration conf =
        globalConfMap.get(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY);
    FederationRouterPolicy policy =
        globalPolicyMap.get(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY);

    // 创建新的空缓存
    globalConfMap = new ConcurrentHashMap<>();
    globalPolicyMap = new ConcurrentHashMap<>();

    // 重新添加默认降级策略到新缓存
    globalConfMap.put(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY, conf);
    globalPolicyMap.put(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY,
        policy);

  }

  /**
   * 为资源预留请求选择目标执行子集群。
   * 内部自动处理配置变更，按需重新初始化策略。
   *
   * @param request 资源预留提交请求，包含队列信息
   * @return 选出来作为预留"home"的目标子集群ID
   * @throws YarnException 策略初始化失败或找不到有效子集群时抛出
   */
  public SubClusterId getReservationHomeSubCluster(
      ReservationSubmissionRequest request) throws YarnException {

    // 局部引用缓存，避免reset()重分配全局map导致的并发问题
    Map<String, SubClusterPolicyConfiguration> cachedConfs = globalConfMap;
    Map<String, FederationRouterPolicy> policyMap = globalPolicyMap;

    if (request == null) {
      throw new FederationPolicyException(
          "The ReservationSubmissionRequest cannot be null.");
    }

    String queue = request.getQueue();
    FederationRouterPolicy policy = getFederationRouterPolicy(cachedConfs, policyMap, queue);

    if (policy == null) {
      // 正常不会发生，缓存的默认策略总会存在
      throw new FederationPolicyException("No FederationRouterPolicy found "
          + "for queue: " + request.getQueue() + " (while routing "
          + "reservation: " + request.getReservationId() + ") "
          + "and no default specified.");
    }

    return policy.getReservationHomeSubcluster(request);
  }

  /**
   * 获取指定队列对应的路由策略实例，自动处理配置加载、降级和重新初始化。
   * 流程：先尝试从状态存储加载队列配置 -> 加载失败回退到默认策略 ->
   * 配置发生变化则重新初始化策略 -> 返回最终策略实例。
   *
   * @param cachedConfiguration 缓存的策略配置map
   * @param policyMap 缓存的路由策略map
   * @param queue 目标队列名称
   * @return 对应队列的路由策略实例
   * @throws FederationPolicyInitializationException 策略重新初始化失败时抛出
   */
  private FederationRouterPolicy getFederationRouterPolicy(
      Map<String, SubClusterPolicyConfiguration> cachedConfiguration,
      Map<String, FederationRouterPolicy> policyMap, String queue)
      throws FederationPolicyInitializationException {

    SubClusterPolicyConfiguration configuration = null;
    String copyQueue = queue;

    // 尝试从联邦状态存储查询当前队列的策略配置
    try {
      configuration = federationFacade.getPolicyConfiguration(copyQueue);
    } catch (YarnException e) {
      LOG.warn("There is no policy configured for the queue: {}, falling back to defaults.",
          copyQueue, e);
    }

    // 当前队列无配置，回退到全局默认策略
    if (configuration == null) {
      final String policyKey = YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY;
      LOG.warn("There is no policies configured for queue: {} " +
          "we fallback to default policy for: {}. ", copyQueue, policyKey);
      copyQueue = YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY;
      try {
        configuration = federationFacade.getPolicyConfiguration(copyQueue);
      } catch (YarnException e) {
        LOG.warn("Cannot retrieve policy configured for the queue: {}, falling back to defaults.",
            copyQueue, e);
      }
    }

    // 默认策略在状态存储也不存在，使用本地缓存的XML默认配置
    if (configuration == null) {
      configuration = cachedConfiguration.get(YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY);
    }

    // 对比缓存配置，如果配置变化则重新初始化策略
    SubClusterPolicyConfiguration policyConfiguration =
        cachedConfiguration.getOrDefault(copyQueue, null);
    if (policyConfiguration == null || !policyConfiguration.equals(configuration)) {
      singlePolicyReinit(policyMap, cachedConfiguration, copyQueue, configuration);
    }

    return policyMap.get(copyQueue);
  }
}