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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN联邦路由策略工具类，提供策略初始化、加载、可用性检查、加权随机选择等通用能力。
 */
@Private
public final class FederationPolicyUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationPolicyUtils.class);

  /** 无可用活跃子集群错误提示常量 */
  public static final String NO_ACTIVE_SUBCLUSTER_AVAILABLE =
      "No active SubCluster available to submit the request.";

  private static Random rand = new Random(System.currentTimeMillis());

  /** 工具类不允许实例化，私有构造方法 */
  private FederationPolicyUtils() {
  }

  /**
   * 根据类名实例化策略管理器对象。
   *
   * @param newType 策略管理器的全类名
   * @return 实例化后的策略管理器对象
   * @throws FederationPolicyInitializationException 反射实例化失败时抛出
   */
  public static FederationPolicyManager instantiatePolicyManager(String newType)
      throws FederationPolicyInitializationException {
    FederationPolicyManager federationPolicyManager = null;
    try {
      Class<?> c = Class.forName(newType);
      federationPolicyManager = (FederationPolicyManager) c.newInstance();
    } catch (ClassNotFoundException e) {
      throw new FederationPolicyInitializationException(e);
    } catch (InstantiationException e) {
      throw new FederationPolicyInitializationException(e);
    } catch (IllegalAccessException e) {
      throw new FederationPolicyInitializationException(e);
    }
    return federationPolicyManager;
  }

  /**
   * 从联邦状态存储加载指定队列的策略配置，按优先级回退到默认配置和本地配置。
   *
   * @param queue 应用所属队列名称
   * @param conf YARN配置对象
   * @param federationFacade 联邦状态存储门面，用于访问状态存储
   * @return 加载完成的子集群策略配置
   */
  public static SubClusterPolicyConfiguration loadPolicyConfiguration(
      String queue, Configuration conf,
      FederationStateStoreFacade federationFacade) {

    // 先尝试从状态存储获取当前队列的策略配置，状态存储可能缓存结果
    SubClusterPolicyConfiguration configuration = null;
    if (queue != null) {
      try {
        configuration = federationFacade.getPolicyConfiguration(queue);
      } catch (YarnException e) {
        LOG.warn("Failed to get policy from FederationFacade with queue "
            + queue + ": " + e.getMessage());
      }
    }

    // 当前队列无配置，回退使用默认队列配置
    if (configuration == null) {
      LOG.info("No policy configured for queue {} in StateStore,"
          + " fallback to default queue", queue);
      queue = YarnConfiguration.DEFAULT_FEDERATION_POLICY_KEY;
      try {
        configuration = federationFacade.getPolicyConfiguration(queue);
      } catch (YarnException e) {
        LOG.warn("No fallback behavior defined in store, defaulting to XML "
            + "configuration fallback behavior.");
      }
    }

    // 默认队列也无配置，回退使用本地XML配置生成默认策略
    if (configuration == null) {
      LOG.info("No policy configured for default queue {} in StateStore,"
          + " fallback to local config", queue);

      String defaultFederationPolicyManager =
          conf.get(YarnConfiguration.FEDERATION_POLICY_MANAGER,
              YarnConfiguration.DEFAULT_FEDERATION_POLICY_MANAGER);
      String defaultPolicyParamString =
          conf.get(YarnConfiguration.FEDERATION_POLICY_MANAGER_PARAMS,
              YarnConfiguration.DEFAULT_FEDERATION_POLICY_MANAGER_PARAMS);
      ByteBuffer defaultPolicyParam = ByteBuffer
          .wrap(defaultPolicyParamString.getBytes(StandardCharsets.UTF_8));

      configuration = SubClusterPolicyConfiguration.newInstance(queue,
          defaultFederationPolicyManager, defaultPolicyParam);
    }
    return configuration;
  }

  /**
   * 加载并初始化AMRMProxy路由策略，支持热更新已有策略实例。
   *
   * @param queue 应用所属队列名称
   * @param oldPolicy 旧的策略实例，用于增量初始化，可为null
   * @param conf YARN配置对象
   * @param federationFacade 联邦状态存储门面
   * @param homeSubClusterId 当前集群Home子集群ID
   * @return 初始化完成的AMRMProxy策略对象
   * @throws FederationPolicyInitializationException 初始化失败时抛出
   */
  public static FederationAMRMProxyPolicy loadAMRMPolicy(String queue,
      FederationAMRMProxyPolicy oldPolicy, Configuration conf,
      FederationStateStoreFacade federationFacade,
      SubClusterId homeSubClusterId)
      throws FederationPolicyInitializationException {

    // 加载策略配置
    SubClusterPolicyConfiguration configuration =
        loadPolicyConfiguration(queue, conf, federationFacade);

    // 构造策略初始化上下文，封装所需依赖
    FederationPolicyInitializationContext context =
        new FederationPolicyInitializationContext(configuration,
            federationFacade.getSubClusterResolver(), federationFacade,
            homeSubClusterId);

    LOG.info("Creating policy manager of type: " + configuration.getType());
    // 实例化策略管理器
    FederationPolicyManager federationPolicyManager =
        instantiatePolicyManager(configuration.getType());
    // 设置对应队列名称
    federationPolicyManager.setQueue(configuration.getQueue());
    // 从策略管理器获取初始化完成的AMRMProxy策略
    return federationPolicyManager.getAMRMPolicy(context, oldPolicy);
  }

  /**
   * 检查是否存在未被拉黑的活跃子集群，无可用子集群则抛出异常。
   *
   * @param activeSubClusters 当前活跃子集群集合
   * @param blackListSubClusters 拉黑的子集群集合
   * @throws FederationPolicyException 无可用子集群时抛出
   */
  public static void validateSubClusterAvailability(
      Collection<SubClusterId> activeSubClusters,
      Collection<SubClusterId> blackListSubClusters)
      throws FederationPolicyException {
    if (activeSubClusters != null && !activeSubClusters.isEmpty()) {
      if (blackListSubClusters == null) {
        return;
      }
      for (SubClusterId scId : activeSubClusters) {
        if (!blackListSubClusters.contains(scId)) {
          // 至少存在一个可用活跃子集群，直接返回
          return;
        }
      }
    }
    throw new FederationPolicyException(
        FederationPolicyUtils.NO_ACTIVE_SUBCLUSTER_AVAILABLE);
  }

  /**
   * 根据权重数组执行加权随机选择，仅选择权重为正的选项，无正权重返回-1。
   *
   * @param weights 权重数组，每个元素对应一个选项的权重
   * @return 选中选项在数组中的索引，无可用选项返回-1
   */
  public static int getWeightedRandom(ArrayList<Float> weights) {
    int i;
    float totalWeight = 0;
    // 计算所有正权重的总权重
    for (i = 0; i < weights.size(); i++) {
      if (weights.get(i) > 0) {
        totalWeight += weights.get(i);
      }
    }
    // 没有正权重直接返回-1
    if (totalWeight == 0) {
      return -1;
    }
    // 在总权重范围内生成随机采样点
    float samplePoint = rand.nextFloat() * totalWeight;
    int lastIndex = 0;
    // 遍历权重查找采样点命中的区间
    for (i = 0; i < weights.size(); i++) {
      if (weights.get(i) > 0) {
        if (samplePoint <= weights.get(i)) {
          return i;
        } else {
          lastIndex = i;
          samplePoint -= weights.get(i);
        }
      }
    }
    // 浮点精度舍入误差处理：采样点非常接近总权重时返回最后一个正权重索引
    return lastIndex;
  }

  /**
   * 供单元测试设置随机数种子，固定随机结果。
   * @param seed 随机数种子
   */
  @VisibleForTesting
  public static void setRand(long seed){
    rand.setSeed(seed);
  }
}