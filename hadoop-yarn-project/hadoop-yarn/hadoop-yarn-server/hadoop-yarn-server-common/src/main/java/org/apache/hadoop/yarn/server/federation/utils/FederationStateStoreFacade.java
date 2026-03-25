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

package org.apache.hadoop.yarn.server.federation.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.util.Collection;

import javax.cache.integration.CacheLoaderException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.cache.FederationCache;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreRetriableException;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;

import static org.apache.hadoop.yarn.server.federation.cache.FederationCache.buildPolicyConfigMap;
import static org.apache.hadoop.yarn.server.federation.cache.FederationCache.buildSubClusterInfoMap;

/**
 * YARN联邦状态存储门面类，提供联邦状态存储的单例访问入口，封装重试逻辑和缓存能力，
 * 简化上层模块对联邦状态存储的访问。
 *
 * 核心能力：提供对联邦元数据（子集群信息、应用归属、调度策略、令牌密钥等）的缓存和重试访问
 */
public final class FederationStateStoreFacade {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreFacade.class);

  // 单例实例，volatile保证双重检查锁定可见性
  private static volatile FederationStateStoreFacade facade;

  // 随机数生成器，用于随机选择子集群
  private static Random rand = new Random(System.currentTimeMillis());

  private FederationStateStore stateStore;
  private Configuration conf;
  private SubClusterResolver subclusterResolver;
  private FederationCache federationCache;

  private FederationStateStoreFacade(Configuration conf) {
    initializeFacadeInternal(conf);
  }

  // 初始化门面内部组件
  private void initializeFacadeInternal(Configuration config) {
    this.conf = config;
    try {
      // 创建带重试代理的联邦状态存储客户端实例
      this.stateStore = (FederationStateStore) createRetryInstance(this.conf,
          YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS,
          YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLIENT_CLASS,
          FederationStateStore.class, createRetryPolicy(conf));
      this.stateStore.init(conf);

      // 创建子集群解析器实例
      this.subclusterResolver = createInstance(conf,
          YarnConfiguration.FEDERATION_CLUSTER_RESOLVER_CLASS,
          YarnConfiguration.DEFAULT_FEDERATION_CLUSTER_RESOLVER_CLASS,
          SubClusterResolver.class);
      this.subclusterResolver.load();

      // 创建联邦缓存实例，如果配置未指定则使用默认实现
      this.federationCache = createInstance(conf,
          YarnConfiguration.FEDERATION_FACADE_CACHE_CLASS,
          YarnConfiguration.DEFAULT_FEDERATION_FACADE_CACHE_CLASS,
          FederationCache.class);
      this.federationCache.initCache(config, stateStore);

    } catch (YarnException ex) {
      LOG.error("Failed to initialize the FederationStateStoreFacade object", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * 删除并重新初始化缓存，使用传入配置强制刷新。
   * 仅用于测试。
   *
   * @param store 用于重新初始化的 {@link FederationStateStore} 实例
   * @param config 更新后的配置
   */
  @VisibleForTesting
  public synchronized void reinitialize(FederationStateStore store,
      Configuration config) {
    this.conf = config;
    this.stateStore = store;
    federationCache.clearCache();
    federationCache.initCache(config, stateStore);
  }

  /**
   * 创建 {@code FederationStateStoreFacade} 的重试策略。
   * 仅对可重试异常进行重试，包括：
   * <ul>
   * <li>{@code FederationStateStoreRetriableException}</li>
   * <li>{@code CacheLoaderException}</li>
   * <li>{@code PoolInitializationException}</li>
   * </ul>
   *
   * @param conf Hadoop配置
   * @return 联邦状态存储门面的重试策略
   */
  public static RetryPolicy createRetryPolicy(Configuration conf) {
    // 读取StateStore重试配置，使用指数退避策略
    RetryPolicy basePolicy = RetryPolicies.exponentialBackoffRetry(
        conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES, Integer.SIZE),
        conf.getLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS,
            YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS),
        TimeUnit.MILLISECONDS);
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<>();
    // 为可重试异常绑定指数退避策略
    exceptionToPolicyMap.put(FederationStateStoreRetriableException.class,
        basePolicy);
    exceptionToPolicyMap.put(CacheLoaderException.class, basePolicy);
    exceptionToPolicyMap.put(PoolInitializationException.class, basePolicy);

    // 根据异常类型选择重试策略，非可重试异常仅尝试一次
    RetryPolicy retryPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    return retryPolicy;
  }

  /**
   * 获取 FederationStateStoreFacade 单例实例（使用默认配置）。
   *
   * @return FederationStateStoreFacade 单例实例
   */
  public static FederationStateStoreFacade getInstance() {
    return getInstanceInternal(new Configuration());
  }

  /**
   * 获取 FederationStateStoreFacade 单例实例（使用指定配置）。
   *
   * @param conf 配置
   * @return FederationStateStoreFacade 单例实例
   */
  public static FederationStateStoreFacade getInstance(Configuration conf) {
    return getInstanceInternal(conf);
  }

  /**
   * 获取 FederationStateStoreFacade 单例实例内部方法。
   *
   * @param conf 配置
   * @return FederationStateStoreFacade 单例实例
   */
  private static FederationStateStoreFacade getInstanceInternal(Configuration conf){
    if (facade != null) {
      return facade;
    }
    generateStateStoreFacade(conf);
    return facade;
  }

  /**
   * 生成 FederationStateStoreFacade 单例实例（双重检查锁定实现线程安全）。
   *
   * @param conf 配置
   */
  private static void generateStateStoreFacade(Configuration conf){
    if (facade == null) {
      synchronized (FederationStateStoreFacade.class) {
        if (facade == null) {
          Configuration yarnConf = new Configuration();
          if (conf != null) {
            yarnConf = conf;
          }
          facade = new FederationStateStoreFacade(yarnConf);
        }
      }
    }
  }

  /**
   * 根据子集群ID获取子集群信息。
   *
   * @param subClusterId 子集群ID
   * @return 子集群信息，如果不存在则返回 {@code null}
   * @throws YarnException 访问状态存储失败时抛出
   */
  public SubClusterInfo getSubCluster(final SubClusterId subClusterId)
      throws YarnException {
    if (federationCache.isCachingEnabled()) {
      return getSubClusters(false).get(subClusterId);
    } else {
      GetSubClusterInfoResponse response = stateStore
          .getSubCluster(GetSubClusterInfoRequest.newInstance(subClusterId));
      if (response == null) {
        return null;
      } else {
        return response.getSubClusterInfo();
      }
    }
  }

  /**
   * 根据子集群ID获取子集群信息，支持强制刷新缓存。
   *
   * @param subClusterId 子集群ID
   * @param flushCache 是否需要刷新缓存标记
   * @return 子集群信息
   * @throws YarnException 访问状态存储失败时抛出
   */
  public SubClusterInfo getSubCluster(final SubClusterId subClusterId,
      final boolean flushCache) throws YarnException {
    if (flushCache && federationCache.isCachingEnabled()) {
      LOG.info("Flushing subClusters from cache and rehydrating from store,"
          + " most likely on account of RM failover.");
      federationCache.removeSubCluster(false);
    }
    return getSubCluster(subClusterId);
  }

  /**
   * 获取所有活跃子集群的信息。
   *
   * @param filterInactiveSubClusters 是否过滤掉不活跃子集群
   * @return 所有活跃子集群的信息，键为子集群ID，值为子集群信息
   * @throws YarnException 访问状态存储失败时抛出
   */
  public Map<SubClusterId, SubClusterInfo> getSubClusters(final boolean filterInactiveSubClusters)
      throws YarnException {
    try {
      if (federationCache.isCachingEnabled()) {
        return federationCache.getSubClusters(filterInactiveSubClusters);
      } else {
        GetSubClustersInfoRequest request =
            GetSubClustersInfoRequest.newInstance(filterInactiveSubClusters);
        return buildSubClusterInfoMap(stateStore.getSubClusters(request));
      }
    } catch (Throwable ex) {
      throw new YarnException(ex);
    }
  }

  /**
   * 获取所有活跃子集群的信息，支持强制刷新缓存。
   *
   * @param filterInactiveSubClusters 是否过滤掉不活跃子集群
   * @param flushCache 是否需要刷新缓存标记
   * @return 所有活跃子集群的信息，键为子集群ID，值为子集群信息
   * @throws YarnException 访问状态存储失败时抛出
   */
  public Map<SubClusterId, SubClusterInfo> getSubClusters(
      final boolean filterInactiveSubClusters, final boolean flushCache)
      throws YarnException {
    if (flushCache && federationCache.isCachingEnabled()) {
      LOG.info("Flushing subClusters from cache and rehydrating from store.");
      federationCache.removeSubCluster(flushCache);
    }
    return getSubClusters(filterInactiveSubClusters);
  }

  /**
   * 根据队列获取联邦调度策略配置。
   *
   * @param queue 需要查询策略的队列名称
   * @return 对应队列的调度策略配置，如果不存在则返回 {@code null}
   * @throws YarnException 访问状态存储失败时抛出
   */
  public SubClusterPolicyConfiguration getPolicyConfiguration(final String queue)
      throws YarnException {
    if (federationCache.isCachingEnabled()) {
      return getPoliciesConfigurations().get(queue);
    } else {
      GetSubClusterPolicyConfigurationRequest request =
          GetSubClusterPolicyConfigurationRequest.newInstance(queue);
      GetSubClusterPolicyConfigurationResponse response =
          stateStore.getPolicyConfiguration(request);
      if (response == null) {
        return null;
      } else {
        return response.getPolicyConfiguration();
      }
    }
  }

  /**
   * 将队列调度策略配置写入状态存储。
   *
   * @param policyConf 要写入的策略配置
   * @throws YarnException 请求无效或访问失败时抛出
   */
  public void setPolicyConfiguration(SubClusterPolicyConfiguration policyConf)
      throws YarnException {
    stateStore.setPolicyConfiguration(
        SetSubClusterPolicyConfigurationRequest.newInstance(policyConf));
  }

  /**
   * 获取所有当前活跃队列的调度策略配置。
   *
   * @return 所有队列的调度策略配置，键为队列名称，值为策略配置
   * @throws YarnException 访问状态存储失败时抛出
   */
  public Map<String, SubClusterPolicyConfiguration> getPoliciesConfigurations()
      throws YarnException {
    try {
      if (federationCache.isCachingEnabled()) {
        return federationCache.getPoliciesConfigurations();
      } else {
        GetSubClusterPoliciesConfigurationsRequest request =
            GetSubClusterPoliciesConfigurationsRequest.newInstance();
        return buildPolicyConfigMap(stateStore.getPoliciesConfigurations(request));
      }
    } catch (Throwable ex) {
      throw new YarnException(ex);
    }