// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 基于Caffeine实现的YARN联邦缓存，Caffeine是高性能Java缓存库，性能优于Ehcache和Guava Cache。
 * 该缓存用于存储联邦集群中的应用信息、Home子集群信息等热点数据，降低对联邦状态存储的访问压力。
 */
public class FederationCaffeineCache extends FederationCache {

  private static final Logger LOG = LoggerFactory.getLogger(FederationCaffeineCache.class);

  // Caffeine缓存实例，存储缓存键与缓存请求对象
  private Cache<String, CacheRequest> cache;

  // 缓存条目存活时间（秒）
  private int cacheTimeToLive;
  // 缓存最大可存储条目数量
  private long cacheEntityNums;

  // 当前类简单名称，用于构建缓存键
  private String className = this.getClass().getSimpleName();

  // 缓存是否启用标记
  private boolean isCachingEnabled = false;

  @Override
  /**
   * 获取缓存是否启用的状态
   * @return true表示缓存已启用，false表示未启用
   */
  public boolean isCachingEnabled() {
    return isCachingEnabled;
  }

  @Override
  /**
   * 初始化Caffeine缓存，从配置中读取缓存参数并构建缓存实例
   * @param pConf YARN配置对象
   * @param pStateStore 联邦状态存储，缓存未命中时从这里加载数据
   */
  public void initCache(Configuration pConf, FederationStateStore pStateStore) {
    // 从配置读取缓存TTL，使用默认值兜底
    cacheTimeToLive = pConf.getInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS,
        YarnConfiguration.DEFAULT_FEDERATION_CACHE_TIME_TO_LIVE_SECS);
    // 从配置读取缓存最大条目数，使用默认值兜底
    cacheEntityNums = pConf.getLong(YarnConfiguration.FEDERATION_CACHE_ENTITY_NUMS,
        YarnConfiguration.DEFAULT_FEDERATION_CACHE_ENTITY_NUMS);
    // TTL小于等于0时禁用缓存
    if (cacheTimeToLive <= 0) {
      isCachingEnabled = false;
      LOG.warn("Federation cache is not enabled. If we want to enable federation cache, " +
          "we need to set yarn.federation.cache-ttl.secs greater than 0.");
      return;
    }
    this.setStateStore(pStateStore);

    // 初始化Caffeine缓存实例
    LOG.info("Creating a JCache Manager with name {}. " +
        "Cache TTL Time = {} secs. Cache Entity Nums = {}.", className, cacheTimeToLive,
        cacheEntityNums);

    this.cache = Caffeine.newBuilder().maximumSize(cacheEntityNums)
        .expireAfterWrite(cacheTimeToLive, TimeUnit.SECONDS).build();
    // 缓存参数合法，启用缓存
    isCachingEnabled = true;
  }

  @Override
  /**
   * 清空缓存并释放缓存实例
   */
  public void clearCache() {
    if (this.cache != null) {
      this.cache.cleanUp();
    }
    this.cache = null;
  }

  @Override
  /**
   * 获取子集群信息列表，优先从缓存读取，未命中则从状态存储加载并写入缓存
   * @param filterInactiveSubClusters 是否过滤非活跃子集群
   * @return 子集群ID与子集群信息的映射
   * @throws YarnException 从状态存储加载数据失败时抛出异常
   */
  public Map<SubClusterId, SubClusterInfo> getSubClusters(
      boolean filterInactiveSubClusters) throws YarnException {
    final String cacheKey = buildCacheKey(className, GET_SUBCLUSTERS_CACHEID,
       Boolean.toString(filterInactiveSubClusters));
    CacheRequest<String, ?> cacheRequest = cache.getIfPresent(cacheKey);
    if (cacheRequest == null) {
      cacheRequest = buildGetSubClustersCacheRequest(className, filterInactiveSubClusters);
      cache.put(cacheKey, cacheRequest);
    }
    return buildSubClusterInfoMap(cacheRequest);
  }

  @Override
  /**
   * 获取所有子集群的策略配置，优先从缓存读取，未命中则从状态存储加载并写入缓存
   * @return 策略名称与策略配置的映射
   * @throws Exception 从状态存储加载数据失败时抛出异常
   */
  public Map<String, SubClusterPolicyConfiguration> getPoliciesConfigurations()
      throws Exception {
    final String cacheKey = buildCacheKey(className, GET_POLICIES_CONFIGURATIONS_CACHEID);
    CacheRequest<String, ?> cacheRequest = cache.getIfPresent(cacheKey);
    if(cacheRequest == null){
      cacheRequest = buildGetPoliciesConfigurationsCacheRequest(className);
      cache.put(cacheKey, cacheRequest);
    }
    return buildPolicyConfigMap(cacheRequest);
  }

  @Override
  /**
   * 获取应用对应的Home子集群，优先从缓存读取，未命中则从状态存储加载并写入缓存
   * @param appId 应用ID
   * @return 应用对应的Home子集群ID
   * @throws Exception 从状态存储加载数据失败时抛出异常
   */
  public SubClusterId getApplicationHomeSubCluster(ApplicationId appId) throws Exception {
    final String cacheKey = buildCacheKey(className, GET_APPLICATION_HOME_SUBCLUSTER_CACHEID,
        appId.toString());
    CacheRequest<String, ?> cacheRequest = cache.getIfPresent(cacheKey);
    if (cacheRequest == null) {
      cacheRequest = buildGetApplicationHomeSubClusterRequest(className, appId);
      cache.put(cacheKey, cacheRequest);
    }
    CacheResponse<SubClusterId> response =
        ApplicationHomeSubClusterCacheResponse.class.cast(cacheRequest.getValue());
    return response.getItem();
  }

  @Override
  /**
   * 从缓存中移除子集群列表缓存条目，子集群变更时调用
   * @param flushCache 是否需要清空缓存
   */
  public void removeSubCluster(boolean flushCache) {
    final String cacheKey = buildCacheKey(className, GET_SUBCLUSTERS_CACHEID,
        Boolean.toString(flushCache));
    cache.invalidate(cacheKey);
  }
}