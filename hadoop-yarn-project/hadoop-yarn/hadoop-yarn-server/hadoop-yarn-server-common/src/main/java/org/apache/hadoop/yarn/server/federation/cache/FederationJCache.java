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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.expiry.ExpiryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

/**
 * 基于Ehcache实现的YARN联邦缓存，缓存联邦状态存储中的高频查询数据，降低状态存储访问压力。
 * 继承FederationCache抽象类，提供基于JCache规范的缓存实现。
 */
public class FederationJCache extends FederationCache {

  private static final Logger LOG = LoggerFactory.getLogger(FederationJCache.class);

  // Ehcache缓存实例，存储键值对形式的缓存请求
  private Cache<String, CacheRequest> cache;

  // 缓存条目存活时间（秒）
  private int cacheTimeToLive;
  // 缓存最大条目数量
  private long cacheEntityNums;

  // 缓存是否启用标识
  private boolean isCachingEnabled = false;

  // 当前类名，用于缓存命名
  private final String className = this.getClass().getSimpleName();

  @Override
  public boolean isCachingEnabled() {
    return isCachingEnabled;
  }

  @Override
  public void initCache(Configuration pConf, FederationStateStore pStateStore) {
    // Picking the JCache provider from classpath, need to make sure there's
    // no conflict or pick up a specific one in the future
    // 从配置读取缓存TTL，使用默认值兜底
    cacheTimeToLive = pConf.getInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS,
        YarnConfiguration.DEFAULT_FEDERATION_CACHE_TIME_TO_LIVE_SECS);
    // 从配置读取缓存最大条目数，使用默认值兜底
    cacheEntityNums = pConf.getLong(YarnConfiguration.FEDERATION_CACHE_ENTITY_NUMS,
        YarnConfiguration.DEFAULT_FEDERATION_CACHE_ENTITY_NUMS);
    // TTL小于等于0时禁用缓存
    if (cacheTimeToLive <= 0) {
      isCachingEnabled = false;
      return;
    }
    // 保存联邦状态存储引用
    this.setStateStore(pStateStore);
    // 创建Ehcache缓存管理器
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);

    if (this.cache == null) {
      LOG.info("Creating a JCache Manager with name {}. " +
          "Cache TTL Time = {} secs. Cache Entity Nums = {}.", className, cacheTimeToLive,
          cacheEntityNums);
      // 配置堆内存缓存容量
      ResourcePoolsBuilder poolsBuilder = ResourcePoolsBuilder.heap(cacheEntityNums);
      // 配置基于TTL的过期策略
      ExpiryPolicy expiryPolicy = ExpiryPolicyBuilder.timeToLiveExpiration(
          Duration.ofSeconds(cacheTimeToLive));
      // 构建缓存配置
      CacheConfigurationBuilder<String, CacheRequest> configurationBuilder =
          CacheConfigurationBuilder.newCacheConfigurationBuilder(
          String.class, CacheRequest.class, poolsBuilder)
          .withExpiry(expiryPolicy);
      // 创建缓存实例
      cache = cacheManager.createCache(className, configurationBuilder);
    }
    // 标记缓存已启用
    isCachingEnabled = true;
  }

  @Override
  public void clearCache() {
    if (this.cache != null) {
      this.cache.clear();
    }
    this.cache = null;
  }

  @Override
  public Map<SubClusterId, SubClusterInfo> getSubClusters(boolean filterInactiveSubClusters)
      throws YarnException {
    // 构建缓存键
    final String cacheKey = buildCacheKey(className, GET_SUBCLUSTERS_CACHEID,
        Boolean.toString(filterInactiveSubClusters));
    // 从缓存查询
    CacheRequest<String, ?> cacheRequest = cache.get(cacheKey);
    // 缓存未命中，构建缓存请求并写入缓存
    if (cacheRequest == null) {
      cacheRequest = buildGetSubClustersCacheRequest(className, filterInactiveSubClusters);
      cache.put(cacheKey, cacheRequest);
    }
    // 返回缓存结果
    return buildSubClusterInfoMap(cacheRequest);
  }

  @Override
  public Map<String, SubClusterPolicyConfiguration> getPoliciesConfigurations()
      throws Exception {
    // 构建缓存键
    final String cacheKey = buildCacheKey(className, GET_POLICIES_CONFIGURATIONS_CACHEID);
    // 从缓存查询
    CacheRequest<String, ?> cacheRequest = cache.get(cacheKey);
    // 缓存未命中，构建缓存请求并写入缓存
    if(cacheRequest == null){
      cacheRequest = buildGetPoliciesConfigurationsCacheRequest(className);
      cache.put(cacheKey, cacheRequest);
    }
    // 返回缓存结果
    return buildPolicyConfigMap(cacheRequest);
  }

  @Override
  public SubClusterId getApplicationHomeSubCluster(ApplicationId appId)
      throws Exception {
    // 构建缓存键
    final String cacheKey = buildCacheKey(className, GET_APPLICATION_HOME_SUBCLUSTER_CACHEID,
        appId.toString());
    // 从缓存查询
    CacheRequest<String, ?> cacheRequest = cache.get(cacheKey);
    // 缓存未命中，构建缓存请求并写入缓存
    if (cacheRequest == null) {
      cacheRequest = buildGetApplicationHomeSubClusterRequest(className, appId);
      cache.put(cacheKey, cacheRequest);
    }
    // 类型转换并返回结果
    CacheResponse<SubClusterId> response =
         ApplicationHomeSubClusterCacheResponse.class.cast(cacheRequest.getValue());
    return response.getItem();
  }

  @Override
  public void removeSubCluster(boolean flushCache) {
    // 构建缓存键并移除对应缓存条目
    final String cacheKey = buildCacheKey(className, GET_SUBCLUSTERS_CACHEID,
        Boolean.toString(flushCache));
    cache.remove(cacheKey);
  }

  @VisibleForTesting
  public Cache<String, CacheRequest> getCache() {
    return cache;
  }

  @VisibleForTesting
  public String getAppHomeSubClusterCacheKey(ApplicationId appId) {
    return buildCacheKey(className, GET_APPLICATION_HOME_SUBCLUSTER_CACHEID,
        appId.toString());
  }
}