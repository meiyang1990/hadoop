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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
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
 * 基于Guava缓存实现的YARN联邦缓存，为联邦状态存储查询结果提供本地缓存加速。
 * 通过缓存子集群信息、应用归属、调度策略等数据，减少对远程联邦状态存储的访问次数，
 * 提升联邦集群查询性能。
 */
public class FederationGuavaCache extends FederationCache {

  private static final Logger LOG = LoggerFactory.getLogger(FederationCache.class);

  // Guava缓存实例，存储缓存键与缓存请求对象
  private Cache<String, CacheRequest<String, ?>> cache;

  // 缓存条目存活时间（秒）
  private int cacheTimeToLive;
  // 缓存最大容纳实体数量
  private long cacheEntityNums;

  // 当前类的简单名称，用于构建缓存键
  private String className = this.getClass().getSimpleName();

  // 缓存是否启用标志
  private boolean isCachingEnabled = false;

  @Override
  public boolean isCachingEnabled() {
    return isCachingEnabled;
  }

  /**
   * 初始化Guava缓存，从配置读取缓存参数并构建缓存实例。
   * @param pConf YARN配置对象
   * @param pStateStore 联邦状态存储引用
   */
  @Override
  public void initCache(Configuration pConf, FederationStateStore pStateStore) {
    // Picking the JCache provider from classpath, need to make sure there's
    // no conflict or pick up a specific one in the future.
    // 从配置读取缓存TTL，使用默认值兜底
    cacheTimeToLive = pConf.getInt(YarnConfiguration.FEDERATION_CACHE_TIME_TO_LIVE_SECS,
        YarnConfiguration.DEFAULT_FEDERATION_CACHE_TIME_TO_LIVE_SECS);
    // 从配置读取缓存最大容量，使用默认值兜底
    cacheEntityNums = pConf.getLong(YarnConfiguration.FEDERATION_CACHE_ENTITY_NUMS,
        YarnConfiguration.DEFAULT_FEDERATION_CACHE_ENTITY_NUMS);
    // TTL小于等于0表示禁用缓存
    if (cacheTimeToLive <= 0) {
      isCachingEnabled = false;
      return;
    }
    // 保存联邦状态存储引用
    this.setStateStore(pStateStore);

    // Initialize Cache.
    LOG.info("Creating a JCache Manager with name {}. " +
        "Cache TTL Time = {} secs. Cache Entity Nums = {}.", className, cacheTimeToLive,
        cacheEntityNums);
    // 构建Guava缓存，配置写入后过期时间和最大容量
    cache = CacheBuilder.newBuilder().expireAfterWrite(cacheTimeToLive,
        TimeUnit.SECONDS).maximumSize(cacheEntityNums).build();
    // 标记缓存已启用
    isCachingEnabled = true;
  }

  @Override
  public void clearCache() {
    if (this.cache != null) {
      // 失效所有缓存条目
      cache.invalidateAll();
    }
    cache = null;
  }

  @Override
  public Map<SubClusterId, SubClusterInfo> getSubClusters(boolean filterInactiveSubClusters)
      throws YarnException {
    // 构建缓存键
    final String cacheKey = buildCacheKey(className, GET_SUBCLUSTERS_CACHEID,
        Boolean.toString(filterInactiveSubClusters));
    // 从缓存查询
    CacheRequest<String, ?> cacheRequest = cache.getIfPresent(cacheKey);
    // 缓存未命中，构建新的缓存请求并放入缓存
    if (cacheRequest == null) {
      cacheRequest = buildGetSubClustersCacheRequest(className, filterInactiveSubClusters);
      cache.put(cacheKey, cacheRequest);
    }
    // 返回缓存查询结果（会自动触发缓存请求执行）
    return buildSubClusterInfoMap(cacheRequest);
  }

  @Override
  public Map<String, SubClusterPolicyConfiguration> getPoliciesConfigurations() throws Exception {
    // 构建缓存键
    final String cacheKey = buildCacheKey(className, GET_POLICIES_CONFIGURATIONS_CACHEID);
    // 从缓存查询
    CacheRequest<String, ?> cacheRequest = cache.getIfPresent(cacheKey);
    // 缓存未命中，构建新的缓存请求并放入缓存
    if(cacheRequest == null){
      cacheRequest = buildGetPoliciesConfigurationsCacheRequest(className);
      cache.put(cacheKey, cacheRequest);
    }
    // 返回缓存查询结果（会自动触发缓存请求执行）
    return buildPolicyConfigMap(cacheRequest);
  }

  @Override
  public SubClusterId getApplicationHomeSubCluster(ApplicationId appId) throws Exception {
    // 构建缓存键，包含应用ID
    final String cacheKey = buildCacheKey(className, GET_APPLICATION_HOME_SUBCLUSTER_CACHEID,
        appId.toString());
    // 从缓存查询
    CacheRequest<String, ?> cacheRequest = cache.getIfPresent(cacheKey);
    // 缓存未命中，构建新的缓存请求并放入缓存
    if (cacheRequest == null) {
      cacheRequest = buildGetApplicationHomeSubClusterRequest(className, appId);
      cache.put(cacheKey, cacheRequest);
    }
    // 类型转换并返回应用归属子集群ID
    CacheResponse<SubClusterId> response =
        ApplicationHomeSubClusterCacheResponse.class.cast(cacheRequest.getValue());
    return response.getItem();
  }

  @Override
  public void removeSubCluster(boolean flushCache) {
    // 构建子集群列表缓存键
    final String cacheKey = buildCacheKey(className, GET_SUBCLUSTERS_CACHEID,
        Boolean.toString(flushCache));
    // 从缓存中失效该条目
    cache.invalidate(cacheKey);
  }
}