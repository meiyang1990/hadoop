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
package org.apache.hadoop.yarn.server.federation.cache;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * YARN联邦缓存抽象基类，为联邦状态存储提供缓存能力，减少对状态存储的重复查询，提升查询性能。
 * 提供了缓存键构建、各类缓存请求构造和结果转换的通用实现，具体缓存实现由子类完成。
 */
public abstract class FederationCache {

  // ------------------------------------ Constants   -------------------------

  // 子集群信息缓存ID
  protected static final String GET_SUBCLUSTERS_CACHEID = "getSubClusters";

  // 子集群策略配置缓存ID
  protected static final String GET_POLICIES_CONFIGURATIONS_CACHEID =
      "getPoliciesConfigurations";
  // 应用归属子集群缓存ID
  protected static final String GET_APPLICATION_HOME_SUBCLUSTER_CACHEID =
      "getApplicationHomeSubCluster";

  // 缓存键分隔符
  protected static final String POINT = ".";

  // 联邦状态存储引用
  private FederationStateStore stateStore;

  /**
   * 判断是否启用缓存，根据缓存过期时间判断，大于0则启用。
   * @return true 启用缓存；false 不启用缓存
   */
  public abstract boolean isCachingEnabled();

  /**
   * 初始化缓存。
   * @param pConf 配置对象
   * @param pStateStore 联邦状态存储
   */
  public abstract void initCache(Configuration pConf, FederationStateStore pStateStore);

  /**
   * 清空所有缓存。
   */
  public abstract void clearCache();

  /**
   * 构建缓存键，格式为 类名.方法名。
   *
   * @param className 缓存类名
   * @param methodName 方法名
   * @return 构建好的缓存键
   */
  protected String buildCacheKey(String className, String methodName) {
    return buildCacheKey(className, methodName, null);
  }

  /**
   * 构建带参数的缓存键，格式为 类名.方法名.参数名。
   *
   * @param className 缓存类名
   * @param methodName 方法名
   * @param argName 参数名
   * @return 构建好的缓存键
   */
  protected String buildCacheKey(String className, String methodName, String argName) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(className).append(POINT).append(methodName);
    if (argName != null) {
      buffer.append(POINT);
      buffer.append(argName);
    }
    return buffer.toString();
  }

  /**
   * 获取所有活跃子集群信息。
   *
   * @param filterInactiveSubClusters 是否过滤非活跃子集群
   * @return 所有活跃子集群信息映射，键为子集群ID，值为子集群信息
   * @throws YarnException 状态存储调用失败时抛出异常
   */
  public abstract Map<SubClusterId, SubClusterInfo> getSubClusters(
      boolean filterInactiveSubClusters) throws YarnException;

  /**
   * 获取所有当前活跃队列的子集群路由策略配置。
   *
   * @return 所有队列的策略配置映射，键为队列名，值为策略配置
   * @throws Exception 状态存储调用失败时抛出异常
   */
  public abstract Map<String, SubClusterPolicyConfiguration> getPoliciesConfigurations()
      throws Exception;

  /**
   * 获取指定应用的归属子集群ID。
   *
   * @param appId 应用ID
   * @return 应用归属子集群ID
   * @throws YarnException 状态存储调用失败时抛出异常
   */
  public abstract SubClusterId getApplicationHomeSubCluster(ApplicationId appId) throws Exception;

  /**
   * 从缓存中移除子集群（根据过滤条件移除非活跃子集群）。
   *
   * @param filterInactiveSubClusters 是否过滤非活跃子集群
   */
  public abstract void removeSubCluster(boolean filterInactiveSubClusters);


  // ------------------------------------ SubClustersCache -------------------------

  /**
   * 构造获取子集群信息的缓存请求，从状态存储加载数据封装为缓存请求。
   *
   * @param cacheKey 缓存键
   * @param filterInactiveSubClusters 是否过滤非活跃子集群
   * @return 封装好的缓存请求
   * @throws YarnException 状态存储调用失败时抛出异常
   */
  protected CacheRequest<String, CacheResponse<SubClusterInfo>> buildGetSubClustersCacheRequest(
      String cacheKey, final boolean filterInactiveSubClusters) throws YarnException {
    CacheResponse<SubClusterInfo> response =
        buildSubClusterInfoResponse(filterInactiveSubClusters);
    CacheRequest<String, CacheResponse<SubClusterInfo>> cacheRequest =
        new CacheRequest<>(cacheKey, response);
    return cacheRequest;
  }

  /**
   * 从状态存储查询子集群信息，封装为缓存响应对象。
   *
   * @param filterInactiveSubClusters 是否过滤非活跃子集群
   * @return 封装好的子集群信息缓存响应
   * @throws YarnException 状态存储调用失败时抛出异常
   */
  private CacheResponse<SubClusterInfo> buildSubClusterInfoResponse(
      final boolean filterInactiveSubClusters) throws YarnException {
    GetSubClustersInfoRequest request = GetSubClustersInfoRequest.newInstance(
        filterInactiveSubClusters);
    GetSubClustersInfoResponse subClusters = stateStore.getSubClusters(request);
    CacheResponse<SubClusterInfo> response = new SubClusterInfoCacheResponse();
    response.setList(subClusters.getSubClusters());
    return response;
  }

  /**
   * 从状态存储响应中构建子集群ID到信息的映射表。
   *
   * @param response 状态存储查询响应
   * @return 子集群ID到信息的映射表
   */
  public static Map<SubClusterId, SubClusterInfo> buildSubClusterInfoMap(
      final GetSubClustersInfoResponse response) {
    List<SubClusterInfo> subClusters = response.getSubClusters();
    return buildSubClusterInfoMap(subClusters);
  }

  /**
   * 从缓存请求中提取并构建子集群ID到信息的映射表。
   *
   * @param cacheRequest 缓存请求对象
   * @return 子集群ID到信息的映射表
   */
  public static Map<SubClusterId, SubClusterInfo> buildSubClusterInfoMap(
      CacheRequest<String, ?> cacheRequest) {
    Object value = cacheRequest.value;
    SubClusterInfoCacheResponse response = SubClusterInfoCacheResponse.class.cast(value);
    List<SubClusterInfo> subClusters = response.getList();
    return buildSubClusterInfoMap(subClusters);
  }

  /**
   * 从子集群列表构建子集群ID到信息的映射表。
   *
   * @param subClusters 子集群信息列表
   * @return 子集群ID到信息的映射表
   */
  private static Map<SubClusterId, SubClusterInfo> buildSubClusterInfoMap(
      List<SubClusterInfo> subClusters) {
    Map<SubClusterId, SubClusterInfo> subClustersMap = new HashMap<>(subClusters.size());
    for (SubClusterInfo subCluster : subClusters) {
      subClustersMap.put(subCluster.getSubClusterId(), subCluster);
    }
    return subClustersMap;
  }

  // ------------------------------------ ApplicationHomeSubClusterCache -------------------------

  /**
   * 构造获取应用归属子集群的缓存请求，从状态存储加载数据封装为缓存请求。
   *
   * @param cacheKey 缓存键
   * @param applicationId 应用ID
   * @return 封装好的缓存请求
   * @throws YarnException 状态存储调用失败时抛出异常
   */
  protected CacheRequest<String, CacheResponse<SubClusterId>>
      buildGetApplicationHomeSubClusterRequest(String cacheKey, ApplicationId applicationId)
      throws YarnException {
    CacheResponse<SubClusterId> response = buildSubClusterIdResponse(applicationId);
    return new CacheRequest<>(cacheKey, response);
  }

  /**
   * 从状态存储查询应用归属子集群，封装为缓存响应对象。
   *
   * @param applicationId 应用ID
   * @return 封装好的应用归属子集群缓存响应
   * @throws YarnException 状态存储调用失败时抛出异常
   */
  private CacheResponse<SubClusterId> buildSubClusterIdResponse(final ApplicationId applicationId)
      throws YarnException {
    GetApplicationHomeSubClusterRequest request =
         GetApplicationHomeSubClusterRequest.newInstance(applicationId);
    GetApplicationHomeSubClusterResponse response =
         stateStore.getApplicationHomeSubCluster(request);
    ApplicationHomeSubCluster appHomeSubCluster = response.getApplicationHomeSubCluster();
    SubClusterId subClusterId = appHomeSubCluster.getHomeSubCluster();
    CacheResponse<SubClusterId> cacheResponse = new ApplicationHomeSubClusterCacheResponse();
    cacheResponse.setItem(subClusterId);
    return cacheResponse;
  }

  // ------------------------------ SubClusterPolicyConfigurationCache -------------------------

  /**
   * 构造获取策略配置的缓存请求，从状态存储加载数据封装为缓存请求。
   *
   * @param cacheKey 缓存键
   * @return 封装好的缓存请求
   * @throws YarnException 状态存储调用失败时抛出异常
   */
  protected CacheRequest<String, CacheResponse<SubClusterPolicyConfiguration>>
      buildGetPoliciesConfigurationsCacheRequest(String cacheKey) throws YarnException {
    CacheResponse<SubClusterPolicyConfiguration> response =
         buildSubClusterPolicyConfigurationResponse();
    return new CacheRequest<>(cacheKey, response);
  }

  /**
   * 从状态存储响应中构建队列到策略配置的映射表。
   *
   * @param response 状态存储查询响应
   * @return 队列名到策略配置的映射表
   */
  public static Map<String, SubClusterPolicyConfiguration> buildPolicyConfigMap(
      GetSubClusterPoliciesConfigurationsResponse response) {
    List<SubClusterPolicyConfiguration> policyConfigs = response.getPoliciesConfigs();
    return buildPolicyConfigMap(policyConfigs);
  }

  /**
   * 从策略配置列表构建队列到策略配置的映射表。
   *
   * @param policyConfigs 策略配置列表
   * @return 队列名到策略配置的映射表
   */
  private static Map<String, SubClusterPolicyConfiguration> buildPolicyConfigMap(
      List<SubClusterPolicyConfiguration> policyConfigs) {
    Map<String, SubClusterPolicyConfiguration> queuePolicyConfigs = new HashMap<>();
    for (SubClusterPolicyConfiguration policyConfig : policyConfigs) {
      queuePolicyConfigs.put(policyConfig.getQueue(), policyConfig);
    }
    return queuePolicyConfigs;
  }

  /**
   * 从缓存请求中提取并构建队列到策略配置的映射表。
   *
   * @param cacheRequest 缓存请求对象
   * @return 队列名到策略配置的映射表
   */
  public static Map<String, SubClusterPolicyConfiguration> buildPolicyConfigMap(
      CacheRequest<String, ?> cacheRequest){
    Object value = cacheRequest.value;
    SubClusterPolicyConfigurationCacheResponse response =
        SubClusterPolicyConfigurationCacheResponse.class.cast(value);
    List<SubClusterPolicyConfiguration> subClusters = response.getList();
    return buildPolicyConfigMap(subClusters);
  }

  /**
   * 从状态存储查询策略配置，封装为缓存响应对象。
   *
   * @return 封装好的策略配置缓存响应
   * @throws YarnException 状态存储调用失败时抛出异常
   */
  private CacheResponse<SubClusterPolicyConfiguration> buildSubClusterPolicyConfigurationResponse()
      throws YarnException {
    GetSubClusterPoliciesConfigurationsRequest request =
        GetSubClusterPoliciesConfigurationsRequest.newInstance();
    GetSubClusterPoliciesConfigurationsResponse response =
        stateStore.getPoliciesConfigurations(request);
    List<SubClusterPolicyConfiguration> policyConfigs = response.getPoliciesConfigs();
    CacheResponse<SubClusterPolicyConfiguration> cacheResponse =
        new SubClusterPolicyConfigurationCacheResponse();
    cacheResponse.setList(policyConfigs);
    return cacheResponse;
  }

  /**
   * 封装缓存请求，包含缓存键和缓存值加载结果。
   * @param <K> 缓存键类型
   * @param <V> 缓存值类型
   */
  public class CacheRequest<K, V> {
    private K key;
    private V value;

    CacheRequest(K pKey, V pValue) {
      this.key = pKey;
      this.value = pValue;
    }

    public V getValue() throws Exception {
      return value;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(key).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj == null) {
        return false;
      }

      if (obj instanceof CacheRequest) {
        Class<CacheRequest> cacheRequestClass = CacheRequest.class;
        CacheRequest other = cacheRequestClass.cast(obj);
        return new EqualsBuilder().append(key, other.key).isEquals();
      }

      return false;
    }
  }

  /**
   * 通用缓存响应容器，可存储列表或单个元素。
   * @param <R> 缓存结果类型
   */
  public class CacheResponse<R> {
    private List<R> list;

    private R item;

    public List<R> getList() {
      return list;
    }

    public void setList(List<R> list) {
      this.list = list;
    }

    public R getItem() {
      return item;
    }

    public void setItem(R pItem) {
      this.item = pItem;
    }
  }

  /**
   * 子集群信息缓存响应实现类。
   */
  public class SubClusterInfoCacheResponse extends CacheResponse<SubClusterInfo> {
    @Override
    public List<SubClusterInfo> getList() {
      return super.getList();
    }

    @Override
    public void setList(List<SubClusterInfo> list) {
      super.setList(list);
    }

    @Override
    public SubClusterInfo getItem() {
      return super.getItem();
    }

    @Override
    public void setItem(SubClusterInfo item) {
      super.setItem(item);
    }
  }

  /**
   * 子集群策略配置缓存响应实现类。
   */
  public class SubClusterPolicyConfigurationCacheResponse
      extends CacheResponse<SubClusterPolicyConfiguration> {
    @Override
    public List<SubClusterPolicyConfiguration> getList() {
      return super.getList();
    }

    @Override
    public void setList(List<SubClusterPolicyConfiguration> list) {
      super.setList(list);
    }

    @Override
    public SubClusterPolicyConfiguration getItem() {
      return super.getItem();
    }

    @Override
    public void setItem(SubClusterPolicyConfiguration item) {
      super.setItem(item);
    }
  }

  /**
   * 应用归属子集群缓存响应实现类。
   */
  public class ApplicationHomeSubClusterCacheResponse
      extends CacheResponse<SubClusterId> {
    @Override
    public List<SubClusterId> getList() {
      return super