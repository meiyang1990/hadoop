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

package org.apache.hadoop.yarn.server.federation.store.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Comparator;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.metrics.FederationStateStoreClientMetrics;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMDTSecretManagerState;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationApplicationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationReservationHomeSubClusterStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationMembershipStateStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationPolicyStoreInputValidator;
import org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.federation.store.utils.FederationStateStoreUtils.filterHomeSubCluster;

/**
 * 文件说明：YARN联邦状态存储的内存实现，将所有联邦元数据存储在进程内存中，
 * 主要用于测试和小规模集群场景，支持子集群注册发现、应用位置存储、策略配置、代理令牌管理等核心功能
 */
/**
 * In-memory implementation of {@link FederationStateStore}.
 */
public class MemoryFederationStateStore implements FederationStateStore {

  // 子集群信息存储，key为子集群ID，value为子集群详细信息
  private Map<SubClusterId, SubClusterInfo> membership;
  // 应用位置存储，key为应用ID，value为应用所属子集群信息
  private Map<ApplicationId, ApplicationHomeSubCluster> applications;
  // 预约位置存储，key为预约ID，value为预约所属子集群ID
  private Map<ReservationId, SubClusterId> reservations;
  // 路由策略配置存储，key为队列名称，value为对应子集群路由策略配置
  private Map<String, SubClusterPolicyConfiguration> policies;
  // 路由器RM代理密钥管理器状态，存储代理密钥和令牌信息
  private RouterRMDTSecretManagerState routerRMSecretManagerState;
  // 状态存储中保留的最大应用数量
  private int maxAppsInStateStore;
  // 代理令牌序列号生成器
  private AtomicInteger sequenceNum;
  // 主密钥ID生成器
  private AtomicInteger masterKeyId;
  // 当前状态存储版本信息
  private static final Version CURRENT_VERSION_INFO = Version
      .newInstance(1, 1);
  // 序列化后的版本字节数组
  private byte[] version;

  private final MonotonicClock clock = new MonotonicClock();

  public static final Logger LOG =
      LoggerFactory.getLogger(MemoryFederationStateStore.class);

  @Override
  /**
   * 初始化内存状态存储，从配置中读取最大应用数量限制，初始化各存储容器
   */
  public void init(Configuration conf) {
    membership = new ConcurrentHashMap<>();
    applications = new ConcurrentHashMap<>();
    reservations = new ConcurrentHashMap<>();
    policies = new ConcurrentHashMap<>();
    routerRMSecretManagerState = new RouterRMDTSecretManagerState();
    maxAppsInStateStore = conf.getInt(
        YarnConfiguration.FEDERATION_STATESTORE_MAX_APPLICATIONS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_MAX_APPLICATIONS);
    sequenceNum = new AtomicInteger();
    masterKeyId = new AtomicInteger();
    version = ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
  }

  @Override
  /**
   * 关闭状态存储，清空所有引用
   */
  public void close() {
    membership = null;
    applications = null;
    reservations = null;
    policies = null;
  }

  @Override
  /**
   * 注册新子集群到联邦状态存储
   */
  public SubClusterRegisterResponse registerSubCluster(SubClusterRegisterRequest request)
      throws YarnException {
    // 记录开始时间用于性能统计
    long startTime = clock.getTime();

    // 输入参数校验
    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterInfo subClusterInfo = request.getSubClusterInfo();

    // 获取当前UTC时间作为注册时间
    long currentTime =
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();
    // 构造要保存的子集群信息，填充当前注册时间
    SubClusterInfo subClusterInfoToSave =
        SubClusterInfo.newInstance(subClusterInfo.getSubClusterId(),
            subClusterInfo.getAMRMServiceAddress(),
            subClusterInfo.getClientRMServiceAddress(),
            subClusterInfo.getRMAdminServiceAddress(),
            subClusterInfo.getRMWebServiceAddress(), currentTime,
            subClusterInfo.getState(), subClusterInfo.getLastStartTime(),
            subClusterInfo.getCapability());

    // 将子集群信息存入内存映射
    membership.put(subClusterInfo.getSubClusterId(), subClusterInfoToSave);
    long stopTime = clock.getTime();

    // 更新指标统计，记录成功的状态存储调用耗时
    FederationStateStoreClientMetrics.succeededStateStoreCall(stopTime - startTime);
    return SubClusterRegisterResponse.newInstance();
  }

  @Override
  /**
   * 注销子集群，更新子集群状态为注销状态
   */
  public SubClusterDeregisterResponse deregisterSubCluster(SubClusterDeregisterRequest request)
      throws YarnException {

    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterInfo subClusterInfo = membership.get(request.getSubClusterId());
    if (subClusterInfo == null) {
      FederationStateStoreUtils.logAndThrowStoreException(
          LOG, "SubCluster %s not found", request.getSubClusterId());
    } else {
      subClusterInfo.setState(request.getState());
    }

    return SubClusterDeregisterResponse.newInstance();
  }

  @Override
  /**
   * 处理子集群心跳，更新子集群最后心跳时间和状态、资源能力
   */
  public SubClusterHeartbeatResponse subClusterHeartbeat(SubClusterHeartbeatRequest request)
      throws YarnException {

    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();
    SubClusterInfo subClusterInfo = membership.get(subClusterId);

    if (subClusterInfo == null) {
      FederationStateStoreUtils.logAndThrowStoreException(
          LOG, "SubCluster %s does not exist; cannot heartbeat.", request.getSubClusterId());
    }

    // 获取当前UTC时间更新心跳
    long currentTime =
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();

    // 更新心跳时间、状态和资源能力
    subClusterInfo.setLastHeartBeat(currentTime);
    subClusterInfo.setState(request.getState());
    subClusterInfo.setCapability(request.getCapability());

    return SubClusterHeartbeatResponse.newInstance();
  }

  @VisibleForTesting
  /**
   * 测试用：手动设置子集群最后心跳时间
   */
  public void setSubClusterLastHeartbeat(SubClusterId subClusterId,
      long lastHeartbeat) throws YarnException {
    SubClusterInfo subClusterInfo = membership.get(subClusterId);
    if (subClusterInfo == null) {
      throw new YarnException(
          "Subcluster " + subClusterId.toString() + " does not exist");
    }
    subClusterInfo.setLastHeartBeat(lastHeartbeat);
  }

  @Override
  /**
   * 根据ID查询单个子集群信息
   */
  public GetSubClusterInfoResponse getSubCluster(GetSubClusterInfoRequest request)
      throws YarnException {

    FederationMembershipStateStoreInputValidator.validate(request);
    SubClusterId subClusterId = request.getSubClusterId();

    if (!membership.containsKey(subClusterId)) {
      LOG.warn("The queried SubCluster: {} does not exist.", subClusterId);
      return null;
    }

    return GetSubClusterInfoResponse.newInstance(membership.get(subClusterId));
  }

  @Override
  /**
   * 查询所有子集群信息，支持过滤非活跃子集群
   */
  public GetSubClustersInfoResponse getSubClusters(GetSubClustersInfoRequest request)
      throws YarnException {

    List<SubClusterInfo> result = new ArrayList<>();

    // 遍历所有子集群，根据过滤条件筛选
    for (SubClusterInfo info : membership.values()) {
      if (!request.getFilterInactiveSubClusters() || info.getState().isActive()) {
        result.add(info);
      }
    }

    return GetSubClustersInfoResponse.newInstance(result);
  }

  // FederationApplicationHomeSubClusterStore methods

  @Override
  /**
   * 添加应用所属子集群信息到状态存储
   */
  public AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationHomeSubCluster homeSubCluster = request.getApplicationHomeSubCluster();
    SubClusterId homeSubClusterId = homeSubCluster.getHomeSubCluster();
    ApplicationSubmissionContext appSubmissionContext = homeSubCluster.getApplicationSubmissionContext();
    ApplicationId appId = homeSubCluster.getApplicationId();

    LOG.info("appId = {}, homeSubClusterId = {}, appSubmissionContext = {}.",
        appId, homeSubClusterId, appSubmissionContext);

    // 仅当应用不存在时添加，避免覆盖
    if (!applications.containsKey(appId)) {
      applications.put(appId, homeSubCluster);
    }

    ApplicationHomeSubCluster respHomeSubCluster = applications.get(appId);
    return AddApplicationHomeSubClusterResponse.newInstance(respHomeSubCluster.getHomeSubCluster());
  }

  @Override
  /**
   * 更新已有应用所属子集群信息
   */
  public UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);

    ApplicationId appId =
        request.getApplicationHomeSubCluster().getApplicationId();

    if (!applications.containsKey(appId)) {
      FederationStateStoreUtils.logAndThrowStoreException(LOG,
          "Application %s does not exist.", appId);
    }

    applications.put(appId, request.getApplicationHomeSubCluster());
    return UpdateApplicationHomeSubClusterResponse.newInstance();
  }

  @Override
  /**
   * 查询单个应用所属子集群信息，支持控制是否返回提交上下文
   */
  public GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException {

    FederationApplicationHomeSubClusterStoreInputValidator.validate(request);
    ApplicationId appId = request.getApplicationId();
    if (!applications.containsKey(appId)) {
      FederationStateStoreUtils.logAndThrowStoreException(LOG,
          "Application %s does not exist.", appId);
    }

    // Whether the returned result contains context
    ApplicationHomeSubCluster appHomeSubCluster = applications.get(appId);
    ApplicationSubmissionContext submissionContext