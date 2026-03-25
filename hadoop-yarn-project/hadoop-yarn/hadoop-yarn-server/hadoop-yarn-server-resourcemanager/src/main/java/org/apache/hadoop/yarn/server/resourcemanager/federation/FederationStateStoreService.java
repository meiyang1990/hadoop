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

package org.apache.hadoop.yarn.server.resourcemanager.federation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.retry.FederationActionRetry;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
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
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN联邦环境下，RM侧联邦状态存储服务，实现了FederationStateStore接口，
 * 负责本子集群向联邦状态注册、维持心跳，并代理对状态存储的各种操作。
 */
public class FederationStateStoreService extends AbstractService
    implements FederationStateStore {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreService.class);

  private Configuration config;
  // 定时心跳执行线程池
  private ScheduledExecutorService scheduledExecutorService;
  // 状态存储心跳任务
  private FederationStateStoreHeartbeat stateStoreHeartbeat;
  // 实际底层状态存储客户端代理
  private FederationStateStore stateStoreClient = null;
  // 当前子集群ID
  private SubClusterId subClusterId;
  // 心跳间隔（秒）
  private long heartbeatInterval;
  // 首次心跳初始延迟（秒）
  private long heartbeatInitialDelay;
  // RM上下文对象
  private RMContext rmContext;
  //  monotonic递增时钟，用于统计耗时
  private final Clock clock = new MonotonicClock();
  // 服务指标采集
  private FederationStateStoreServiceMetrics metrics;
  // 清理线程名称前缀
  private String cleanUpThreadNamePrefix = "FederationStateStoreService-Clean-Thread";
  // 清理操作最大重试次数
  private int cleanUpRetryCountNum;
  // 清理重试间隔（毫秒）
  private long cleanUpRetrySleepTime;
  // JAXB上下文解析器，用于Web服务序列化
  private JAXBContextResolver resolver;

  /**
   * 构造函数，基于RM上下文初始化服务。
   * @param rmContext RM上下文
   */
  public FederationStateStoreService(RMContext rmContext) {
    super(FederationStateStoreService.class.getName());
    LOG.info("FederationStateStoreService initialized");
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.config = conf;

    // 创建联邦状态存储操作的重试策略
    RetryPolicy retryPolicy =
        FederationStateStoreFacade.createRetryPolicy(conf);

    // 基于配置创建带重试的状态存储客户端实例
    this.stateStoreClient =
        (FederationStateStore) FederationStateStoreFacade.createRetryInstance(
            conf, YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS,
            YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLIENT_CLASS,
            FederationStateStore.class, retryPolicy);
    this.stateStoreClient.init(conf);
    LOG.info("Initialized state store client class");

    // 从配置获取当前集群ID，构造子集群ID
    this.subClusterId =
        SubClusterId.newInstance(YarnConfiguration.getClusterId(conf));

    // 加载心跳间隔配置，若非法则使用默认值
    heartbeatInterval = conf.getLong(
        YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INTERVAL_SECS,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INTERVAL_SECS);

    if (heartbeatInterval <= 0) {
      heartbeatInterval =
          YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INTERVAL_SECS;
    }

    // 加载心跳初始延迟配置
    heartbeatInitialDelay = conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY,
        TimeUnit.SECONDS);

    if (heartbeatInitialDelay <= 0) {
      LOG.warn("{} configured value is wrong, must be > 0; using default value of {}",
          YarnConfiguration.FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY,
          YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY);
      heartbeatInitialDelay =
          YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_HEARTBEAT_INITIAL_DELAY;
    }

    // 加载已完成应用清理重试配置
    cleanUpRetryCountNum = conf.getInt(YarnConfiguration.FEDERATION_STATESTORE_CLEANUP_RETRY_COUNT,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLEANUP_RETRY_COUNT);

    cleanUpRetrySleepTime = conf.getTimeDuration(
        YarnConfiguration.FEDERATION_STATESTORE_CLEANUP_RETRY_SLEEP_TIME,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLEANUP_RETRY_SLEEP_TIME,
        TimeUnit.MILLISECONDS);

    LOG.info("Initialized federation membership service.");

    // 初始化服务指标
    this.metrics = FederationStateStoreServiceMetrics.getMetrics();
    LOG.info("Initialized federation statestore service metrics.");

    // 初始化JAXB上下文解析器
    this.resolver = new JAXBContextResolver(conf);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 注册子集群并启动定时心跳任务
    registerAndInitializeHeartbeat();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    Exception ex = null;
    try {
      // 关闭定时心跳线程池
      if (this.scheduledExecutorService != null
          && !this.scheduledExecutorService.isShutdown()) {
        this.scheduledExecutorService.shutdown();
        LOG.info("Stopped federation membership heartbeat");
      }
    } catch (Exception e) {
      LOG.error("Failed to shutdown ScheduledExecutorService", e);
      ex = e;
    }

    // 子集群注销并关闭底层状态存储客户端
    if (this.stateStoreClient != null) {
      try {
        deregisterSubCluster(SubClusterDeregisterRequest
            .newInstance(subClusterId, SubClusterState.SC_UNREGISTERED));
      } finally {
        this.stateStoreClient.close();
      }
    }

    if (ex != null) {
      throw ex;
    }
  }

  // Return a client accessible string representation of the service address.
  /**
   * 将InetSocketAddress转换为客户端可访问的"IP:端口"格式字符串。
   * @param address 原始地址对象
   * @return 格式化地址字符串
   */
  private String getServiceAddress(InetSocketAddress address) {
    InetSocketAddress socketAddress = NetUtils.getConnectAddress(address);
    return socketAddress.getAddress().getHostAddress() + ":"
        + socketAddress.getPort();
  }

  /**
   * 向联邦状态存储注册当前子集群，并初始化定时心跳任务。
   */
  private void registerAndInitializeHeartbeat() {
    // 获取RM各服务地址并格式化
    String clientRMAddress =
        getServiceAddress(rmContext.getClientRMService().getBindAddress());
    String amRMAddress = getServiceAddress(
        rmContext.getApplicationMasterService().getBindAddress());
    String rmAdminAddress = getServiceAddress(
        config.getSocketAddr(YarnConfiguration.RM_ADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADMIN_PORT));
    String webAppAddress = getServiceAddress(NetUtils
        .createSocketAddr(WebAppUtils.getRMWebAppURLWithScheme(config)));

    // 构造子集群信息对象
    SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
        amRMAddress, clientRMAddress, rmAdminAddress, webAppAddress,
        SubClusterState.SC_NEW, ResourceManager.getClusterTimeStamp(), "");
    try {
      // 向状态存储注册子集群
      registerSubCluster(SubClusterRegisterRequest.newInstance(subClusterInfo));
      LOG.info("Successfully registered for federation subcluster: {}",
          subClusterInfo);
    } catch (Exception e) {
      throw new YarnRuntimeException(
          "Failed to register Federation membership with the StateStore", e);
    }
    // 创建心跳任务实例
    stateStoreHeartbeat = new FederationStateStoreHeartbeat(subClusterId,
        stateStoreClient, rmContext.getScheduler(), resolver);
    // 创建单线程定时线程池，启动固定延迟心跳任务
    scheduledExecutorService =
        HadoopExecutors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleWithFixedDelay(stateStoreHeartbeat,
        heartbeatInitialDelay, heartbeatInterval, TimeUnit.SECONDS);
    LOG.info("Started federation membership heartbeat with interval: {} and initial delay: {}",
        heartbeatInterval, heartbeatInitialDelay);
  }

  @VisibleForTesting
  public FederationStateStore getStateStoreClient() {
    return stateStoreClient;
  }

  @VisibleForTesting
  public FederationStateStoreHeartbeat getStateStoreHeartbeatThread() {
    return stateStoreHeartbeat;
  }

  @Override
  public Version getCurrentVersion() {
    return stateStoreClient.getCurrentVersion();
  }

  @Override
  public Version loadVersion() throws Exception {
    return stateStoreClient.getCurrentVersion();
  }

  @Override
  public void storeVersion() throws Exception {
    stateStoreClient.storeVersion();
  }

  @Override
  public void checkVersion() throws Exception {
    stateStoreClient.checkVersion();
  }

  @Override
  public void deleteStateStore() throws Exception {
    stateStoreClient.deleteStateStore();
  }

  @Override
  public GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException {
    FederationClientMethod<GetSubClusterPolicyConfigurationResponse> clientMethod =
        new FederationClientMethod<>("getPolicyConfiguration",
        GetSubClusterPolicyConfigurationRequest.class, request,
        GetSubClusterPolicyConfigurationResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException {
    FederationClientMethod<SetSubClusterPolicyConfigurationResponse> clientMethod =
        new FederationClientMethod<>("setPolicyConfiguration",
        SetSubClusterPolicyConfigurationRequest.class, request,
        SetSubClusterPolicyConfigurationResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException {
    FederationClientMethod<GetSubClusterPoliciesConfigurationsResponse> clientMethod =
        new FederationClientMethod<>("getPoliciesConfigurations",
        GetSubClusterPoliciesConfigurationsRequest.class, request,
        GetSubClusterPoliciesConfigurationsResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();
  }

  @Override
  public DeleteSubClusterPoliciesConfigurationsResponse deletePoliciesConfigurations(
      DeleteSubClusterPoliciesConfigurationsRequest request) throws YarnException {
    FederationClientMethod<DeleteSubClusterPoliciesConfigurationsResponse> clientMethod =
        new FederationClientMethod<>("deletePoliciesConfigurations",
        DeleteSubClusterPoliciesConfigurationsRequest.class, request,
        DeleteSubClusterPoliciesConfigurationsResponse.class, stateStoreClient, clock);
    return clientMethod.invoke();