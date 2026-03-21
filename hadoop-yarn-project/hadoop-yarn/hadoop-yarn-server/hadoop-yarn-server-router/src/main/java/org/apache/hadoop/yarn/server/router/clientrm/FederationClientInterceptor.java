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

package org.apache.hadoop.yarn.server.router.clientrm;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.RouterPolicyFacade;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.retry.FederationActionRetry;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.RouterAuditLogger;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SUBMIT_NEW_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APP_REPORT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.FORCE_KILL_APP;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.TARGET_CLIENT_RM_SERVICE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UNKNOWN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERNODES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_QUEUE_USER_ACLS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APPLICATIONS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERMETRICS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_QUEUEINFO;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.MOVE_APPLICATION_ACROSS_QUEUES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NEW_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SUBMIT_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.LIST_RESERVATIONS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.DELETE_RESERVATION;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NODETOLABELS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_LABELSTONODES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERNODELABELS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APPLICATION_ATTEMPT_REPORT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_APPLICATION_ATTEMPTS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CONTAINERREPORT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CONTAINERS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_DELEGATIONTOKEN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.RENEW_DELEGATIONTOKEN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.CANCEL_DELEGATIONTOKEN;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.FAIL_APPLICATIONATTEMPT;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_APPLICATIONPRIORITY;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.SIGNAL_TOCONTAINER;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.UPDATE_APPLICATIONTIMEOUTS;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_RESOURCEPROFILES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_RESOURCEPROFILE;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_RESOURCETYPEINFO;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_ATTRIBUTESTONODES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_CLUSTERNODEATTRIBUTES;
import static org.apache.hadoop.yarn.server.router.RouterAuditLogger.AuditConstants.GET_NODESTOATTRIBUTES;

/**
 * YARN Router联邦集群客户端请求拦截器，是拦截器链的最后一环，封装了所有联邦集群
 * 客户端请求的路由转发逻辑，将请求转发到对应子集群RM并聚合返回结果。
 * 支持跨多个YARN子集群的应用调度与扩展，所有联邦特有的逻辑都封装在此类中。
 */
public class FederationClientInterceptor
    extends AbstractClientRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationClientInterceptor.class);

  // 应用提交重试次数
  private int numSubmitRetries;
  // 子集群ID到RM客户端代理的缓存映射
  private Map<SubClusterId, ApplicationClientProtocol> clientRMProxies;
  // 联邦状态存储门面，提供查询、修改应用与子集群映射关系能力
  private FederationStateStoreFacade federationFacade;
  // 随机数生成器，用于随机选择子集群
  private Random rand;
  // 路由策略门面，负责根据策略选择应用/预约的主站子集群
  private RouterPolicyFacade policyFacade;
  // Router指标收集器
  private RouterMetrics routerMetrics;
  // 并行请求子集群的线程池
  private ThreadPoolExecutor executorService;
  // 单调时钟，用于计算请求处理耗时
  private final Clock clock = new MonotonicClock();
  // 是否允许返回部分应用报告（部分子集群失败时仍返回可用结果）
  private boolean returnPartialReport;
  // 提交重试间隔时间（毫秒）
  private long submitIntervalTime;
  // 是否允许并行请求返回部分结果
  private boolean allowPartialResult;

  @Override
  public void init(String userName) {
    super.init(userName);

    federationFacade = FederationStateStoreFacade.getInstance(getConf());
    rand = new Random(System.currentTimeMillis());

    int numMinThreads = getNumMinThreads(getConf());

    int numMaxThreads = getNumMaxThreads(getConf());

    long keepAliveTime = getConf().getTimeDuration(
        YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_KEEP_ALIVE_TIME,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREAD_POOL_KEEP_ALIVE_TIME, TimeUnit.SECONDS);

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("RPC Router Client-" + userName + "-%d ").build();

    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    this.executorService = new ThreadPoolExecutor(numMinThreads, numMaxThreads,
        keepAliveTime, TimeUnit.MILLISECONDS, workQueue, threadFactory);

    // 允许空闲线程超时退出清理
    boolean allowCoreThreadTimeOut =  getConf().getBoolean(
        YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT);

    if (keepAliveTime > 0 && allowCoreThreadTimeOut) {
      this.executorService.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
    }

    final Configuration conf = this.getConf();

    try {
      // 初始化路由策略门面
      policyFacade = new RouterPolicyFacade(conf, federationFacade,
          this.federationFacade.getSubClusterResolver(), null);
    } catch (FederationPolicyInitializationException e) {
      LOG.error(e.getMessage());
    }

    // 读取应用提交重试次数配置
    numSubmitRetries = conf.getInt(
        YarnConfiguration.ROUTER_CLIENTRM_SUBMIT_RETRY,
        YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_SUBMIT_RETRY);

    // 读取提交重试间隔配置
    submitIntervalTime = conf.getTimeDuration(
        YarnConfiguration.ROUTER_CLIENTRM_SUBMIT_INTERVAL_TIME,
        YarnConfiguration.DEFAULT_CLIENTRM_SUBMIT_INTERVAL_TIME, TimeUnit.MILLISECONDS);

    // 初始化客户端代理缓存
    clientRMProxies = new ConcurrentHashMap<>();
    routerMetrics = RouterMetrics.getMetrics();

    // 读取是否允许返回部分应用报告配置
    returnPartialReport = conf.getBoolean(
        YarnConfiguration.ROUTER_CLIENTRM_PARTIAL_RESULTS_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_PARTIAL_RESULTS_ENABLED);

    // 读取是否允许并行请求返回部分结果配置
    allowPartialResult = conf.getBoolean(
        YarnConfiguration.ROUTER_INTERCEPTOR_ALLOW_PARTIAL_RESULT_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_INTERCEPTOR_ALLOW_PARTIAL_RESULT_ENABLED);
  }

  @Override
  public void setNextInterceptor(ClientRequestInterceptor next) {
    // 本拦截器必须是链的最后一环，不允许设置下一个拦截器
    throw new YarnRuntimeException("setNextInterceptor is being called on "
        + "FederationClientRequestInterceptor, which should be the last one "
        + "in the chain. Check if the interceptor pipeline configuration "
        + "is correct");
  }

  /**
   * 获取指定子集群的RM客户端代理，缓存已创建的代理实例。
   * @param subClusterId 子集群ID
   * @return RM客户端代理
   * @throws YarnException 获取代理失败时抛出异常
   */
  @VisibleForTesting
  protected Application