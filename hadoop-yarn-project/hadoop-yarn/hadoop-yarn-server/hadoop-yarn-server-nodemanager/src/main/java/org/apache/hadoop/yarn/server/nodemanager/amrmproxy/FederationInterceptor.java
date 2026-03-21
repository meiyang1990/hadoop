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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RegisterApplicationMasterResponseProto;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.AMHeartbeatRequestHandler;
import org.apache.hadoop.yarn.server.AMRMClientRelayer;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.retry.FederationActionRetry;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationRegistryClient;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.uam.UnmanagedAMPoolManager;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件功能：YARN联邦场景下AMRMProxy的请求拦截器实现，负责将应用主(AM)的请求路由分发到多个子集群RM，合并响应返回给AM
 * 核心职责：支持应用跨多个YARN子集群扩展运行，封装所有联邦特有的请求处理逻辑，始终作为拦截器链的最后一环
 */
public class FederationInterceptor extends AbstractRequestInterceptor {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationInterceptor.class);

  public static final String NMSS_CLASS_PREFIX = "FederationInterceptor/";

  public static final String NMSS_REG_REQUEST_KEY =
      NMSS_CLASS_PREFIX + "registerRequest";
  public static final String NMSS_REG_RESPONSE_KEY =
      NMSS_CLASS_PREFIX + "registerResponse";

  /**
   * 当启用AMRMProxy高可用时，二级AMRMToken存储在Yarn Registry；否则如果启用NM恢复，UAM令牌存储在本地NMSS的该目录下。
   */
  public static final String NMSS_SECONDARY_SC_PREFIX =
      NMSS_CLASS_PREFIX + "secondarySC/";
  public static final String STRING_TO_BYTE_FORMAT = "UTF-8";

  private static final RecordFactory RECORD_FACTORY =
      RecordFactoryProvider.getRecordFactory(null);

  /**
   * 对AM而言，FederationInterceptor行为和YARN RM完全一致。这里保存上一次心跳响应，用于处理AM的重复心跳和响应ID。
   */
  private AllocateResponse lastAllocateResponse;
  private final Object lastAllocateResponseLock = new Object();

  private ApplicationAttemptId attemptId;

  /**
   * home子集群是当前AM容器运行所在的子集群。
   */
  private AMRMClientRelayer homeRMRelayer;
  private SubClusterId homeSubClusterId;
  private AMHeartbeatRequestHandler homeHeartbeatHandler;

  /**
   * 二级子集群（home之外的子集群）UAM池，使用subClusterId作为uamId。除home RM外每个子集群RM创建一个UAM。
   * UAM的创建和注册按需触发，当联邦策略第一次将资源请求路由到对应子集群时执行。心跳异步处理以提升性能。
   */
  private final UnmanagedAMPoolManager uamPool;

  /**
   * 二级子集群的RM代理中继器，管理所有待处理请求。
   */
  private final Map<String, AMRMClientRelayer> secondaryRelayers;

  /**
   * 存储从所有子集群RM异步接收、尚未合并返回给AM的AllocateResponses。
   */
  private final Map<SubClusterId, List<AllocateResponse>> asyncResponseSink;

  /**
   * 保存所有已知子集群的上一次分配响应，结合子集群超时，用于组装返回给AM的集群全局信息（可用资源、节点数等）。
   */
  private final Map<SubClusterId, AllocateResponse> lastSCResponse;

  /**
   * 存储尚未消费的异步UAM注册结果。
   */
  private final Map<SubClusterId, RegisterApplicationMasterResponse> uamRegistrations;

  // 用于单元测试同步
  private final Map<SubClusterId, Future<?>> uamRegisterFutures;

  /** 异步操作使用的线程池。 */
  private ExecutorService threadpool;

  /**
   * 支持NM重启后工作保留的标志。如果刚完成恢复，需要在下一次allocate时向AM抛出
   * {@link ApplicationMasterNotRegisteredException}，触发AM重新注册（我们会返回保存的注册响应）
   * 和重发所有待处理请求，确保所有{@link AMRMClientRelayer}重新填充待处理请求。
   */
  private volatile boolean justRecovered;

  /** 如果为true，allocate将是空操作，跳过实际处理。 */
  private volatile boolean finishAMCalled;

  /**
   * 记录容器ID和创建该容器的子集群RM映射，后续针对已有容器的请求可以转发到正确子集群。
   */
  private final Map<ContainerId, SubClusterId> containerIdToSubClusterIdMap;

  /**
   * AM发送的原始注册请求，会被复用向所有子集群RM注册/重新注册。
   */
  private RegisterApplicationMasterRequest amRegistrationRequest;

  /**
   * 返回给AM的原始注册响应，会被复用处理AM因为超时触发的重复注册请求。
   */
  private RegisterApplicationMasterResponse amRegistrationResponse;

  private FederationStateStoreFacade federationFacade;

  private SubClusterResolver subClusterResolver;

  /**
   * 记录已知子集群上一次收到成功心跳响应的时间。lastHeartbeatTimeStamp.keySet()
   * 应该和uamPool.getAllUAMIds()保持同步。
   */
  private Map<SubClusterId, Long> lastSCResponseTime;
  private long subClusterTimeOut;

  private long lastAMHeartbeatTime;

  /** 用于在子集群之间拆分请求的联邦路由策略。 */
  private FederationAMRMProxyPolicy policyInterpreter;

  private FederationRegistryClient registryClient;

  // 第一次异步心跳响应的最大等待时间
  private long heartbeatMaxWaitTimeMs;

  private int registerUamRetryNum;

  private long registerUamRetryInterval;

  private boolean waitUamRegisterDone;

  private final MonotonicClock clock = new MonotonicClock();

  /*
   * 对于UAM，keepContainersAcrossApplicationAttempts始终为true。
   * 重新向RM注册时，RM会清空节点集合并为传输的容器重新生成NMToken。但如果AM的keepContainersAcrossApplicationAttempts为false，
   * AM可能不会调用getNMTokensFromPreviousAttempts，导致RegisterApplicationMasterResponse传递的NMToken丢失。
   * 这里缓存这些NMToken，在allocate阶段传递给AM。
   * */
  private Set<NMToken> nmTokenMapFromRegisterSecondaryCluster;

  /**
   * 创建FederationInterceptor实例。
   */
  public FederationInterceptor() {
    this.containerIdToSubClusterIdMap = new ConcurrentHashMap<>();
    this.asyncResponseSink = new ConcurrentHashMap<>();
    this.lastSCResponse = new ConcurrentHashMap<>();
    this.uamRegistrations = new ConcurrentHashMap<>();
    this.uamRegisterFutures = new ConcurrentHashMap<>();
    this.threadpool = Executors.newCachedThreadPool();
    this.uamPool = createUnmanagedAMPoolManager(this.threadpool);
    this.secondaryRelayers = new ConcurrentHashMap<>();
    this.amRegistrationRequest = null;
    this.amRegistrationResponse = null;
    this.justRecovered = false;
    this.finishAMCalled = false;
    this.lastSCResponseTime = new ConcurrentHashMap<>();
    this.lastAMHeartbeatTime = this.clock.getTime();
    this.nmTokenMapFromRegisterSecondaryCluster = new ConcurrentHashSet<>();
  }

  /**
   * 使用指定上下文初始化拦截器实例。
   */
  @Override
  public void init(AMRMProxyApplicationContext appContext) {
    super.init(appContext);
    LOG.info("Initializing Federation Interceptor");

    // 更新配置（如果存在）
    Configuration conf = appContext.getConf();
    if (conf == null) {
      conf = getConf();
    } else {
      setConf(conf);
    }

    // 用于和home RM以及Yarn Registry通信的代理UGI，加载home RM颁发的最新AMRMToken
    UserGroupInformation appOwner;
    try {
      appOwner = UserGroupInformation.createProxyUser(appContext.getUser(),
          UserGroupInformation.getCurrentUser());
    } catch (Exception ex) {
      throw new YarnRuntimeException(ex);
    }

    if (appContext.getRegistryClient() != null) {
      this.registryClient = new FederationRegistryClient(conf,
          appContext.getRegistryClient(), appOwner);
      // 添加所有应用凭据用于访问Yarn Registry
      if (appContext.getCredentials() != null) {
        appOwner.addCredentials(appContext.getCredentials());
      }
    }

    this.attemptId = appContext.getApplicationAttemptId();
    ApplicationId appId = this.attemptId.getApplicationId();
    this.homeSubClusterId =
        SubClusterId.newInstance(YarnConfiguration.getClusterId(conf));
    this.homeRMRelayer = new AMRMClientRelayer(createHomeRMProxy(appContext,
        ApplicationMasterProtocol.class, appOwner), appId,
        this.homeSubClusterId.toString(), conf);

    this.homeHeartbeatHandler =
        createHomeHeartbeatHandler(conf, appId, this.homeRMRelayer);
    this.homeHeartbeatHandler.setUGI(appOwner);
    this.homeHeartbeatHandler.setDaemon(true);
    this.homeHeartbeatHandler.start();

    // 应用主注册前将lastResponseId设置为-1
    this.lastAllocateResponse =
        RECORD_FACTORY.newRecordInstance(AllocateResponse.class);
    this.lastAllocateResponse
        .setResponseId(AMRMClientUtils.PRE_REGISTER_RESPONSE_ID);

    this.federationFacade = FederationStateStoreFacade.getInstance(conf);
    this.subClusterResolver = this.federationFacade.getSubClusterResolver();

    // AMRMProxyPolicy将在registerApplicationMaster中初始化
    this.policyInterpreter = null;

    this.uamPool.init(conf);
    this.uamPool.start();

    this.heartbeatMaxWaitTimeMs =
        conf.getLong(YarnConfiguration.FEDERATION_AMRMPROXY_HB_MAX_WAIT_MS,
            YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_HB_MAX_WAIT_MS);

    this.subClusterTimeOut =
        conf.getLong(YarnConfiguration.FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT,
            YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT);
    if (this.subClusterTimeOut <= 0) {
      LOG.info(
          "{} configured to be {}, should be positive. Using default of {}.",
          YarnConfiguration.FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT,
          this.subClusterTimeOut,
          YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT);
      this.subClusterTimeOut =
          YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_SUBCLUSTER_TIMEOUT);
    }

    this.registerUamRetryNum = conf.getInt(
        YarnConfiguration.FEDERATION_AMRMPROXY_REGISTER_UAM_RETRY_COUNT,
        YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_REGISTER_UAM_RETRY_COUNT);
    if (this.registerUamRetryNum <= 0) {
      LOG.info("{} configured to be {}, should be positive. Using default of {}.",
          YarnConfiguration.FEDERATION_AMRMPROXY_REGISTER_UAM_RETRY_COUNT,
          this.subClusterTimeOut,
          YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_REGISTER_UAM_RETRY_COUNT);
      this.registerUamRetryNum =
          YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_REGISTER_UAM_RETRY_COUNT);
    }

    this.registerUamRetryInterval = conf.getTimeDuration(
        YarnConfiguration.FEDERATION_AMRMPROXY_REGISTER_UAM_RETRY_INTERVAL,
        YarnConfiguration.DEFAULT_FEDERATION_AMRMPROXY_REGISTER_UAM_RETRY_INTERVAL,
        TimeUnit.MILLISECONDS);

    this.waitUamRegisterDone = conf.getBoolean(YarnConfiguration.AMRM_PROXY_WAIT_UAM_REGISTER_DONE,
        YarnConfiguration.DEFAULT_AMRM_PROXY_WAIT_UAM_REGISTER_DONE);
  }

  @Override
  public void recover(Map<String, byte[]> recoveredDataMap) {
    super.recover(recoveredDataMap);
    LOG.info("Recovering data for FederationInterceptor for {}.", this.attemptId);
    // 标记刚完成恢复，后续触发AM重注册
    this.justRecovered = true;

    if (recoveredDataMap == null || recoveredDataMap.isEmpty()) {
      LOG.warn("recoveredDataMap isNull Or isEmpty, FederationInterceptor can't recover.");
      return;
    }