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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredAMRMProxyState;
import org.apache.hadoop.yarn.server.nodemanager.scheduler.DistributedScheduler;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize
    .NMPolicyProvider;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.Preconditions;

/**
 * AMRMProxyService是运行在NodeManager上的代理服务，用于拦截和检查ApplicationMaster发送给ResourceManager的请求
 * 为每个应用创建独立的请求拦截处理管道，管道中的拦截器可以按需检查和修改请求/响应内容
 */
public class AMRMProxyService extends CompositeService implements
    ApplicationMasterProtocol {
  private static final Logger LOG = LoggerFactory
      .getLogger(AMRMProxyService.class);

  private static final String NMSS_USER_KEY = "user";
  private static final String NMSS_AMRMTOKEN_KEY = "amrmtoken";

  private final Clock clock = new MonotonicClock();
  private Server server;
  private final Context nmContext;
  private final AsyncDispatcher dispatcher;
  private InetSocketAddress listenerEndpoint;
  private AMRMProxyTokenSecretManager secretManager;
  private Map<ApplicationId, RequestInterceptorChainWrapper> applPipelineMap;
  private RegistryOperations registry;
  private AMRMProxyMetrics metrics;
  private FederationStateStoreFacade federationFacade;
  private boolean federationEnabled = false;

  /**
   * 构造AMRMProxy服务实例
   *
   * @param nmContext NodeManager上下文
   * @param dispatcher NodeManager事件分发器
   */
  public AMRMProxyService(Context nmContext, AsyncDispatcher dispatcher) {
    super(AMRMProxyService.class.getName());
    Preconditions.checkArgument(nmContext != null, "nmContext is null");
    Preconditions.checkArgument(dispatcher != null, "dispatcher is null");
    this.nmContext = nmContext;
    this.dispatcher = dispatcher;
    this.applPipelineMap = new ConcurrentHashMap<>();

    this.dispatcher.register(ApplicationEventType.class, new ApplicationEventHandler());
    metrics = AMRMProxyMetrics.getMetrics();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 初始化AMRMToken密钥管理器
    this.secretManager =
        new AMRMProxyTokenSecretManager(this.nmContext.getNMStateStore());
    this.secretManager.init(conf);

    // 如果启用AMRMProxy高可用，初始化服务注册中心
    if (conf.getBoolean(YarnConfiguration.AMRM_PROXY_HA_ENABLED,
        YarnConfiguration.DEFAULT_AMRM_PROXY_HA_ENABLED)) {
      this.registry = FederationStateStoreFacade.createInstance(conf,
          YarnConfiguration.YARN_REGISTRY_CLASS,
          YarnConfiguration.DEFAULT_YARN_REGISTRY_CLASS,
          RegistryOperations.class);
      addService(this.registry);
    }
    // 获取联邦状态存储门面，读取联邦配置
    this.federationFacade = FederationStateStoreFacade.getInstance(conf);
    this.federationEnabled =
        conf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
            YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting AMRMProxyService.");
    Configuration conf = getConfig();
    // 创建YARN RPC实例
    YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation.setConfiguration(conf);

    // 解析绑定地址配置
    this.listenerEndpoint =
        conf.getSocketAddr(YarnConfiguration.AMRM_PROXY_ADDRESS,
            YarnConfiguration.DEFAULT_AMRM_PROXY_ADDRESS,
            YarnConfiguration.DEFAULT_AMRM_PROXY_PORT);

    // 创建服务端配置，启用Token认证
    Configuration serverConf = new Configuration(conf);
    serverConf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());

    // 获取工作线程数配置
    int numWorkerThreads =
        serverConf.getInt(
            YarnConfiguration.AMRM_PROXY_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_AMRM_PROXY_CLIENT_THREAD_COUNT);

    // 启动AMRMToken密钥管理器
    this.secretManager.start();

    // 创建并启动RPC服务端
    this.server =
        rpc.getServer(ApplicationMasterProtocol.class, this,
            listenerEndpoint, serverConf, this.secretManager,
            numWorkerThreads);

    // 如果启用授权，刷新服务访问控制列表
    if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      this.server.refreshServiceAcl(conf, NMPolicyProvider.getInstance());
    }

    this.server.start();
    LOG.info("AMRMProxyService listening on address: {}.", this.server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping AMRMProxyService.");
    if (this.server != null) {
      this.server.stop();
    }
    this.secretManager.stop();
    super.serviceStop();
  }

  /**
   * 从NodeManager状态存储恢复AMRMProxy状态，在serviceInit之后serviceStart之前调用
   *
   * @throws IOException 恢复失败时抛出
   */
  public void recover() throws IOException {
    LOG.info("Recovering AMRMProxyService.");

    // 从状态存储加载恢复数据
    RecoveredAMRMProxyState state =
        this.nmContext.getNMStateStore().loadAMRMProxyState();

    // 恢复密钥管理器状态
    this.secretManager.recover(state);

    LOG.info("Recovering {} running applications for AMRMProxy.",
        state.getAppContexts().size());

    // 遍历恢复每个应用尝试的上下文
    for (Map.Entry<ApplicationAttemptId, Map<String, byte[]>> entry : state
        .getAppContexts().entrySet()) {
      ApplicationAttemptId attemptId = entry.getKey();
      LOG.info("Recovering app attempt {}.", attemptId);
      long startTime = clock.getTime();

      // 尝试恢复运行中的应用尝试
      try {
        String user = null;
        Token<AMRMTokenIdentifier> amrmToken = null;
        // 解析恢复数据中的用户名和AMRMToken
        for (Map.Entry<String, byte[]> contextEntry : entry.getValue()
            .entrySet()) {
          if (contextEntry.getKey().equals(NMSS_USER_KEY)) {
            user = new String(contextEntry.getValue(), StandardCharsets.UTF_8);
          } else if (contextEntry.getKey().equals(NMSS_AMRMTOKEN_KEY)) {
            amrmToken = new Token<>();
            amrmToken.decodeFromUrlString(
                new String(contextEntry.getValue(), StandardCharsets.UTF_8));
            // 清空服务字段，模拟RM刚签发Token的状态
            amrmToken.setService(new Text());
          }
        }

        if (amrmToken == null) {
          throw new IOException("No amrmToken found for app attempt " + attemptId);
        }
        if (user == null) {
          throw new IOException("No user found for app attempt " + attemptId);
        }

        // 重新生成本地AMRMToken
        Token<AMRMTokenIdentifier> localToken =
            this.secretManager.createAndGetAMRMToken(attemptId);

        // 从NodeManager上下文中获取AM容器凭证
        Credentials amCred = null;
        for (Container container : this.nmContext.getContainers().values()) {
          LOG.debug("From NM Context container {}.", container.getContainerId());
          if (container.getContainerId().getApplicationAttemptId().equals(
              attemptId) && container.getContainerTokenIdentifier() != null) {
            LOG.debug("Container type {}.",
                container.getContainerTokenIdentifier().getContainerType());
            if (container.getContainerTokenIdentifier()
                .getContainerType() == ContainerType.APPLICATION_MASTER) {
              LOG.info("AM container {} found in context, has credentials: {}.",
                  container.getContainerId(),
                  (container.getCredentials() != null));
              amCred = container.getCredentials();
            }
          }
        }
        if (amCred == null) {
          LOG.error("No credentials found for AM container of {}. "
              + "Yarn registry access might not work.", attemptId);
        }

        // 为应用创建拦截管道
        initializePipeline(attemptId, user, amrmToken, localToken,
            entry.getValue(), true, amCred);
        long endTime = clock.getTime();
        this.metrics.succeededRecoverRequests(endTime - startTime);
      } catch (Throwable e) {
        LOG.error("Exception when recovering {}, removing it from NMStateStore and move on.",
            attemptId, e);
        this.metrics.incrFailedAppRecoveryCount();
        this.nmContext.getNMStateStore().removeAMRMProxyAppContext(attemptId);
      }
    }
  }

  /**
   * 处理ApplicationMaster向ResourceManager的注册请求，完成初始认证后转发到应用专属拦截链处理
   */
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    this.metrics.incrRequestCount();
    long startTime = clock.getTime();
    try {
      // 认证并获取拦截管道
      RequestInterceptorChainWrapper pipeline =
          authorizeAndGetInterceptorChain();

      LOG.info("RegisteringAM Host: {}, Port: {}, Tracking Url: {} for application {}. ",
          request.getHost(), request.getRpcPort(), request.getTrackingUrl(),
          pipeline.getApplicationAttemptId());

      // 调用拦截链处理注册请求
      RegisterApplicationMasterResponse response =
          pipeline.getRootInterceptor().registerApplicationMaster(request);

      long endTime = clock.getTime();
      this.metrics.succeededRegisterAMRequests(endTime - startTime);
      LOG.info("RegisterAM processing finished in {} ms for application {}.",
          endTime - startTime, pipeline.getApplicationAttemptId());
      return response;
    } catch (Throwable t) {
      this.metrics.incrFailedRegisterAMRequests();
      throw t;
    }
  }

  /**
   * 处理ApplicationMaster向ResourceManager的取消注册请求，完成初始认证后转发到应用专属拦截链处理
   */
  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    this.metrics.incrRequestCount();
    long startTime = clock.getTime();
    try {
      RequestInterceptorChainWrapper pipeline =
          authorizeAndGetInterceptorChain();
      LOG.info("Finishing application master for {}. Tracking Url: {}.",
          pipeline.getApplicationAttemptId(), request.getTrackingUrl());
      FinishApplicationMasterResponse response =
          pipeline.getRootInterceptor().finishApplicationMaster(request);

      long endTime = clock.getTime();
      this.metrics.succeededFinishAMRequests(endTime - startTime);
      LOG.info("FinishAM finished with isUnregistered = {} in {} ms for {}.",
          response.getIsUnregistered(), endTime - startTime,
          pipeline.getApplicationAttemptId());
      return response;
    } catch (Throwable t) {
      this.metrics.incrFailedFinishAMRequests();
      throw t;
    }
  }

  /**
   * 处理ApplicationMaster向ResourceManager的心跳分配请求，完成初始认证后转发到应用专属拦截链处理
   * 每个AM实例对应一个请求处理管道
   */
  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {
    this.metrics.incrAllocateCount();
    long startTime = clock.getTime();
    try {
      AMRMTokenIdentifier amrmTokenIdentifier =
          YarnServerSecurityUtils.authorizeRequest();
      // 获取该应用的拦截管道
      RequestInterceptorChainWrapper pipeline =
          getInterceptorChain(amrmTokenIdentifier);
      // 调用拦截链处理分配请求
      AllocateResponse allocateResponse =
          pipeline.getRootInterceptor().allocate(request);

      // 更新AMRMToken
      updateAMRMTokens(amrmTokenIdentifier, pipeline, allocateResponse);

      long endTime = clock.getTime();
      this.metrics.succeededAllocateRequests(endTime - startTime);
      LOG.info("Allocate processing finished in {} ms for application {}.",
          endTime - startTime, pipeline.getApplicationAttemptId());
      return allocateResponse;
    } catch (Throwable t) {
      this.metrics.incrFailedAllocateRequests();
      throw t;
    }
  }

  /**
   * 容器管理器启动AM时的回调，用于初始化应用请求处理管道
   *
   * @param request 启动容器请求，包含AM启动信息
   * @throws IOException 处理失败时抛出
   * @throws YarnException 处理失败时抛出
   */
  public void processApplicationStartRequest(StartContainerRequest request)
      throws IOException, YarnException {
    this.metrics.incrRequestCount();
    long startTime = clock.getTime();
    try {
      // 从容器Token中解析标识信息
      ContainerTokenIdentifier containerTokenIdentifierForKey =
          BuilderUtils.newContainerTokenIdentifier(request.getContainerToken());
      ApplicationAttemptId appAttemptId =
          containerTokenIdentifierForKey.getContainerID()
              .getApplicationAttemptId();
      ApplicationId applicationID = appAttemptId.getApplicationId();
      // 仅在联邦启用时检查应用是否存在于联邦状态存储，不存在则跳过处理
      if (!checkIfAppExistsInStateStore(applicationID)) {
        return;
      }
      LOG.info("Callback received for initializing request processing pipeline for an AM.");
      // 解析凭证信息
      Credentials credentials = YarnServerSecurityUtils
          .parseCredentials(request.getContainerLaunchContext());

      // 从凭证中提取RM签发的AMRMToken
      Token<