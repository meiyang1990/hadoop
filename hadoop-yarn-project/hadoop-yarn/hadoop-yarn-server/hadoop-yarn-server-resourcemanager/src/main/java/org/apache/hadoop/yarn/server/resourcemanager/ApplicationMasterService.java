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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.AbstractPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.DisabledPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.PlacementConstraintProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.SchedulerPlacementProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * ApplicationMaster服务实现，处理ApplicationMaster向ResourceManager注册、心跳、资源请求、完成注销等核心RPC请求，
 * 负责维护AM生命周期和分配资源，通过处理器链扩展支持调度约束等功能
 */
@SuppressWarnings("unchecked")
@Private
public class ApplicationMasterService extends AbstractService implements
    ApplicationMasterProtocol {
  private static final Logger LOG = LoggerFactory.
      getLogger(ApplicationMasterService.class);

  // AM存活性监控器
  private final AMLivelinessMonitor amLivelinessMonitor;
  // YARN调度器引用
  private YarnScheduler rScheduler;
  // 服务监听地址
  protected InetSocketAddress masterServiceAddress;
  // RPC服务端实例
  protected Server server;
  // 记录工厂，用于创建API记录对象
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  // 存储每个应用尝试的分配响应锁，保证并发注册/分配操作线程安全
  private final ConcurrentMap<ApplicationAttemptId, AllocateResponseLock> responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AllocateResponseLock>();
  // 缓存已完成的应用尝试，防止重复处理完成请求
  private final ConcurrentHashMap<ApplicationAttemptId, Boolean>
      finishedAttemptCache = new ConcurrentHashMap<>();
  // RM上下文对象
  protected final RMContext rmContext;
  // AM请求处理链，支持多处理器扩展处理AM请求
  private final AMSProcessingChain amsProcessingChain;
  // 是否开启Timeline Service V2
  private boolean timelineServiceV2Enabled;

  /**
   * 构造ApplicationMasterService实例
   * @param rmContext RM上下文
   * @param scheduler 调度器
   */
  public ApplicationMasterService(RMContext rmContext,
      YarnScheduler scheduler) {
    this(ApplicationMasterService.class.getName(), rmContext, scheduler);
  }

  /**
   * 构造ApplicationMasterService实例
   * @param name 服务名称
   * @param rmContext RM上下文
   * @param scheduler 调度器
   */
  public ApplicationMasterService(String name, RMContext rmContext,
      YarnScheduler scheduler) {
    super(name);
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.rScheduler = scheduler;
    this.rmContext = rmContext;
    this.amsProcessingChain = new AMSProcessingChain(new DefaultAMSProcessor());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置读取服务绑定地址
    masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    // 初始化AM请求处理链
    initializeProcessingChain(conf);
  }

  /**
   * 根据配置添加位置约束处理器
   * @param conf 配置对象
   */
  private void addPlacementConstraintHandler(Configuration conf) {
    String placementConstraintsHandler =
        conf.get(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
            YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    if (placementConstraintsHandler
        .equals(YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      LOG.info(YarnConfiguration.DISABLED_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used, all scheduling requests will "
          + "be rejected.");
      amsProcessingChain.addProcessor(new DisabledPlacementProcessor());
    } else if (placementConstraintsHandler
        .equals(YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      LOG.info(YarnConfiguration.PROCESSOR_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used. Scheduling requests will be "
          + "handled by the placement constraint processor");
      amsProcessingChain.addProcessor(new PlacementConstraintProcessor());
    } else if (placementConstraintsHandler
        .equals(YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER)) {
      LOG.info(YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " placement handler will be used. Scheduling requests will be "
          + "handled by the main scheduler.");
      amsProcessingChain.addProcessor(new SchedulerPlacementProcessor());
    }
  }

  /**
   * 初始化AM请求处理链，加载配置的所有处理器
   * @param conf 配置对象
   */
  private void initializeProcessingChain(Configuration conf) {
    amsProcessingChain.init(rmContext, null);
    addPlacementConstraintHandler(conf);

    List<ApplicationMasterServiceProcessor> processors = getProcessorList(conf);
    if (processors != null) {
      Collections.reverse(processors);
      for (ApplicationMasterServiceProcessor p : processors) {
        // 确保只存在一个位置处理器实例，忽略配置文件中额外添加的位置处理器
        if (p instanceof AbstractPlacementProcessor) {
          LOG.warn("Found PlacementProcessor=" + p.getClass().getCanonicalName()
              + " defined in "
              + YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS
              + ", however PlacementProcessor handler should be configured "
              + "by using " + YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER
              + ", this processor will be ignored.");
          continue;
        }
        this.amsProcessingChain.addProcessor(p);
      }
    }
  }

  /**
   * 从配置中加载自定义AM处理器列表
   * @param conf 配置对象
   * @return 处理器列表
   */
  protected List<ApplicationMasterServiceProcessor> getProcessorList(
      Configuration conf) {
    return conf.getInstances(
        YarnConfiguration.RM_APPLICATION_MASTER_SERVICE_PROCESSORS,
        ApplicationMasterServiceProcessor.class);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    Configuration serverConf = conf;
    // 强制启用令牌认证，非简单认证模式下
    serverConf = new Configuration(conf);
    serverConf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
    this.server = getServer(rpc, serverConf, masterServiceAddress,
        this.rmContext.getAMRMTokenSecretManager());
    // 添加简洁异常，减少客户端不必要的栈信息
    this.server.addTerseExceptions(
        ApplicationMasterNotRegisteredException.class);

    // 启用服务权限认证
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.server.start();
    // 更新实际绑定地址，处理动态端口分配
    this.masterServiceAddress =
        conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
                               YarnConfiguration.RM_SCHEDULER_ADDRESS,
                               YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                               server.getListenerAddress());
    this.timelineServiceV2Enabled = YarnConfiguration.
        timelineServiceV2Enabled(conf);

    super.serviceStart();
  }

  /**
   * 创建RPC服务端实例
   * @param rpc RPC工厂
   * @param serverConf 服务配置
   * @param addr 绑定地址
   * @param secretManager AMRM令牌秘钥管理器
   * @return RPC服务端实例
   */
  protected Server getServer(YarnRPC rpc, Configuration serverConf,
      InetSocketAddress addr, AMRMTokenSecretManager secretManager) {
    return rpc.getServer(ApplicationMasterProtocol.class, this, addr,
        serverConf, secretManager,
        serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
  }

  /**
   * 获取AM请求处理链
   * @return 处理链实例
   */
  protected AMSProcessingChain getProcessingChain() {
    return this.amsProcessingChain;
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.masterServiceAddress;
  }

  @Override
  /**
   * 处理ApplicationMaster注册请求
   */
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {

    // 从RPC上下文获取AMRM令牌，完成请求认证
    AMRMTokenIdentifier amrmTokenIdentifier =
        YarnServerSecurityUtils.authorizeRequest();
    ApplicationAttemptId applicationAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();

    ApplicationId appID = applicationAttemptId.getApplicationId();
    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      // 缓存不存在该应用尝试，记录审计日志并抛出异常
      RMAuditLogger.logFailure(this.rmContext.getRMApps().get(appID).getUser(),
          AuditConstants.REGISTER_AM, "Application doesn't exist in cache "
              + applicationAttemptId, "ApplicationMasterService",
          "Error in registering application master", appID,
          applicationAttemptId);
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // 同一AM的注册操作串行执行，保证线程安全
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (hasApplicationMasterRegistered(applicationAttemptId)) {
        // 仅非UAM或未开启容器保留时，拒绝重复注册
        ApplicationSubmissionContext appContext =
            rmContext.getRMApps().get(appID).getApplicationSubmissionContext();
        if (!(appContext.getUnmanagedAM()
            && appContext.getKeepContainersAcrossApplicationAttempts())) {
          String message =
              AMRMClientUtils.APP_ALREADY_REGISTERED_MESSAGE + appID;
          LOG.warn(message);
          RMAuditLogger.logFailure(
              this.rmContext.getRMApps().get(appID).getUser(),
              AuditConstants.REGISTER_AM, "", "ApplicationMasterService",
              message, appID, applicationAttemptId);
          throw new InvalidApplicationMasterRequestException(message);
        }
      }

      // 更新存活性监控，标记AM存活
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      // 设置初始响应ID为0，表示已注册
      lastResponse.setResponseId(0);
      lock.setAllocateResponse(lastResponse);

      RegisterApplicationMasterResponse response =
          recordFactory.newRecordInstance(
              RegisterApplicationMasterResponse.class);
      // 通过处理链处理注册请求
      this.amsProcessingChain.registerApplicationMaster(
          amrmTokenIdentifier.getApplicationAttemptId(), request, response);
      return response;
    }
  }

  @Override
  /**
   * 处理ApplicationMaster注销完成请求
   */
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {

    // 认证获取应用尝试ID
    ApplicationAttemptId applicationAttemptId =
        YarnServerSecurityUtils.authorizeRequest().getApplicationAttemptId();
    ApplicationId appId = applicationAttemptId.getApplicationId();

    RMApp rmApp =
        rmContext.getRMApps().get(applicationAttemptId.getApplicationId());

    // 开启Timeline V2时，清除应用收集器地址信息
    if (timelineServiceV2Enabled) {
      ((RMAppImpl) rmApp).removeCollectorData();
    }
    // 若应用最终状态已存储，直接返回成功，避免RM重启后重复报错
    if (rmApp.isAppFinalStateStored()) {
      LOG.info(rmApp.getApplicationId() + " unregistered successfully. ");
      return FinishApplicationMasterResponse.newInstance(true);
    }

    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // 同一AM的完成操作串行执行
    synchronized (lock) {
      if (!hasApplicationMasterRegistered(applicationAttemptId)) {
        String message =
            "Application Master is trying to unregister before registering for: "
                + appId;
        LOG.error(message);
        RMAuditLogger.logFailure(
            this.rmContext.getRMApps()
                .get(appId).getUser(),
            AuditConstants.UNREGISTER_AM, "", "ApplicationMasterService",
            message, appId,
            applicationAttemptId);
        throw new ApplicationMasterNotRegisteredException(message);
      }

      FinishApplicationMasterResponse response =
          FinishApplicationMasterResponse.newInstance(false);
      // 仅处理一次完成请求，幂等处理
      if (finishedAttemptCache.putIfAbsent(applicationAttemptId, true)
          == null) {
        this.amsProcessingChain
            .finishApplicationMaster(applicationAttemptId, request, response);
      }
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);
      return response;
    }