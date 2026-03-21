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

package org.apache.hadoop.yarn.server.uam;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.AMHeartbeatRequestHandler;
import org.apache.hadoop.yarn.server.AMRMClientRelayer;
import org.apache.hadoop.yarn.util.AsyncCallback;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 非托管ApplicationMaster，用于向ResourceManager注册非托管应用并申请资源。
 * 非托管AM指不由RM启动和管理的AM，allocate调用通过{@link AsyncCallback}异步处理。
 */
@Public
@Unstable
public class UnmanagedApplicationManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(UnmanagedApplicationManager.class);
  // AM状态等待超时时间，单位毫秒
  private static final long AM_STATE_WAIT_TIMEOUT_MS = 10000;
  public static final String APP_NAME = "UnmanagedAM";
  // 默认队列配置项名称
  private static final String DEFAULT_QUEUE_CONFIG = "uam.default.queue.name";

  // AM心跳请求处理器
  private AMHeartbeatRequestHandler heartbeatHandler;
  // AM-RM客户端代理中继器
  private AMRMClientRelayer rmProxyRelayer;
  // 当前UAM所属应用ID
  private ApplicationId applicationId;
  // 应用提交者用户名
  private String submitter;
  // 应用名称后缀
  private String appNameSuffix;
  // YARN配置对象
  private Configuration conf;
  // 应用提交队列名称
  private String queueName;
  // 当前UAM运行用户UGI
  private UserGroupInformation userUgi;
  // AM注册请求，用于后续重注册
  private RegisterApplicationMasterRequest registerRequest;
  // RM客户端协议代理
  private ApplicationClientProtocol rmClient;
  // 异步API轮询间隔，单位毫秒
  private long asyncApiPollIntervalMillis;
  // YARN记录工厂，用于创建记录对象
  private RecordFactory recordFactory;
  // 跨应用尝试保留容器标记，用于UAM恢复
  private boolean keepContainersAcrossApplicationAttempts;
  // 原始应用提交上下文，用于复制自定义配置
  private ApplicationSubmissionContext applicationSubmissionContext;

  /*
   * 该标记用于表示launchUAM/reAttachUAM已经被调用，可能在initializeUnmanagedAM中因为RM连接/故障切换问题阻塞尚未完成。
   * 在调用RM阻塞方法前设置该标记。
   */
  // 连接初始化已发起标记
  private boolean connectionInitiated;

  /**
   * 构造非托管ApplicationManager实例。
   *
   * @param conf YARN配置
   * @param appId 当前UAM对应的应用ID
   * @param queueName UAM提交队列
   * @param submitter 应用提交用户名
   * @param appNameSuffix 应用名称后缀
   * @param keepContainersAcrossApplicationAttempts 跨应用尝试保留容器标记，用于UAM恢复
   * @param rmName YARN ResourceManager名称
   * @param originalApplicationSubmissionContext 原始应用提交上下文
   */
  public UnmanagedApplicationManager(Configuration conf, ApplicationId appId,
      String queueName, String submitter, String appNameSuffix,
      boolean keepContainersAcrossApplicationAttempts, String rmName,
      ApplicationSubmissionContext originalApplicationSubmissionContext) {
    Preconditions.checkNotNull(conf, "Configuration cannot be null");
    Preconditions.checkNotNull(appId, "ApplicationId cannot be null");
    Preconditions.checkNotNull(submitter, "App submitter cannot be null");

    this.conf = conf;
    this.applicationId = appId;
    this.queueName = queueName;
    this.submitter = submitter;
    this.appNameSuffix = appNameSuffix;
    this.userUgi = null;
    // 中继器的RM客户端会在创建RM连接后设置
    this.rmProxyRelayer =
        new AMRMClientRelayer(null, this.applicationId, rmName, this.conf);
    this.heartbeatHandler = createAMHeartbeatRequestHandler(this.conf,
        this.applicationId, this.rmProxyRelayer);

    this.connectionInitiated = false;
    this.registerRequest = null;
    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);
    this.asyncApiPollIntervalMillis = conf.getLong(
        YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS,
        YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
    this.keepContainersAcrossApplicationAttempts =
        keepContainersAcrossApplicationAttempts;
    this.applicationSubmissionContext = originalApplicationSubmissionContext;
  }

  @VisibleForTesting
  protected AMHeartbeatRequestHandler createAMHeartbeatRequestHandler(
      Configuration config, ApplicationId appId,
      AMRMClientRelayer relayer) {
    return new AMHeartbeatRequestHandler(config, appId, relayer);
  }

  /**
   * 在ResourceManager中启动一个新的UAM。
   *
   * @return UAM的AM-RM身份令牌
   * @throws YarnException 操作失败抛出异常
   * @throws IOEException IO操作失败抛出异常
   */
  public Token<AMRMTokenIdentifier> launchUAM()
      throws YarnException, IOException {
    this.connectionInitiated = true;

    // 阻塞调用RM初始化UAM
    Token<AMRMTokenIdentifier> amrmToken = initializeUnmanagedAM(this.applicationId);

    // 创建UAM到RM的连接代理
    createUAMProxy(amrmToken);
    return amrmToken;
  }

  /**
   * 重新附加到ResourceManager中已存在的UAM。
   *
   * @param amrmToken 已有UAM的AM-RM身份令牌
   * @throws IOException 重新附加失败抛出异常
   * @throws YarnException 重新附加失败抛出异常
   */
  public void reAttachUAM(Token<AMRMTokenIdentifier> amrmToken)
      throws IOException, YarnException {
    this.connectionInitiated = true;

    // 创建UAM到RM的连接代理
    createUAMProxy(amrmToken);
  }

  protected void createUAMProxy(Token<AMRMTokenIdentifier> amrmToken)
      throws IOException {
    // 创建代理用户UGI，以应用身份访问RM
    this.userUgi = UserGroupInformation.createProxyUser(
        this.applicationId.toString(), UserGroupInformation.getCurrentUser());
    // 设置RM协议代理
    this.rmProxyRelayer.setRMClient(createRMProxy(
        ApplicationMasterProtocol.class, this.conf, this.userUgi, amrmToken));
    // 设置心跳处理器的UGI
    this.heartbeatHandler.setUGI(this.userUgi);
  }

  /**
   * 向ResourceManager注册当前UnmanagedApplicationManager。
   *
   * @param request AM注册请求
   * @return 注册响应
   * @throws YarnException 注册失败抛出异常
   * @throws IOException 注册失败抛出异常
   */
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException, IOException {

    // 保存注册请求供后续重注册使用
    this.registerRequest = request;

    LOG.info("Registering the Unmanaged application master {}",
        this.applicationId);
    // 通过中继器向RM发起注册
    RegisterApplicationMasterResponse response =
        this.rmProxyRelayer.registerApplicationMaster(this.registerRequest);
    // 重置心跳处理器的最后响应ID
    this.heartbeatHandler.resetLastResponseId();

    if (LOG.isDebugEnabled()) {
      // 打印前一次尝试保留下来的容器
      for (Container container : response.getContainersFromPreviousAttempts()) {
        LOG.debug("RegisterUAM returned existing running container {}", container.getId());
      }

      // 打印前一次尝试保留下来的NM令牌
      for (NMToken nmToken : response.getNMTokensFromPreviousAttempts()) {
        LOG.debug("RegisterUAM returned existing NM token for node {}", nmToken.getNodeId());
      }
    }

    LOG.info("RegisterUAM returned {} existing running container and {} NM tokens",
        response.getContainersFromPreviousAttempts().size(),
        response.getNMTokensFromPreviousAttempts().size());

    // 注册成功后才启动心跳线程
    this.heartbeatHandler.setDaemon(true);
    this.heartbeatHandler.start();

    return response;
  }

  /**
   * 向ResourceManager注销，停止请求处理线程。
   *
   * @param request 结束AM请求
   * @return 结束AM响应
   * @throws YarnException 结束AM调用失败抛出异常
   * @throws IOException 结束AM调用失败抛出异常
   */
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException, IOException {

    if (this.userUgi == null) {
      if (this.connectionInitiated) {
        // 这种情况可能是异步launchUAM仍在阻塞重试，直接返回虚拟响应并停止心跳线程
        LOG.warn("Unmanaged AM still not successfully launched/registered yet."
            + " Stopping the UAM heartbeat thread anyways.");
        return FinishApplicationMasterResponse.newInstance(false);
      } else {
        throw new YarnException("finishApplicationMaster should not "
            + "be called before createAndRegister");
      }
    }
    FinishApplicationMasterResponse response =
        this.rmProxyRelayer.finishApplicationMaster(request);
    if (response.getIsUnregistered()) {
      shutDownConnections();
    }
    return response;
  }

  /**
   * 强制杀死当前UAM应用。
   *
   * @return 杀死应用响应
   * @throws IOException 创建RM代理失败抛出异常
   * @throws YarnException 强制杀死失败抛出异常
   */
  public KillApplicationResponse forceKillApplication()
      throws IOException, YarnException {
    // 关闭本地连接
    shutDownConnections();

    KillApplicationRequest request =
        KillApplicationRequest.newInstance(this.applicationId);
    if (this.rmClient == null) {
      // 创建应用客户端RM代理，使用提交者身份
      this.rmClient = createRMProxy(ApplicationClientProtocol.class, this.conf,
          UserGroupInformation.createRemoteUser(this.submitter), null);
    }
    return this.rmClient.forceKillApplication(request);
  }

  /**
   * 发送分配请求给ResourceManager，异步通过回调返回结果。
   *
   * @param request 分配请求
   * @param callback 结果回调
   * @throws YarnException AM未注册时抛出异常
   */
  public void allocateAsync(AllocateRequest request,
      AsyncCallback<AllocateResponse> callback) throws YarnException {
    this.heartbeatHandler.allocateAsync(request, callback);

    // 两种情况UAM还未注册成功：
    // 1. launchUAM根本没调用，这里直接抛出异常
    // 2. launchUAM已调用但还未成功返回
    // 第二种情况下请求已经保存在队列中，注册成功后会自动发送，不会丢失请求
    if (this.userUgi == null) {
      if (this.connectionInitiated) {
        LOG.info("Unmanaged AM still not successfully launched/registered yet."
            + " Saving the allocate request and send later.");
      } else {
        throw new YarnException("AllocateAsync should not be called before launchUAM");
      }
    }
  }

  /**
   * 关闭本地UAM客户端连接，不会杀死RM端的UAM应用。
   */
  public void shutDownConnections() {
    this.heartbeatHandler.shutdown();
    this.rmProxyRelayer.shutdown();
  }

  /**
   * 获取当前UAM对应的应用ID。
   *
   * @return 当前UAM应用ID
   */
  public ApplicationId getAppId() {
    return this.applicationId;
  }

  /**
   * 获取当前UAM的RM代理中继器。
   *
   * @return 当前UAM的AMRMClientRelayer
   */
  public AMRMClientRelayer getAMRMClientRelayer() {
    return this.rmProxyRelayer;
  }

  /**
   * 创建指定协议类型的RM代理。单元测试可以覆盖该方法返回Mock代理。
   *
   * @param protocol 代理协议类型
   * @param config YARN配置
   * @param user 连接使用的用户UGI
   * @param token 连接身份令牌
   * @param <T> 代理类型泛型
   * @return RM代理实例
   * @throws IOException 创建代理失败抛出异常
   */
  protected <T> T createRMProxy(Class<T> protocol, Configuration config,
      UserGroupInformation user, Token<AMRMTokenIdentifier> token)
      throws IOException {
    return AMRMClientUtils.createRMProxy(config, protocol, user, token);
  }

  /**
   * 启动并初始化非托管AM。首先在RM上创建新应用，协商得到尝试ID，然后等待RM应用尝试状态变为LAUNCHED，
   * 之后返回AM-RM身份令牌。
   *
   * @param appId 应用ID
   * @return UAM身份令牌
   * @throws IOException 初始化失败抛出异常
   * @throws YarnException 初始化失败抛出异常
   */
  protected Token<AMRMTokenIdentifier> initializeUnmanagedAM(
      ApplicationId appId) throws IOException, YarnException {
    try {
      UserGroupInformation appSubmitter;
      // 根据安全状态创建提交者代理用户
      if (UserGroupInformation.isSecurityEnabled()) {
        appSubmitter = UserGroupInformation.createProxyUser(this.submitter,
            UserGroupInformation.getLoginUser());
      } else {
        appSubmitter = UserGroupInformation.createRemoteUser(this.submitter);
      }
      // 创建应用客户端协议RM代理
      this.rmClient = createRMProxy(ApplicationClientProtocol.class, this.conf,
          appSubmitter, null);

      // 提交非托管应用到RM
      submitUnmanagedApp(appId);

      // 监控应用尝试状态直到达到LAUNCHED
      monitorCurrentAppAttempt(appId,
          EnumSet.of(YarnApplicationState.ACCEPTED,
              YarnApplicationState.RUNNING, YarnApplicationState.KILLED,
              YarnApplicationState.FAILED, Y