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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.reader.TimelineDomainReader;
import org.apache.hadoop.yarn.api.records.timelineservice.reader.TimelineEntitiesReader;
import org.apache.hadoop.yarn.api.records.timelineservice.writer.TimelineEntitiesWriter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;
import org.apache.hadoop.yarn.server.timelineservice.security.TimelineV2DelegationTokenSecretManagerService;
import org.apache.hadoop.yarn.server.util.timeline.TimelineServerUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * NodeManager侧的时间线采集器管理器，负责管理采集器的添加、移除和生命周期，同时启动节点级采集器Web服务。
 */
@Private
@Unstable
public class NodeTimelineCollectorManager extends TimelineCollectorManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(NodeTimelineCollectorManager.class);

  // 当前采集器管理器的REST服务实例
  private HttpServer2 timelineRestServer;

  // REST服务绑定地址
  private String timelineRestServerBindAddress;

  // NodeManager采集器服务代理对象
  private volatile CollectorNodemanagerProtocol nmCollectorService;

  // Timeline V2 委托令牌密钥管理服务
  private TimelineV2DelegationTokenSecretManagerService tokenMgrService;

  // 标记是否作为NodeManager辅助服务运行
  private final boolean runningAsAuxService;

  // 登录用户信息
  private UserGroupInformation loginUGI;

  // 令牌续期定时任务执行器
  private ScheduledThreadPoolExecutor tokenRenewalExecutor;

  // 令牌续期间隔
  private long tokenRenewInterval;

  // 令牌提前10秒进行续期
  private static final long TIME_BEFORE_RENEW_DATE = 10 * 1000; // 10 seconds.

  // 令牌提前5分钟重新生成
  private static final long TIME_BEFORE_EXPIRY = 5 * 60 * 1000; // 5 minutes.

  static final String COLLECTOR_MANAGER_ATTR_KEY = "collector.manager";

  @VisibleForTesting
  protected NodeTimelineCollectorManager() {
    this(true);
  }

  protected NodeTimelineCollectorManager(boolean asAuxService) {
    super(NodeTimelineCollectorManager.class.getName());
    this.runningAsAuxService = asAuxService;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 创建令牌管理服务
    tokenMgrService = createTokenManagerService();
    // 添加服务到服务框架
    addService(tokenMgrService);
    // 获取当前登录用户
    this.loginUGI = UserGroupInformation.getCurrentUser();
    // 从配置读取令牌续期间隔
    tokenRenewInterval = conf.getLong(
        YarnConfiguration.TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL,
        YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 安全模式下处理登录
    if (UserGroupInformation.isSecurityEnabled()) {
      // 非辅助服务运行模式下需要自行完成安全登录
      if (!runningAsAuxService) {
        try {
          doSecureLogin();
        } catch(IOException ie) {
          throw new YarnRuntimeException("Failed to login", ie);
        }
      }
      this.loginUGI = UserGroupInformation.getLoginUser();
    }
    // 创建令牌续期定时线程池
    tokenRenewalExecutor = new ScheduledThreadPoolExecutor(
        1, new ThreadFactoryBuilder().setNameFormat(
            "App Collector Token Renewal thread").build());
    super.serviceStart();
    // 启动REST Web服务
    startWebApp();
  }

  /**
   * 创建令牌管理服务实例。
   * @return 令牌管理服务实例
   */
  protected TimelineV2DelegationTokenSecretManagerService
      createTokenManagerService() {
    return new TimelineV2DelegationTokenSecretManagerService();
  }

  @VisibleForTesting
  public TimelineV2DelegationTokenSecretManagerService
      getTokenManagerService() {
    return tokenMgrService;
  }

  /**
   * 安全模式下完成Kerberos登录。
   * @throws IOException 登录失败抛出异常
   */
  private void doSecureLogin() throws IOException {
    Configuration conf = getConfig();
    String webAppURLWithoutScheme =
        WebAppUtils.getTimelineCollectorWebAppURLWithoutScheme(conf);
    InetSocketAddress addr = NetUtils.createSocketAddr(webAppURLWithoutScheme);
    SecurityUtil.login(conf, YarnConfiguration.TIMELINE_SERVICE_KEYTAB,
        YarnConfiguration.TIMELINE_SERVICE_PRINCIPAL, addr.getHostName());
  }

  @Override
  protected void serviceStop() throws Exception {
    // 停止REST服务
    if (timelineRestServer != null) {
      timelineRestServer.stop();
    }
    // 关闭定时任务线程池
    if (tokenRenewalExecutor != null) {
      tokenRenewalExecutor.shutdownNow();
    }
    super.serviceStop();
  }

  @VisibleForTesting
  public Token<TimelineDelegationTokenIdentifier> generateTokenForAppCollector(
      String user) {
    Token<TimelineDelegationTokenIdentifier> token  = tokenMgrService.
        generateToken(UserGroupInformation.createRemoteUser(user),
            loginUGI.getShortUserName());
    // 设置令牌服务地址
    token.setService(new Text(timelineRestServerBindAddress));
    return token;
  }

  @VisibleForTesting
  public long renewTokenForAppCollector(
      AppLevelTimelineCollector appCollector) throws IOException {
    if (appCollector.getDelegationTokenForApp() != null) {
      return tokenMgrService.renewToken(appCollector.getDelegationTokenForApp(),
          appCollector.getAppDelegationTokenRenewer());
    } else {
      LOG.info("Delegation token not available for renewal for app {}",
          appCollector.getTimelineEntityContext().getAppId());
      return -1;
    }
  }

  @VisibleForTesting
  public void cancelTokenForAppCollector(
      AppLevelTimelineCollector appCollector) throws IOException {
    if (appCollector.getDelegationTokenForApp() != null) {
      tokenMgrService.cancelToken(appCollector.getDelegationTokenForApp(),
          appCollector.getAppUser());
    }
  }

  /**
   * 计算下次续期的延迟时间，提前进行续期。
   * @param renewInterval 原始续期间隔
   * @return 调整后的延迟时间
   */
  private long getRenewalDelay(long renewInterval) {
    return ((renewInterval > TIME_BEFORE_RENEW_DATE) ?
        renewInterval - TIME_BEFORE_RENEW_DATE : renewInterval);
  }

  /**
   * 计算令牌重新生成的延迟时间，提前过期前重新生成。
   * @param tokenMaxDate 令牌最大过期时间
   * @return 调整后的延迟时间
   */
  private long getRegenerationDelay(long tokenMaxDate) {
    long regenerateTime = tokenMaxDate - Time.now();
    return ((regenerateTime > TIME_BEFORE_EXPIRY) ?
        regenerateTime - TIME_BEFORE_EXPIRY : regenerateTime);
  }

  /**
   * 生成应用采集器令牌并设置续期定时任务。
   * @param appId 应用ID
   * @param appCollector 应用级采集器实例
   * @return 生成的YARN令牌
   * @throws IOException 生成失败抛出异常
   */
  private org.apache.hadoop.yarn.api.records.Token generateTokenAndSetTimer(
      ApplicationId appId, AppLevelTimelineCollector appCollector)
      throws IOException {
    // 生成新的委托令牌
    Token<TimelineDelegationTokenIdentifier> timelineToken =
        generateTokenForAppCollector(appCollector.getAppUser());
    // 解码令牌标识符
    TimelineDelegationTokenIdentifier tokenId =
        timelineToken.decodeIdentifier();
    // 计算续期延迟
    long renewalDelay = getRenewalDelay(tokenRenewInterval);
    // 计算重新生成延迟
    long regenerationDelay = getRegenerationDelay(tokenId.getMaxDate());
    // 需要安排定时任务
    if (renewalDelay > 0 || regenerationDelay > 0) {
      // 选择先执行哪个任务
      boolean isTimerForRenewal = renewalDelay < regenerationDelay;
      // 提交定时任务
      Future<?> renewalOrRegenerationFuture = tokenRenewalExecutor.schedule(
          new CollectorTokenRenewer(appId, isTimerForRenewal),
          isTimerForRenewal? renewalDelay : regenerationDelay,
          TimeUnit.MILLISECONDS);
      // 保存令牌和任务future到采集器
      appCollector.setDelegationTokenAndFutureForApp(timelineToken,
          renewalOrRegenerationFuture, tokenId.getMaxDate(),
          tokenId.getRenewer().toString());
    }
    LOG.info("Generated a new token {} for app {}", timelineToken, appId);
    // 转换为YARN令牌格式返回
    return org.apache.hadoop.yarn.api.records.Token.newInstance(
        timelineToken.getIdentifier(), timelineToken.getKind().toString(),
        timelineToken.getPassword(), timelineToken.getService().toString());
  }

  @Override
  protected void doPostPut(ApplicationId appId, TimelineCollector collector) {
    try {
      // 从NodeManager获取采集器上下文信息并更新
      updateTimelineCollectorContext(appId, collector);
      // 生成应用采集器令牌
      org.apache.hadoop.yarn.api.records.Token token = null;
      if (UserGroupInformation.isSecurityEnabled() &&
          collector instanceof AppLevelTimelineCollector) {
        AppLevelTimelineCollector appCollector =
            (AppLevelTimelineCollector) collector;
        token = generateTokenAndSetTimer(appId, appCollector);
      }
      // 向NodeManager报告新采集器信息
      reportNewCollectorInfoToNM(appId, token);
    } catch (YarnException | IOException e) {
      // 和NodeManager通信失败，无法继续使用，抛出运行时异常
      LOG.error("Failed to communicate with NM Collector Service for {}", appId);
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  protected void postRemove(ApplicationId appId, TimelineCollector collector) {
    // 应用级采集器移除后取消令牌
    if (collector instanceof AppLevelTimelineCollector) {
      try {
        cancelTokenForAppCollector((AppLevelTimelineCollector) collector);
      } catch (IOException e) {
        LOG.warn("Failed to cancel token for app collector with appId {}",
            appId, e);
      }
    }
  }

  /**
   * 启动节点级采集器的REST Web服务。
   */
  private void startWebApp() {
    Configuration conf = getConfig();
    String initializers = conf.get("hadoop.http.filter.initializers", "");
    Set<String> defaultInitializers = new LinkedHashSet<String>();
    // 添加Timeline认证过滤器
    TimelineServerUtils.addTimelineAuthFilter(
        initializers, defaultInitializers, tokenMgrService);
    // 设置最终过滤器配置
    TimelineServerUtils.setTimelineFilters(
        conf, initializers, defaultInitializers);

    String bindAddress = null;
    // 获取绑定主机配置
    String host =
        conf.getTrimmed(YarnConfiguration.TIMELINE_SERVICE_COLLECTOR_BIND_HOST);
    // 获取端口范围配置
    Configuration.IntegerRanges portRanges = conf.getRange(
        YarnConfiguration.TIMELINE_SERVICE_COLLECTOR_BIND_PORT_RANGES, "");
    int startPort = 0;
    if (portRanges != null && !portRanges.isEmpty()) {
      startPort = portRanges.getRangeStart();
    }
    // 主机未配置，兼容旧配置，使用全局timeline绑定主机
    if (host == null || host.isEmpty()) {
      bindAddress =
          conf.get(YarnConfiguration.DEFAULT_TIMELINE_SERVICE_BIND_HOST,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_BIND_HOST)
              + ":" + startPort;
    } else {
      bindAddress = host + ":" + startPort;
    }

    try {
      // 构建HttpServer
      HttpServer2.Builder builder = new HttpServer2.Builder()
          .setName("timeline")
          .setConf(conf)
          .addEndpoint(URI.create(
              (YarnConfiguration.useHttps(conf) ? "https://" : "http://") +
                  bindAddress));
      // 设置端口范围
      if (portRanges != null && !portRanges.isEmpty()) {
        builder.setPortRanges(portRanges);
      }
      // HTTPS模式下加载SSL配置
      if (YarnConfiguration.useHttps(conf)) {
        builder = WebAppUtils.loadSslConfiguration(builder, conf);
      }
      timelineRestServer = builder.build();
      // 添加Jersey资源配置
      timelineRestServer.addJerseyResourceConfig(configure(), "/*", null);
      // 设置当前管理器到Servlet上下文属性
      timelineRestServer.setAttribute(COLLECTOR_MANAGER_ATTR_KEY, this);
      // 启动服务
      timelineRestServer.start();
    } catch (Exception e) {
      String msg = "The per-node collector webapp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
    // 解析获取实际绑定地址
    this.timelineRestServerBindAddress = WebAppUtils.getResolvedAddress(
        timelineRestServer.getConnectorAddress(0));
    LOG.info("Instantiated the per-node collector webapp at {}",
        timelineRestServerBindAddress);
  }

  /**
   * 配置Jersey REST资源。
   * @return 配置好的ResourceConfig
   */
  protected static ResourceConfig configure() {
    ResourceConfig config = new ResourceConfig();
    // 扫描当前包注册资源
    config.packages("org.apache.hadoop.yarn.server.timelineservice.collector");
    // 注册异常处理器
    config.register(GenericExceptionHandler.class);
    // 注册采集器Web服务
    config.register(TimelineCollectorWebService.class);
    // 注册时间线实体写服务
    config.register(TimelineEntitiesWriter.class);
    // 注册时间线实体读服务
    config.register(TimelineEntitiesReader.class);
    // 注册时间线域读服务
    config.register(TimelineDomainReader.class);
    // 注册JSON处理组件
    config.register(new JettisonFeature()).register(YarnJacksonJaxbJsonProvider.class);
    return config;
  }

  /**
   * 向NodeManager报告新采集器地址和令牌信息。
   * @param appId 应用ID
   * @param token 应用采集器令牌
   * @throws YarnException 报告失败抛出异常
   * @throws IOException 通信失败抛出异常
   */
  private void reportNewCollectorInfoToNM(Application