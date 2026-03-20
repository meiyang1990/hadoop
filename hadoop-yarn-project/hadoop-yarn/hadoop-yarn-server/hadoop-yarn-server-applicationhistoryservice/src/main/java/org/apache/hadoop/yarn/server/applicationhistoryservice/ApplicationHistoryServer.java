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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.records.timeline.reader.TimelineDomainReader;
import org.apache.hadoop.yarn.api.records.timeline.reader.TimelineEntityReader;
import org.apache.hadoop.yarn.api.records.timeline.reader.TimelinePutResponseReader;
import org.apache.hadoop.yarn.api.records.timeline.reader.TimelineEntitiesReader;
import org.apache.hadoop.yarn.api.records.timeline.writer.TimelineEntityWriter;
import org.apache.hadoop.yarn.api.records.timeline.writer.TimelineEntitiesWriter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.AHSWebApp;
import org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.AHSWebServices;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.server.timeline.security.TimelineV1DelegationTokenSecretManagerService;
import org.apache.hadoop.yarn.server.timeline.webapp.CrossOriginFilterInitializer;
import org.apache.hadoop.yarn.server.timeline.webapp.TimelineWebServices;
import org.apache.hadoop.yarn.server.util.timeline.TimelineServerUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import org.apache.hadoop.classification.VisibleForTesting;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 应用历史服务器，负责跟踪集群中所有类型的历史数据。
 * 主要提供应用级别的历史记录服务。
 */
public class ApplicationHistoryServer extends CompositeService {

  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private static final Logger LOG = LoggerFactory
      .getLogger(ApplicationHistoryServer.class);

  // 客户端 RPC 服务
  private ApplicationHistoryClientService ahsClientService;
  // ACL 管理器
  private ApplicationACLsManager aclsManager;
  // 应用历史管理器
  private ApplicationHistoryManager historyManager;
  // Timeline 数据存储
  private TimelineStore timelineStore;
  // 代理令牌密钥管理服务
  private TimelineV1DelegationTokenSecretManagerService secretManagerService;
  // Timeline 数据管理器
  private TimelineDataManager timelineDataManager;
  // Web 应用
  private WebApp webApp;
  // JVM 暂停监控器
  private JvmPauseMonitor pauseMonitor;

  public ApplicationHistoryServer() {
    super(ApplicationHistoryServer.class.getName());
  }

  /**
   * 初始化服务：安全登录、创建各子服务并注册
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    // 首先执行安全登录
    try {
      doSecureLogin(conf);
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to login", ie);
    }
    // 初始化 Timeline 相关服务
    timelineStore = createTimelineStore(conf);
    addIfService(timelineStore);
    secretManagerService = createTimelineDelegationTokenSecretManagerService(conf);
    addService(secretManagerService);
    timelineDataManager = createTimelineDataManager(conf);
    addService(timelineDataManager);

    // 初始化通用历史服务
    aclsManager = createApplicationACLsManager(conf);
    historyManager = createApplicationHistoryManager(conf);
    ahsClientService = createApplicationHistoryClientService(historyManager);
    addService(ahsClientService);
    addService((Service) historyManager);

    // 初始化监控系统
    DefaultMetricsSystem.initialize("ApplicationHistoryServer");
    JvmMetrics jm = JvmMetrics.initSingleton("ApplicationHistoryServer", null);
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);
    super.serviceInit(conf);
  }

  /**
   * 启动服务，包括 Web 应用
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    startWebApp();
  }

  /**
   * 停止服务
   */
  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
    }
    DefaultMetricsSystem.shutdown();
    super.serviceStop();
  }

  @Private
  @VisibleForTesting
  ApplicationHistoryClientService getClientService() {
    return this.ahsClientService;
  }

  private InetSocketAddress getListenerAddress() {
    return this.webApp.httpServer().getConnectorAddress(0);
  }

  @Private
  @VisibleForTesting
  public int getPort() {
    return this.getListenerAddress().getPort();
  }

  /**
   * @return Timeline 存储
   */
  @Private
  @VisibleForTesting
  public TimelineStore getTimelineStore() {
    return timelineStore;
  }

  @Private
  @VisibleForTesting
  ApplicationHistoryManager getApplicationHistoryManager() {
    return this.historyManager;
  }

  /**
   * 启动应用历史服务器的静态入口
   */
  static ApplicationHistoryServer launchAppHistoryServer(String[] args) {
    Thread
      .setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(ApplicationHistoryServer.class, args,
      LOG);
    ApplicationHistoryServer appHistoryServer = null;
    try {
      appHistoryServer = new ApplicationHistoryServer();
      ShutdownHookManager.get().addShutdownHook(
        new CompositeServiceShutdownHook(appHistoryServer),
        SHUTDOWN_HOOK_PRIORITY);
      YarnConfiguration conf = new YarnConfiguration();
      new GenericOptionsParser(conf, args);
      appHistoryServer.init(conf);
      appHistoryServer.start();
    } catch (Throwable t) {
      LOG.error("Error starting ApplicationHistoryServer", t);
      ExitUtil.terminate(-1, "Error starting ApplicationHistoryServer");
    }
    return appHistoryServer;
  }

  public static void main(String[] args) {
    launchAppHistoryServer(args);
  }

  private ApplicationHistoryClientService
      createApplicationHistoryClientService(
          ApplicationHistoryManager historyManager) {
    return new ApplicationHistoryClientService(historyManager);
  }

  private ApplicationACLsManager createApplicationACLsManager(
      Configuration conf) {
    return new ApplicationACLsManager(conf);
  }

  /**
   * 创建应用历史管理器，支持向后兼容
   * 如果未显式配置旧版存储，默认使用 Timeline 存储
   */
  private ApplicationHistoryManager createApplicationHistoryManager(
      Configuration conf) {
    // 向后兼容：如果显式配置了旧版存储，则使用旧实现
    if (conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE) == null ||
        conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE).length() == 0 ||
        conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE).equals(
            NullApplicationHistoryStore.class.getName())) {
      return new ApplicationHistoryManagerOnTimelineStore(
          timelineDataManager, aclsManager);
    } else {
      LOG.warn("The filesystem based application history store is deprecated.");
      return new ApplicationHistoryManagerImpl();
    }
  }

  private TimelineStore createTimelineStore(
      Configuration conf) {
    return ReflectionUtils.newInstance(conf.getClass(
        YarnConfiguration.TIMELINE_SERVICE_STORE, LeveldbTimelineStore.class,
        TimelineStore.class), conf);
  }

  private TimelineV1DelegationTokenSecretManagerService
      createTimelineDelegationTokenSecretManagerService(Configuration conf) {
    return new TimelineV1DelegationTokenSecretManagerService();
  }

  private TimelineDataManager createTimelineDataManager(Configuration conf) {
    TimelineACLsManager aclsMgr = new TimelineACLsManager(conf);
    aclsMgr.setTimelineStore(timelineStore);
    return new TimelineDataManager(timelineStore, aclsMgr);
  }

  /**
   * 启动 Web 应用，配置 CORS、认证过滤器和 UI 插件
   */
  @SuppressWarnings("unchecked")
  private void startWebApp() {
    Configuration conf = getConfig();
    // 始终加载伪认证过滤器以解析 URL 中的 "user.name"
    // 在非安全模式下用于识别 HTTP 请求的用户
    // 当启用 Kerberos 认证时，加载自定义过滤器进行 Kerberos + DT 认证
    String initializers = conf.get("hadoop.http.filter.initializers", "");
    Set<String> defaultInitializers = new LinkedHashSet<>();
    // 添加 CORS 过滤器
    if (!initializers.contains(CrossOriginFilterInitializer.class.getName())) {
      if(conf.getBoolean(YarnConfiguration.
          TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED,
          YarnConfiguration.
          TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED_DEFAULT)) {
        if (initializers.contains(
            HttpCrossOriginFilterInitializer.class.getName())) {
          initializers = initializers.replaceAll(
              HttpCrossOriginFilterInitializer.class.getName(),
              CrossOriginFilterInitializer.class.getName());
        } else {
          defaultInitializers.add(CrossOriginFilterInitializer.class.getName());
        }
      }
    }
    TimelineServerUtils.addTimelineAuthFilter(
        initializers, defaultInitializers, secretManagerService);
    TimelineServerUtils.setTimelineFilters(
        conf, initializers, defaultInitializers);
    String bindAddress = WebAppUtils.getWebAppBindURL(conf,
                          YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
                          WebAppUtils.getAHSWebAppURLWithoutScheme(conf));
    try {
      AHSWebApp ahsWebApp =
          new AHSWebApp(timelineDataManager, ahsClientService);
      webApp =
          WebApps
            .$for("applicationhistory", ApplicationHistoryClientService.class,
                ahsClientService, "app-hs-ws")
             .with(conf)
              .withAttribute(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
                 conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS))
              .withCSRFProtection(YarnConfiguration.TIMELINE_CSRF_PREFIX)
              .withXFSProtection(YarnConfiguration.TIMELINE_XFS_PREFIX)
              .withResourceConfig(configure())
              .at(bindAddress).build(ahsWebApp);
       HttpServer2 httpServer = webApp.httpServer();

       // 加载配置的 UI 插件
       String[] names = conf.getTrimmedStrings(
           YarnConfiguration.TIMELINE_SERVICE_UI_NAMES);
       WebAppContext webAppContext = httpServer.getWebAppContext();

      for (String name : names) {
        String webPath = conf.get(
            YarnConfiguration.TIMELINE_SERVICE_UI_WEB_PATH_PREFIX + name);
        String onDiskPath = conf.get(
            YarnConfiguration.TIMELINE_SERVICE_UI_ON_DISK_PATH_PREFIX + name);
        WebAppContext uiWebAppContext = new WebAppContext();
        uiWebAppContext.setContextPath(webPath);
        if (onDiskPath.endsWith(".war")) {
          uiWebAppContext.setWar(onDiskPath);
        } else {
          uiWebAppContext.setResourceBase(onDiskPath);
        }
        final String[] ALL_URLS = {"/*"};
        // 复制过滤器到 UI 上下文
        FilterHolder[] filterHolders =
            webAppContext.getServletHandler().getFilters();
        for (FilterHolder filterHolder : filterHolders) {
          if (!"guice".equals(filterHolder.getName())) {
            HttpServer2.defineFilter(uiWebAppContext, filterHolder.getName(),
                filterHolder.getClassName(), filterHolder.getInitParameters(),
                ALL_URLS);
          }
        }
        LOG.info("Hosting {} from {} at {}.", name, onDiskPath, webPath);
        httpServer.addHandlerAtFront(uiWebAppContext);
      }
       httpServer.start();
       conf.updateConnectAddr(YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
        YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS,
        this.getListenerAddress());
      LOG.info("Instantiating AHSWebApp at {}.", getPort());
    } catch (Exception e) {
      String msg = "AHSWebApp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }

  /**
   * 执行安全登录
   */
  private void doSecureLogin(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getBindAddress(conf);
    SecurityUtil.login(conf, YarnConfiguration.TIMELINE_SERVICE_KEYTAB,
        YarnConfiguration.TIMELINE_SERVICE_PRINCIPAL, socAddr.getHostName());
  }

  /**
   * 从配置中获取 Timeline 服务器绑定地址
   */
  private static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_PORT);
  }

  /**
   * 配置 Jersey REST 资源
   */
  protected ResourceConfig configure() {
    ResourceConfig config = new ResourceConfig();
    config.packages("org.apache.hadoop.yarn.server.timeline.webapp");
    config.packages("org.apache.hadoop.yarn.server.applicationhistoryservice.webapp");
    config.packages("org.apache.hadoop.yarn.api.records.writer");
    config.register(TimelineWebServices.class);
    config.register(AHSWebServices.class);
    config.register(TimelineEntitiesWriter.class);
    config.register(TimelineEntitiesReader.class);
    config.register(TimelineEntityReader.class);
    config.register(TimelineEntityWriter.class);
    config.register(TimelineDomainReader.class);
    config.register(TimelinePutResponseReader.class);
    config.register(new JerseyBinder());
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(YarnJacksonJaxbJsonProvider.class);
    return config;
  }

  /**
   * Jersey 依赖注入绑定器
   */
  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      bind(timelineDataManager).to(TimelineDataManager.class).named("manager");
      bind(getConfig()).to(Configuration.class).named("conf");
      bind(ahsClientService).to(ApplicationBaseProtocol.class).named("appBaseProt");
    }
  }
}
