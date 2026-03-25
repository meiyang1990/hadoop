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

package org.apache.hadoop.yarn.server.router;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.router.cleaner.SubClusterCleaner;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.rmadmin.RouterRMAdminService;
import org.apache.hadoop.yarn.server.router.webapp.RouterWebApp;
import org.apache.hadoop.yarn.server.webproxy.FedAppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.WebServiceClient;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_ROUTER_DEREGISTER_SUBCLUSTER_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_DEREGISTER_SUBCLUSTER_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_SUBCLUSTER_CLEANER_INTERVAL_TIME;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_ROUTER_SUBCLUSTER_CLEANER_INTERVAL_TIME;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_SCHEDULED_EXECUTOR_THREADS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_ROUTER_SCHEDULED_EXECUTOR_THREADS;

/**
 * YARN联邦集群路由器，是YARN联邦对外的统一入口点，是无状态的可水平扩展组件。
 * 它可以部署在多个节点，放置在负载均衡器和VIP后面对外提供服务。
 * 
 * 路由器对外暴露ApplicationClientProtocol（RPC和REST接口），对客户端透明隐藏
 * 后端多个ResourceManager的存在，允许用户提交/杀死应用、查询应用状态、
 * 预约/更新资源等操作。同时也对外暴露了ResourceManager管理API。
 * 
 * 主要作用：作为YARN联邦的统一入口，隔离客户端对后端多个RM的直接访问，
 * 支持客户端限流，屏蔽多RM实现细节，支持集群横向扩展。
 */
public class Router extends CompositeService {

  private static final Logger LOG = LoggerFactory.getLogger(Router.class);
  private static CompositeServiceShutdownHook routerShutdownHook;
  private Configuration conf;
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  private JvmPauseMonitor pauseMonitor;
  @VisibleForTesting
  protected RouterClientRMService clientRMProxyService;
  @VisibleForTesting
  protected RouterRMAdminService rmAdminProxyService;
  private WebApp webApp;
  @VisibleForTesting
  protected String webAppAddress;
  private static long clusterTimeStamp = System.currentTimeMillis();
  private FedAppReportFetcher fetcher = null;
  private static final String CMD_FORMAT_STATE_STORE = "-format-state-store";
  private static final String CMD_REMOVE_APPLICATION_FROM_STATE_STORE =
      "-remove-application-from-state-store";

  /**
   * Router关闭钩子的优先级。
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final String METRICS_NAME = "Router";

  private static final String UI2_WEBAPP_NAME = "/ui2";

  private ScheduledThreadPoolExecutor scheduledExecutorService;
  private SubClusterCleaner subClusterCleaner;

  /**
   * 构造Router实例。
   */
  public Router() {
    super(Router.class.getName());
  }

  /**
   * 执行安全Kerberos登录。
   * @throws IOException 登录失败抛出异常
   */
  protected void doSecureLogin() throws IOException {
    SecurityUtil.login(this.conf, YarnConfiguration.ROUTER_KEYTAB,
        YarnConfiguration.ROUTER_PRINCIPAL, getHostName(this.conf));
  }

  @Override
  protected void serviceInit(Configuration config) throws Exception {
    this.conf = config;
    UserGroupInformation.setConfiguration(this.conf);
    // 初始化客户端RM代理服务
    clientRMProxyService = createClientRMProxyService();
    addService(clientRMProxyService);
    // 初始化RM管理API代理服务
    rmAdminProxyService = createRMAdminProxyService();
    addService(rmAdminProxyService);
    // 初始化Web服务地址
    webAppAddress = WebAppUtils.getWebAppBindURL(this.conf,
        YarnConfiguration.ROUTER_BIND_HOST,
        WebAppUtils.getRouterWebAppURLWithoutScheme(this.conf));
    // 初始化指标系统
    DefaultMetricsSystem.initialize(METRICS_NAME);
    JvmMetrics jm = JvmMetrics.initSingleton("Router", null);
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);

    // 初始化子集群下线清理器
    this.subClusterCleaner = new SubClusterCleaner(this.conf);
    int scheduledExecutorThreads = conf.getInt(ROUTER_SCHEDULED_EXECUTOR_THREADS,
        DEFAULT_ROUTER_SCHEDULED_EXECUTOR_THREADS);
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(scheduledExecutorThreads);

    WebServiceClient.initialize(config);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed Router login", e);
    }
    // 判断是否开启子集群自动注销清理功能
    boolean isDeregisterSubClusterEnabled = this.conf.getBoolean(
        ROUTER_DEREGISTER_SUBCLUSTER_ENABLED, DEFAULT_ROUTER_DEREGISTER_SUBCLUSTER_ENABLED);
    if (isDeregisterSubClusterEnabled) {
      // 获取清理任务执行间隔
      long scCleanerIntervalMs = this.conf.getTimeDuration(ROUTER_SUBCLUSTER_CLEANER_INTERVAL_TIME,
          DEFAULT_ROUTER_SUBCLUSTER_CLEANER_INTERVAL_TIME, TimeUnit.MILLISECONDS);
      // 启动定时清理任务，首次立即执行
      this.scheduledExecutorService.scheduleAtFixedRate(this.subClusterCleaner,
          0, scCleanerIntervalMs, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled SubClusterCleaner With Interval: {}.",
          DurationFormatUtils.formatDurationISO(scCleanerIntervalMs));
    }
    startWepApp();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
    }
    if (isStopping.getAndSet(true)) {
      return;
    }
    super.serviceStop();
    DefaultMetricsSystem.shutdown();
    WebServiceClient.destroy();
  }

  /**
   * 停止Router服务，在独立线程中执行避免阻塞关闭钩子。
   */
  protected void shutDown() {
    new SubjectInheritingThread(Router.this::stop).start();
  }

  /**
   * 创建客户端RM代理服务实例。
   * @return 客户端RM代理服务
   */
  protected RouterClientRMService createClientRMProxyService() {
    return new RouterClientRMService();
  }

  /**
   * 创建RM管理API代理服务实例。
   * @return RM管理API代理服务
   */
  protected RouterRMAdminService createRMAdminProxyService() {
    return new RouterRMAdminService();
  }

  @Private
  public WebApp getWebapp() {
    return this.webApp;
  }

  @VisibleForTesting
  public void startWepApp() {

    // 初始化Router Web端跨域支持
    boolean enableCors = conf.getBoolean(YarnConfiguration.ROUTER_WEBAPP_ENABLE_CORS_FILTER,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_ENABLE_CORS_FILTER);
    if (enableCors) {
      conf.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }

    LOG.info("Instantiating RouterWebApp at {}.", webAppAddress);

    // 配置安全和过滤器
    RMWebAppUtil.setupSecurityAndFilters(conf, null);

    // 构建Web应用
    Builder<Object> builder =
        WebApps.$for("cluster", null, null, "router-ws").with(conf).at(webAppAddress);
    // 如果开启了Web应用代理，添加代理Servlet
    if (RouterServerUtil.isRouterWebProxyEnable(conf)) {
      fetcher = new FedAppReportFetcher(conf);
      builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME, ProxyUriUtils.PROXY_PATH_SPEC,
          WebAppProxyServlet.class);
      builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
      String proxyHostAndPort = getProxyHostAndPort(conf);
      String[] proxyParts = proxyHostAndPort.split(":");
      builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);
    }
    RouterWebApp routerWebApp = new RouterWebApp(this);
    builder.withResourceConfig(routerWebApp.resourceConfig());
    // 启动Web应用，绑定UI2上下文
    webApp = builder.start(routerWebApp, getUIWebAppContext());
  }

  /**
   * 获取UI2的Web应用上下文。
   * @return UI2 Web应用上下文，未开启则返回null
   */
  private WebAppContext getUIWebAppContext() {
    WebAppContext uiWebAppContext = null;
    boolean isWebUI2Enabled = conf.getBoolean(YarnConfiguration.YARN_WEBAPP_UI2_ENABLE,
        YarnConfiguration.DEFAULT_YARN_WEBAPP_UI2_ENABLE);

    if(isWebUI2Enabled) {
      // 从配置获取UI2 war包路径
      String onDiskPath = conf.get(YarnConfiguration.YARN_WEBAPP_UI2_WARFILE_PATH);
      uiWebAppContext = new WebAppContext();
      uiWebAppContext.setContextPath(UI2_WEBAPP_NAME);

      // 配置中未指定，自动查找war包
      if (null == onDiskPath) {
        String war = "hadoop-yarn-ui-" + VersionInfo.getVersion() + ".war";
        URL url = getClass().getClassLoader().getResource(war);
        if (null == url) {
          onDiskPath = getWebAppsPath("ui2");
        } else {
          onDiskPath = url.getFile();
        }
      }

      if (onDiskPath == null || onDiskPath.isEmpty()) {
        LOG.error("No war file or webapps found for yarn federation!");
      } else {
        // 是war文件直接设置war路径，否则设置资源基准路径
        if (onDiskPath.endsWith(".war")) {
          uiWebAppContext.setWar(onDiskPath);
          LOG.info("Using war file at: {}.", onDiskPath);
        } else {
          uiWebAppContext.setResourceBase(onDiskPath);
          LOG.info("Using webapps at: {}.", onDiskPath);
        }
      }
    }
    return uiWebAppContext;
  }

  /**
   * 获取webapps目录路径。
   * @param appName 应用名称
   * @return webapps目录路径，未找到返回空字符串
   */
  private String getWebAppsPath(String appName) {
    URL url = getClass().getClassLoader().getResource("webapps/" + appName);
    if (url == null) {
      return "";
    }
    return url.toString();
  }

  /**
   * 获取代理服务的主机和端口。
   * @param conf 配置对象
   * @return 代理服务地址字符串
   */
  public static String getProxyHostAndPort(Configuration conf) {
    String addr = conf.get(YarnConfiguration.PROXY_ADDRESS);
    if(addr == null || addr.isEmpty()) {
      InetSocketAddress address = conf.getSocketAddr(YarnConfiguration.ROUTER_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_PORT);
      addr = WebAppUtils.getResolvedAddress(address);
    }
    return addr;
  }

  /**
   * Router主入口方法，启动Router服务或执行管理命令。
   * @param argv 命令行参数
   */
  public static void main(String[] argv) {
    Configuration conf = new YarnConfiguration();
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(Router.class, argv, LOG);
    Router router = new Router();
    try {
      GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
      argv = hParser.getRemainingArgs();
      // 如果参数大于1个，说明是执行管理命令，不是启动服务
      if (argv.length > 1) {
        executeRouterCommand(conf, argv);
      } else {
        // 重启场景移除旧的关闭钩子
        if (null != routerShutdownHook) {
          ShutdownHookManager.get().removeShutdownHook(routerShutdownHook);
        }
        routerShutdownHook = new CompositeServiceShutdownHook(router);
        ShutdownHookManager.get().addShutdownHook(routerShutdownHook, SHUTDOWN_HOOK_PRIORITY);
        // 初始化并启动Router服务
        router.init(conf);
        router.start();
      }
    } catch (Throwable t) {
      LOG.error("Error starting Router", t);
      System.exit(-1);
    }
  }

  @VisibleForTesting
  public RouterClientRMService getClientRMProxyService() {
    return clientRMProxyService;
  }

  @VisibleForTesting
  public RouterRMAdminService getRmAdminProxyService() {
    return rmAdminProxyService;
  }

  /**
   * 获取Router的主机名，如果配置未指定则自动获取本地主机名。
   * 用于Kerberos主体名中的主机部分替换。
   *
   * @param config 配置对象
   * @return 主机名，不一定是全限定域名
   * @throws UnknownHostException 无法获取主机名时抛出异常
   */
  private String getHostName(Configuration config)
      throws UnknownHostException {
    String name = config.get(YarnConfiguration.ROUTER_KERBEROS_PRINCIPAL_HOSTNAME_KEY);
    if (name == null) {
      name = InetAddress.getLocalHost().getHostName();
    }
    return name;
  }

  /**
   * 获取Router集群启动时间戳。
   * @return 集群启动时间戳
   */
  public static long getClusterTimeStamp() {
    return clusterTimeStamp;
  }

  @VisibleForTesting
  public FedAppReportFetcher