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

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.utils.FederationRegistryClient;
import org.apache.hadoop.yarn.server.globalpolicygenerator.applicationcleaner.ApplicationCleaner;
import org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator.PolicyGenerator;
import org.apache.hadoop.yarn.server.globalpolicygenerator.subclustercleaner.SubClusterCleaner;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.GPGWebApp;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.WebServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 全局策略生成器(GPG)是YARN联邦的核心组件，通过调整联邦状态存储中的路由策略，
 * 监控整个联邦集群状态，持续保障集群负载均衡和全局状态一致。
 * 
 * GPG以后台异步方式持续运行，独立于常规集群操作，可实现全局策略统一规划、
 * 负载均衡调整、待维护子集群任务 draining 等功能。
 */
public class GlobalPolicyGenerator extends CompositeService {

  public static final Logger LOG =
      LoggerFactory.getLogger(GlobalPolicyGenerator.class);

  // YARN 全局变量
  private static CompositeServiceShutdownHook gpgShutdownHook;
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  private static final String METRICS_NAME = "Global Policy Generator";
  private static long gpgStartupTime = System.currentTimeMillis();

  // 联邦相关变量
  private GPGContext gpgContext;
  private RegistryOperations registry;

  // 周期性任务调度执行器
  private ScheduledThreadPoolExecutor scheduledExecutorService;
  private SubClusterCleaner subClusterCleaner;
  private ApplicationCleaner applicationCleaner;
  private PolicyGenerator policyGenerator;
  private String webAppAddress;
  private JvmPauseMonitor pauseMonitor;
  private WebApp webApp;

  public GlobalPolicyGenerator() {
    super(GlobalPolicyGenerator.class.getName());
    this.gpgContext = new GPGContextImpl();
  }

  /**
   * 安全登录Kerberos获取凭证。
   * @throws IOException 登录失败抛出异常
   */
  protected void doSecureLogin() throws IOException {
    Configuration config = getConfig();
    SecurityUtil.login(config, YarnConfiguration.GPG_KEYTAB,
        YarnConfiguration.GPG_PRINCIPAL, getHostName(config));
  }

  /**
   * 初始化并启动GPG服务，注册关闭钩子。
   * @param conf 配置对象
   * @param hasToReboot 是否重启，重启需要移除旧钩子
   */
  protected void initAndStart(Configuration conf, boolean hasToReboot) {
    // 重启时移除旧的关闭钩子
    if (hasToReboot && null != gpgShutdownHook) {
      ShutdownHookManager.get().removeShutdownHook(gpgShutdownHook);
    }
    gpgShutdownHook = new CompositeServiceShutdownHook(this);
    ShutdownHookManager.get().addShutdownHook(gpgShutdownHook,
        SHUTDOWN_HOOK_PRIORITY);
    this.init(conf);
    this.start();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    UserGroupInformation.setConfiguration(conf);
    // 初始化GPG上下文
    this.gpgContext.setStateStoreFacade(FederationStateStoreFacade.getInstance(conf));
    GPGPolicyFacade gpgPolicyFacade =
        new GPGPolicyFacade(this.gpgContext.getStateStoreFacade(), conf);
    this.gpgContext.setPolicyFacade(gpgPolicyFacade);

    // 创建并初始化服务注册中心
    this.registry = FederationStateStoreFacade.createInstance(conf,
        YarnConfiguration.YARN_REGISTRY_CLASS,
        YarnConfiguration.DEFAULT_YARN_REGISTRY_CLASS,
        RegistryOperations.class);
    this.registry.init(conf);

    // 初始化联邦注册客户端
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    FederationRegistryClient registryClient =
        new FederationRegistryClient(conf, this.registry, user);
    this.gpgContext.setRegistryClient(registryClient);

    // 创建周期性任务线程池
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(
        conf.getInt(YarnConfiguration.GPG_SCHEDULED_EXECUTOR_THREADS,
            YarnConfiguration.DEFAULT_GPG_SCHEDULED_EXECUTOR_THREADS));
    // 初始化子集群清理器
    this.subClusterCleaner = new SubClusterCleaner(conf, this.gpgContext);

    // 创建并初始化应用清理器
    this.applicationCleaner = FederationStateStoreFacade.createInstance(conf,
        YarnConfiguration.GPG_APPCLEANER_CLASS,
        YarnConfiguration.DEFAULT_GPG_APPCLEANER_CLASS, ApplicationCleaner.class);
    this.applicationCleaner.init(conf, this.gpgContext);

    // 初始化策略生成器
    this.policyGenerator = new PolicyGenerator(conf, this.gpgContext);

    // 获取Web服务地址
    this.webAppAddress = WebAppUtils.getGPGWebAppURLWithoutScheme(conf);
    // 初始化 metrics 系统
    DefaultMetricsSystem.initialize(METRICS_NAME);
    JvmMetrics jm = JvmMetrics.initSingleton("GPG", null);
    // 初始化JVM暂停监控
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);

    // 所有服务添加完成后调用父类初始化
    super.serviceInit(conf);
    WebServiceClient.initialize(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed GPG login", e);
    }

    super.serviceStart();

    // 启动注册中心服务
    this.registry.start();

    // 调度子集群清理任务
    Configuration config = getConfig();
    long scCleanerIntervalMs = config.getTimeDuration(
        YarnConfiguration.GPG_SUBCLUSTER_CLEANER_INTERVAL_MS,
        YarnConfiguration.DEFAULT_GPG_SUBCLUSTER_CLEANER_INTERVAL_MS, TimeUnit.MILLISECONDS);
    // 间隔大于0才启动周期性任务
    if (scCleanerIntervalMs > 0) {
      this.scheduledExecutorService.scheduleAtFixedRate(this.subClusterCleaner,
          0, scCleanerIntervalMs, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled sub-cluster cleaner with interval: {}",
          DurationFormatUtils.formatDurationISO(scCleanerIntervalMs));
    }

    // 调度应用清理任务
    long appCleanerIntervalMs = config.getTimeDuration(
        YarnConfiguration.GPG_APPCLEANER_INTERVAL_MS,
        YarnConfiguration.DEFAULT_GPG_APPCLEANER_INTERVAL_MS, TimeUnit.MILLISECONDS);

    if (appCleanerIntervalMs > 0) {
      this.scheduledExecutorService.scheduleAtFixedRate(this.applicationCleaner,
          0, appCleanerIntervalMs, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled application cleaner with interval: {}",
          DurationFormatUtils.formatDurationISO(appCleanerIntervalMs));
    }

    // 调度策略生成任务，兼容新旧配置项
    // 推荐使用yarn.federation.gpg.policy.generator.interval，替代旧的毫秒配置

    // 先读取旧配置项的值兼容旧版本
    long policyGeneratorIntervalMillis = 0L;
    String generatorIntervalMS = config.get(YarnConfiguration.GPG_POLICY_GENERATOR_INTERVAL_MS);
    if (generatorIntervalMS != null) {
      LOG.warn("yarn.federation.gpg.policy.generator.interval-ms is deprecated property, " +
          " we better set it yarn.federation.gpg.policy.generator.interval.");
      policyGeneratorIntervalMillis = Long.parseLong(generatorIntervalMS);
    }

    // 旧配置不存在时，读取新配置项
    if (policyGeneratorIntervalMillis == 0) {
      policyGeneratorIntervalMillis = config.getTimeDuration(
          YarnConfiguration.GPG_POLICY_GENERATOR_INTERVAL,
          YarnConfiguration.DEFAULT_GPG_POLICY_GENERATOR_INTERVAL, TimeUnit.MILLISECONDS);
    }

    if(policyGeneratorIntervalMillis > 0){
      this.scheduledExecutorService.scheduleAtFixedRate(this.policyGenerator,
          0, policyGeneratorIntervalMillis, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled policy-generator with interval: {}",
          DurationFormatUtils.formatDurationISO(policyGeneratorIntervalMillis));
    }
    // 启动Web服务
    startWepApp();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.registry != null) {
      this.registry.stop();
      this.registry = null;
    }

    try {
      if (this.scheduledExecutorService != null
          && !this.scheduledExecutorService.isShutdown()) {
        this.scheduledExecutorService.shutdown();
        LOG.info("Stopped ScheduledExecutorService");
      }
    } catch (Exception e) {
      LOG.error("Failed to shutdown ScheduledExecutorService", e);
      throw e;
    }

    // 避免重复停止
    if (this.isStopping.getAndSet(true)) {
      return;
    }
    if (webApp != null) {
      webApp.stop();
    }
    DefaultMetricsSystem.shutdown();
    super.serviceStop();
    WebServiceClient.destroy();
  }

  public String getName() {
    return "FederationGlobalPolicyGenerator";
  }

  public GPGContext getGPGContext() {
    return this.gpgContext;
  }

  @VisibleForTesting
  public void startWepApp() {
    Configuration configuration = getConfig();

    // 处理跨域配置
    boolean enableCors = configuration.getBoolean(YarnConfiguration.GPG_WEBAPP_ENABLE_CORS_FILTER,
        YarnConfiguration.DEFAULT_GPG_WEBAPP_ENABLE_CORS_FILTER);

    if (enableCors) {
      configuration.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }

    // 确保认证过滤器被加载，用于解析URL中的user.name识别请求用户
    boolean hasHadoopAuthFilterInitializer = false;
    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    Class<?>[] initializersClasses = configuration.getClasses(filterInitializerConfKey);

    List<String> targets = new ArrayList<>();
    if (initializersClasses != null) {
      for (Class<?> initializer : initializersClasses) {
        if (initializer.getName().equals(AuthenticationFilterInitializer.class.getName())) {
          hasHadoopAuthFilterInitializer = true;
          break;
        }
        targets.add(initializer.getName());
      }
    }
    // 如果未配置认证过滤器，添加进去
    if (!hasHadoopAuthFilterInitializer) {
      targets.add(AuthenticationFilterInitializer.class.getName());
      configuration.set(filterInitializerConfKey, StringUtils.join(",", targets));
    }
    LOG.info("Instantiating GPGWebApp at {}.", webAppAddress);
    GPGWebApp gpgWebApp = new GPGWebApp(this);
    // 启动Web应用
    webApp = WebApps.$for("gpg", GPGContext.class, this.gpgContext,
        "gpg-ws").at(webAppAddress).
         withResourceConfig(gpgWebApp.resourceConfig()).start(gpgWebApp);
  }

  /**
   * 启动GPG服务入口方法。
   * @param argv 启动参数
   * @param conf 配置对象
   */
  @SuppressWarnings("resource")
  public static void startGPG(String[] argv, Configuration conf) {
    boolean federationEnabled = conf.getBoolean(YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    // 联邦未开启不启动GPG
    if (federationEnabled) {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      StringUtils.startupShutdownMessage(GlobalPolicyGenerator.class, argv, LOG);
      GlobalPolicyGenerator globalPolicyGenerator = new GlobalPolicyGenerator();
      globalPolicyGenerator.initAndStart(conf, false);
    } else {
      LOG.warn("Federation is not enabled. The gpg cannot start.");
    }
  }

  /**
   * 获取GPG服务主机名，未配置则自动获取本机主机名。
   * @param config 配置对象
   * @return 主机名
   * @throws UnknownHostException 无法获取主机名抛出异常
   */
  private String getHostName(Configuration config)
      throws UnknownHostException {
    String name = config.get(YarnConfiguration.GPG_KERBEROS_PRINCIPAL_HOSTNAME_KEY);
    if (name == null) {
      name = InetAddress.getLocalHost().getHostName();
    }
    return name;
  }

  public static void main(String[] argv) {
    try {
      YarnConfiguration conf = new YarnConfiguration();
      GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
      argv = hParser.getRemainingArgs();
      // 处理命令行参数
      if (argv.length > 1) {
        if (argv[0].equals("-format-policy-store")) {
          // 格式化清空策略存储
          handFormatPolicyStateStore(conf);
        } else {
          printUsage(System.err);
        }
      } else {
        // 正常启动GPG服务
        startGPG(argv, conf);
      }
    } catch (Throwable t) {
      LOG.error("Error starting global policy generator", t);
      System.exit(-1);
    }
  }

  public static long getGPGStartupTime() {
    return gpgStartupTime;
  }

  @VisibleForTesting
  public WebApp getWebApp() {
    return webApp;
  }

  /**
   * 打印命令行帮助信息。
   * @param out 输出流
   */
  private static void printUsage(PrintStream out) {
    out.println("Usage: yarn gpg [-format-policy-store]");
  }

  /**
   * 格式化清空联邦策略状态存储。
   * @param conf 配置对象
   */
  private static void handFormatPolicyStateStore(Configuration conf) {
    try {
      System.out.println("Deleting Federation policy state store.");
      FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance(conf);
      System.out.println("Federation policy state store has been cleaned.");
      facade.deleteAllPoliciesConfigurations();
    } catch (Exception e) {
      LOG.error("Delete Federation policy state store error.", e);
      System.err.println("Delete Federation policy state store error, exception = " + e);
    }
  }

  @Override
  public void setConfig(Configuration conf) {
    super.setConfig(conf);
  }
}