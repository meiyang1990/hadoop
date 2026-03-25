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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.VisibleForTesting;
import org.glassfish.jersey.servlet.ServletContainer;

import org.apache.hadoop.yarn.metrics.GenericEventTypeMetrics;
import org.apache.hadoop.yarn.server.webproxy.DefaultAppReportFetcher;
import org.apache.hadoop.yarn.webapp.WebAppException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.federation.FederationStateStoreService;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.CombinedSystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.NoOpSystemMetricPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV1Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV2Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeAttributesManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.AbstractReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.MemoryPlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManagerService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.ProxyCAManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManagerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.processor.VolumeAMSProcessor;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.service.SystemServiceManager;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyCA;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * YARN 资源管理器，是YARN集群全局资源调度与管理的核心入口，聚合各类核心组件提供集群资源管理能力。
 * 负责统一管理集群节点资源，调度用户提交的应用程序任务，在高可用模式下仅活跃节点对外提供服务。
 */
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService
        implements Recoverable, ResourceManagerMXBean {

  /**
   * ResourceManager 关闭钩子优先级，越高越先执行关闭。
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  /**
   * ID生成时使用的Epoch位偏移量。
   */
  public static final int EPOCH_BIT_SHIFT = 40;

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceManager.class);
  private static final Marker FATAL = MarkerFactory.getMarker("FATAL");
  private static long clusterTimeStamp = System.currentTimeMillis();

  /*
   * UI2 webapp 名称
   */
  public static final String UI2_WEBAPP_NAME = "/ui2";

  /*
   * 调度器UI webapp 名称
   */
  public static final String SCHEDULER_UI_WEBAPP_NAME = "/scheduler-ui";

  /**
   * "Always On" 服务，无论RM处于何种HA状态都需要持续运行的服务。
   */
  @VisibleForTesting
  protected RMContextImpl rmContext;
  private Dispatcher rmDispatcher;
  @VisibleForTesting
  protected AdminService adminService;

  /**
   * "Active" 服务，仅活跃RM需要运行的服务，包括客户端接口、调度器、应用管理器等核心业务组件。
   * 当HA启用且当前RM为standby，或HA未启用时始终为活跃状态。
   * 这些服务由{@link CompositeService} RMActiveServices 统一管理其初始化、启动和停止。
   */
  protected RMActiveServices activeServices;
  protected RMSecretManagerService rmSecretManagerService;

  protected ResourceScheduler scheduler;
  protected ReservationSystem reservationSystem;
  private ClientRMService clientRM;
  protected ApplicationMasterService masterService;
  protected NMLivelinessMonitor nmLivelinessMonitor;
  protected NodesListManager nodesListManager;
  protected RMAppManager rmAppManager;
  protected ApplicationACLsManager applicationACLsManager;
  protected QueueACLsManager queueACLsManager;
  private FederationStateStoreService federationStateStoreService;
  private ProxyCAManager proxyCAManager;
  private WebApp webApp;
  private AppReportFetcher fetcher = null;
  protected ResourceTrackerService resourceTracker;
  private JvmMetrics jvmMetrics;
  private boolean curatorEnabled = false;
  private ZKCuratorManager zkManager;
  private final String zkRootNodePassword =
      Long.toString(new SecureRandom().nextLong());
  private boolean recoveryEnabled;

  @VisibleForTesting
  protected String webAppAddress;
  private ConfigurationProvider configurationProvider = null;
  /** End of Active services */

  private Configuration conf;

  private UserGroupInformation rmLoginUGI;

  public ResourceManager() {
    super("ResourceManager");
  }

  public RMContext getRMContext() {
    return this.rmContext;
  }

  public static long getClusterTimeStamp() {
    return clusterTimeStamp;
  }

  public String getRMLoginUser() {
    return rmLoginUGI.getShortUserName();
  }

  private RMInfo rmStatusInfoBean;

  @VisibleForTesting
  protected static void setClusterTimeStamp(long timestamp) {
    clusterTimeStamp = timestamp;
  }

  @VisibleForTesting
  Dispatcher getRmDispatcher() {
    return rmDispatcher;
  }

  @VisibleForTesting
  protected ResourceProfilesManager createResourceProfileManager() {
    ResourceProfilesManager resourceProfilesManager =
        new ResourceProfilesManagerImpl();
    return resourceProfilesManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.conf = conf;
    UserGroupInformation.setConfiguration(conf);
    this.rmContext = new RMContextImpl();
    rmContext.setResourceManager(this);
    rmContext.setYarnConfiguration(conf);

    // 注册RM运行状态JMX监控Bean
    rmStatusInfoBean = new RMInfo(this);
    rmStatusInfoBean.register();

    // HA配置需要在登录前完成设置
    this.rmContext.setHAEnabled(HAUtil.isHAEnabled(this.conf));
    if (this.rmContext.isHAEnabled()) {
      HAUtil.verifyAndSetConfiguration(this.conf);
    }

    // 设置UGI并完成安全登录：安全启用则使用登录用户，否则使用当前用户
    this.rmLoginUGI = UserGroupInformation.getCurrentUser();
    try {
      doSecureLogin();
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to login", ie);
    }

    this.configurationProvider =
        ConfigurationProviderFactory.getConfigurationProvider(conf);
    this.configurationProvider.init(this.conf);
    rmContext.setConfigurationProvider(configurationProvider);

    // 加载core-site.xml配置
    loadConfigurationXml(YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);

    // 加载完core-site后刷新代理用户配置，如果存在RM特定配置则覆盖公共配置
    RMServerUtils.processRMProxyUsersConf(conf);
    ProxyUsers.refreshSuperUserGroupsConfiguration(this.conf);

    // 加载yarn-site.xml配置
    loadConfigurationXml(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);

    // 配置合法性校验
    validateConfigs(this.conf);

    // 为所有AlwaysOn服务注册事件处理器，初始化事件分发器
    rmDispatcher = setupDispatcher();
    addIfService(rmDispatcher);
    rmContext.setDispatcher(rmDispatcher);

    // 服务添加顺序不能改变，启动顺序与添加顺序一致
    // 选举服务需要admin服务先完成初始化启动，因此先添加admin服务再添加选举服务
    adminService = createAdminService();
    addService(adminService);
    rmContext.setRMAdminService(adminService);

    // 选举服务必须在admin服务之后添加
    if (this.rmContext.isHAEnabled()) {
      // 如果配置了嵌入式主节点选举，则初始化选举器
      if (HAUtil.isAutomaticFailoverEnabled(conf)
          && HAUtil.isAutomaticFailoverEmbedded(conf)) {
        EmbeddedElector elector = createEmbeddedElector();
        addIfService(elector);
        rmContext.setLeaderElectorService(elector);
      }
    }

    // 创建并初始化活跃服务（非HA场景或已切换到活跃时使用）
    createAndInitActiveServices(false);

    webAppAddress = WebAppUtils.getWebAppBindURL(this.conf,
                      YarnConfiguration.RM_BIND_HOST,
                      WebAppUtils.getRMWebAppURLWithoutScheme(this.conf));

    RMApplicationHistoryWriter rmApplicationHistoryWriter =
        createRMApplicationHistoryWriter();
    addService(rmApplicationHistoryWriter);
    rmContext.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

    // 先初始化RM时间线收集器，让系统指标发布器可以绑定到它
    if (YarnConfiguration.timelineServiceV2Enabled(this.conf)) {
      RMTimelineCollectorManager timelineCollectorManager =
          createRMTimelineCollectorManager();
      addService(timelineCollectorManager);
      rmContext.setRMTimelineCollectorManager(timelineCollectorManager);
    }

    SystemMetricsPublisher systemMetricsPublisher =
        createSystemMetricsPublisher();
    addIfService(systemMetricsPublisher);
    rmContext.setSystemMetricsPublisher(systemMetricsPublisher);

    // 注册RM JMX监控Bean
    registerMXBean();

    super.serviceInit(this.conf);
  }

  private void loadConfigurationXml(String configurationFile)
      throws YarnException, IOException {
    InputStream configurationInputStream =
        this.configurationProvider.getConfigurationInputStream(this.conf,
            configurationFile);
    if (configurationInputStream != null) {
      this.conf.addResource(configurationInputStream, configurationFile);
    }
  }

  protected EmbeddedElector createEmbeddedElector() throws IOException {
    EmbeddedElector elector;
    curatorEnabled =
        conf.getBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR,
            YarnConfiguration.DEFAULT_CURATOR_LEADER_ELECTOR_ENABLED);
    if (curatorEnabled) {
      this.zkManager = createAndStartZKManager(conf);
      elector = new CuratorBasedElectorService(this);
    } else {
      elector = new