
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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLocalizationStatusesResponse;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.apache.hadoop.yarn.metrics.GenericEventTypeMetrics;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.UpdateContainerTokenEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerTokenUpdatedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.RecoveryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidAuxServiceException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerException;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationACLMapProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.FlowContextProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.AuxiliaryLocalPathHandler;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.GenericEventTypeMetricsManager;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrUpdateContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrSignalContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerInitEvent;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationFinishEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl.FlowContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerReInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.AbstractContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.SignalContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceSet;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.LogAggregationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.NonAggregatingLogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerScheduler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEventType;

import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredApplicationsState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerType;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import static org.apache.hadoop.service.Service.STATE.STARTED;

/**
 * 容器管理器实现类 - NodeManager的核心组件
 * 负责管理和协调容器的生命周期，包括启动、停止、监控和资源分配
 * 实现了ContainerManagementProtocol接口，处理来自ApplicationMaster的RPC请求
 */
public class ContainerManagerImpl extends CompositeService implements
    ContainerManager {

  private enum ReInitOp {
    RE_INIT, COMMIT, ROLLBACK, LOCALIZE;
  }
  
  /**
   * 关闭时等待应用程序清理的额外时间（毫秒）
   */
  private static final int SHUTDOWN_CLEANUP_SLOP_MS = 1000;

  private static final Logger LOG =
       LoggerFactory.getLogger(ContainerManagerImpl.class);

  /** 无效NMToken错误消息 */
  public static final String INVALID_NMTOKEN_MSG = "Invalid NMToken";
  /** 无效容器令牌错误消息 */
  static final String INVALID_CONTAINERTOKEN_MSG =
      "Invalid ContainerToken";

  /** NodeManager上下文对象，包含运行时状态信息 */
  protected final Context context;
  /** 容器监控服务，监控容器资源使用情况 */
  private final ContainersMonitor containersMonitor;
  /** RPC服务器，处理容器管理协议请求 */
  private Server server;
  /** 资源本地化服务，负责下载和管理容器所需的资源 */
  private final ResourceLocalizationService rsrcLocalizationSrvc;
  /** 容器启动器抽象类，负责启动容器进程 */
  private final AbstractContainersLauncher containersLauncher;
  /** 辅助服务管理器，管理各种辅助服务 */
  private final AuxServices auxiliaryServices;
  /** NodeManager指标收集器 */
  @VisibleForTesting final NodeManagerMetrics metrics;

  /** 节点状态更新器，负责与ResourceManager通信 */
  protected final NodeStatusUpdater nodeStatusUpdater;

  /** 本地目录处理器服务 */
  protected LocalDirsHandlerService dirsHandler;
  /** 异步事件分发器 */
  private AsyncDispatcher dispatcher;

  /** 删除服务，负责清理临时文件和目录 */
  private final DeletionService deletionService;
  /** 日志处理器 */
  private LogHandler logHandler;
  /** 服务是否已停止的标志 */
  private boolean serviceStopped = false;
  /** 读锁 */
  private final ReadLock readLock;
  /** 写锁 */
  private final WriteLock writeLock;
  /** AM-RM代理服务 */
  private AMRMProxyService amrmProxyService;
  /** AM-RM代理是否启用的标志 */
  protected boolean amrmProxyEnabled = false;
  /** 容器调度器，负责任务调度和资源分配 */
  private final ContainerScheduler containerScheduler;

  /** 关闭时等待容器完成的超时时间（毫秒） */
  private long waitForContainersOnShutdownMillis;

  /** NM指标发布器（仅在启用时间线服务v2时设置） */
  private NMTimelinePublisher nmMetricsPublisher;
  /** 时间线服务v2是否启用的标志 */
  private boolean timelineServiceV2Enabled;
  /** NM分发器指标是否启用的标志 */
  private boolean nmDispatherMetricEnabled;

  /**
   * 构造ContainerManagerImpl实例
   * 
   * @param context NodeManager上下文
   * @param exec 容器执行器
   * @param deletionContext 删除服务上下文
   * @param nodeStatusUpdater 节点状态更新器
   * @param metrics NodeManager指标收集器
   * @param dirsHandler 本地目录处理器服务
   */
  public ContainerManagerImpl(Context context, ContainerExecutor exec,
      DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater,
      NodeManagerMetrics metrics, LocalDirsHandlerService dirsHandler) {
    super(ContainerManagerImpl.class.getName());
    this.context = context;
    this.dirsHandler = dirsHandler;

    // 创建ContainerManager级别的事件分发器
    dispatcher = createContainerManagerDispatcher();
    this.deletionService = deletionContext;
    this.metrics = metrics;

    // 创建并添加资源本地化服务
    rsrcLocalizationSrvc =
        createResourceLocalizationService(exec, deletionContext, context,
            metrics);
    addService(rsrcLocalizationSrvc);

    // 创建并添加容器启动器
    containersLauncher = createContainersLauncher(context, exec);
    addService(containersLauncher);

    this.nodeStatusUpdater = nodeStatusUpdater;
    // 创建并添加容器调度器
    this.containerScheduler = createContainerScheduler(context);
    addService(containerScheduler);

    // 初始化辅助服务
    AuxiliaryLocalPathHandler auxiliaryLocalPathHandler =
        new AuxiliaryLocalPathHandlerImpl(dirsHandler);
    auxiliaryServices = new AuxServices(auxiliaryLocalPathHandler,
        this.context, this.deletionService);
    auxiliaryServices.registerServiceListener(this);
    context.setAuxServices(auxiliaryServices);
    addService(auxiliaryServices);

    // 初始化时间线服务v2指标发布器
    Configuration conf = context.getConf();
    if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
      if (YarnConfiguration.systemMetricsPublisherEnabled(conf)) {
        LOG.info("YARN system metrics publishing service is enabled");
        nmMetricsPublisher = createNMTimelinePublisher(context);
        context.setNMTimelinePublisher(nmMetricsPublisher);
      }
      this.timelineServiceV2Enabled = true;
    }
    // 创建并添加容器监控服务
    this.containersMonitor = createContainersMonitor(exec);
    addService(this.containersMonitor);

    // 注册事件处理器
    dispatcher.register(ContainerEventType.class,
        new ContainerEventDispatcher());
    dispatcher.register(ApplicationEventType.class,
        createApplicationEventDispatcher());
    dispatcher.register(LocalizationEventType.class,
        new LocalizationEventHandlerWrapper(rsrcLocalizationSrvc,
            nmMetricsPublisher));
    dispatcher.register(AuxServicesEventType.class, auxiliaryServices);
    dispatcher.register(ContainersMonitorEventType.class, containersMonitor);
    dispatcher.register(ContainersLauncherEventType.class, containersLauncher);
    dispatcher.register(ContainerSchedulerEventType.class, containerScheduler);

    addService(dispatcher);

    // 初始化读写锁
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
  }

  /**
   * 服务初始化方法
   * 初始化日志处理器、共享缓存上传服务、AM-RM代理服务等组件
   * 
   * @param conf 配置对象
   * @throws Exception 初始化失败时抛出异常
   */
  @Override
  public void serviceInit(Configuration conf) throws Exception {

    // 创建并添加日志处理器
    logHandler =
      createLogHandler(conf, this.context, this.deletionService);
    addIfService(logHandler);
    dispatcher.register(LogHandlerEventType.class, logHandler);
    
    // 添加共享缓存上传服务（如果共享缓存被禁用，则不执行任何操作）
    SharedCacheUploadService sharedCacheUploader =
        createSharedCacheUploaderService();
    addService(sharedCacheUploader);
    dispatcher.register(SharedCacheUploadEventType.class, sharedCacheUploader);

    // 创建AM-RM代理服务
    createAMRMProxyService(conf);

    // 计算关闭时等待容器完成的超时时间
    waitForContainersOnShutdownMillis =
        conf.getLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
            YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS) +
        conf.getLong(YarnConfiguration.NM_PROCESS_KILL_WAIT_MS,
            YarnConfiguration.DEFAULT_NM_PROCESS_KILL_WAIT_MS) +
        SHUTDOWN_CLEANUP_SLOP_MS;

    // 检查NM分发器指标是否启用
    nmDispatherMetricEnabled = conf.getBoolean(
        YarnConfiguration.NM_DISPATCHER_METRIC_ENABLED,
        YarnConfiguration.DEFAULT_NM_DISPATCHER_METRIC_ENABLED);

    super.serviceInit(conf);
    // 执行恢复操作
    recover();
  }

  /**
   * 创建ContainerManager调度器
   * 如果启用了NM分发器指标，则为各种事件类型添加指标收集
   * 
   * @return 配置好的AsyncDispatcher实例
   */
  @SuppressWarnings("unchecked")
  protected AsyncDispatcher createContainerManagerDispatcher() {
    dispatcher = new AsyncDispatcher("NM ContainerManager dispatcher");

    // 如果不启用NM分发器指标，直接返回调度器
    if (!nmDispatherMetricEnabled) {
      return dispatcher;
    }

    // 为各种事件类型添加指标收集器
    GenericEventTypeMetrics<ContainerEventType> containerEventTypeMetrics =
        GenericEventTypeMetricsManager.create(dispatcher.getName(), ContainerEventType.class);
    dispatcher.addMetrics(containerEventTypeMetrics, containerEventTypeMetrics.getEnumClass());

    GenericEventTypeMetrics<LocalizationEventType> localizationEventTypeMetrics =
        GenericEventTypeMetricsManager.create(dispatcher.getName(), LocalizationEventType.class);
    dispatcher.addMetrics(localizationEventTypeMetrics,
        localizationEventTypeMetrics.getEnumClass());

    GenericEventTypeMetrics<ApplicationEventType> applicationEventTypeMetrics =
        GenericEventTypeMetricsManager.create(dispatcher.getName(), ApplicationEventType.class);
    dispatcher.addMetrics(applicationEventTypeMetrics,
        applicationEventTypeMetrics.getEnumClass());

    GenericEventTypeMetrics<ContainersLauncherEventType> containersLauncherEventTypeMetrics =
        GenericEventTypeMetricsManager.create(dispatcher.getName(),
        ContainersLauncherEventType.class);
    dispatcher.addMetrics(containersLauncherEventTypeMetrics,
        containersLauncherEventTypeMetrics.getEnumClass());

    GenericEventTypeMetrics<ContainerSchedulerEventType> containerSchedulerEventTypeMetrics =
        GenericEventTypeMetricsManager.create(dispatcher.getName(),
        ContainerSchedulerEventType.class);
    dispatcher.addMetrics(containerSchedulerEventTypeMetrics,
        containerSchedulerEventTypeMetrics.getEnumClass());

    GenericEventTypeMetrics<ContainersMonitorEventType> containersMonitorEventTypeMetrics =
        GenericEventTypeMetricsManager.create(dispatcher.getName(),
        ContainersMonitorEventType.class);
    dispatcher.addMetrics(containersMonitorEventTypeMetrics,
        containersMonitorEventTypeMetrics.getEnumClass());

    GenericEventTypeMetrics<AuxServicesEventType> auxServicesEventTypeTypeMetrics =
        GenericEventTypeMetricsManager.create(dispatcher.getName(), AuxServicesEventType.class);
    dispatcher.addMetrics(auxServicesEventTypeTypeMetrics,
        auxServicesEventTypeTypeMetrics.getEnumClass());

    GenericEventTypeMetrics<LocalizerEventType> localizerEventTypeMetrics =
        GenericEventTypeMetricsManager.create(dispatcher.getName(), LocalizerEventType.class);
    dispatcher.addMetrics(localizerEventTypeMetrics, localizerEventTypeMetrics.getEnumClass());
    LOG.info("NM ContainerManager dispatcher Metric Initialization Completed.");

    return dispatcher;
  }

  /**
   * 创建AM-RM代理服务
   * 根据配置决定是否启用AM-RM代理和分布式调度
   * 
   * @param conf 配置对象
   */
  protected void createAMRMProxyService(Configuration conf) {
    // 检查是否启用AM-RM代理或分布式调度
    this.amrmProxyEnabled =
        conf.getBoolean(YarnConfiguration.AMRM_PROXY_ENABLED,
            YarnConfiguration.DEFAULT_AMRM_PROXY_ENABLED) ||
            conf.getBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED,
                YarnConfiguration.DEFAULT_DIST_SCHEDULING_ENABLED);

    if (amrmProxyEnabled) {
      LOG.info("AMRMProxyService is enabled. "
          + "All the AM->RM requests will be intercepted by the proxy");
      // 创建并添加AM-RM代理服务
      this.setAMRMProxyService(
          new AMRMProxyService(this.context, this.dispatcher));
      addService(this.getAMRMProxyService());
    } else {
      LOG.info("AMRMProxyService is disabled");
    }
  }

  /**
   * 创建容器调度器
   * 目前调度器与ContainerManager共享事件分发器
   * 
   * @param cntxt NodeManager上下文
   * @return 新创建的ContainerScheduler实例
   */
  @VisibleForTesting
  protected ContainerScheduler createContainerScheduler(Context cntxt) {
    // 目前，这个调度器与ContainerManager、所有容器、容器监控器共享
    // 容器调度器可以使用自己的调度器
    return new ContainerScheduler(cntxt, dispatcher, metrics);
  }

  /**
   * 创建容器监控器
   * 
   * @param exec 容器执行器
   * @return 新创建的ContainersMonitorImpl实例
   */
  protected ContainersMonitor createContainersMonitor(ContainerExecutor exec) {
    return new ContainersMonitorImpl(exec, dispatcher, this.context);
  }

  /**
   * 恢复NodeManager状态
   * 从持久化存储中恢复应用程序和容器的状态
   * 
   * @throws IOException IO异常
   * @throws URISyntaxException URI语法异常
   */
  @SuppressWarnings("unchecked")
  private void recover() throws IOException, URISyntaxException {
    NMStateStoreService stateStore = context.getNMStateStore();
    if (stateStore.canRecover()) {
      // 恢复本地化资源状态
      rsrcLocalizationSrvc.recoverLocalizedResources(
          stateStore.loadLocalizationState());

      // 恢复应用程序状态
      RecoveredApplicationsState appsState = stateStore.loadApplicationsState();
      try (RecoveryIterator<ContainerManagerApplicationProto> rasIterator =
               appsState.getIterator()) {
        while (rasIterator.hasNext()) {
          ContainerManagerApplicationProto proto = rasIterator.next();
          LOG.debug("Recovering application with state: {}", proto);
          recoverApplication(proto);
        }
      }

      // 恢复容器状态
      try (RecoveryIterator<RecoveredContainerState> rcsIterator =
               stateStore.getContainerStateIterator()) {
        while (rcsIterator.hasNext()) {
          RecoveredContainerState rcs = rcsIterator.next();
          LOG.debug("Recovering container with state: {}", rcs);
          recoverContainer(rcs);
        }
      }

      // 在应用程序和容器恢复后恢复AM-RM代理状态
      if (this.amrmProxyEnabled) {
        this.getAMRMProxyService().recover();
      }

      // 分发恢复完成事件，使暂停、调度和排队的容器可以在资源可用时执行
      dispatcher.getEventHandler().handle(
          new ContainerSchedulerEvent(null,
              ContainerSchedulerEventType.RECOVERY_COMPLETED));
    } else {
      LOG.info("Not a recoverable state store. Nothing to recover.");
    }
  }

  /**
   * 恢复应用程序状态
   * 
   * @param p 应用程序协议缓冲区对象
   * @throws IOException IO异常
   */
  private void recoverApplication(ContainerManagerApplicationProto p)
      throws IOException {
    ApplicationId appId = new ApplicationIdPBImpl(p.getId());
    // 读取凭证信息
    Credentials creds = new Credentials();
    creds.readTokenStorageStream(
        new DataInputStream(p.getCredentials().newInput()));

    // 处理访问控制列表
    List<ApplicationACLMapProto> aclProtoList = p.getAclsList();
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(aclProtoList.size());
    for (ApplicationACLMapProto aclProto : aclProtoList) {
      acls.put(ProtoUtils.convertFromProtoFormat(aclProto.getAccessType()),
          aclProto.getAcl());
    }

    // 处理日志聚合上下文
    LogAggregationContext logAggregationContext = null;
    if (p.getLogAggregationContext() != null) {
      logAggregationContext =
          new LogAggregationContextPBImpl(p.getLogAggregationContext());
    }

    // 处理流上下文
    FlowContext fc = null;
    if (p.getFlowContext() != null) {
      FlowContextProto fcp = p.getFlowContext();
      fc = new FlowContext(fcp.getFlowName(), fcp.getFlowVersion(),
          fcp.getFlowRunId());
      LOG.debug(
          "Recovering Flow context: {} for an application {}", fc, appId);
    } else {
      // 升级情况下，如果没有现有的流上下文，使用默认值
      fc = new FlowContext(TimelineUtils.generateDefaultFlowName(null, appId),
          YarnConfiguration.DEFAULT_FLOW_VERSION, appId.getClusterTimestamp());
      LOG.debug(
          "No prior existing flow context found. Using default Flow context: "
          + "{} for an application {}", fc, appId);
    }

    LOG.info("Recovering application " + appId);
    // 创建应用程序实例并添加到上下文
    ApplicationImpl app = new ApplicationImpl(dispatcher, p.getUser(), fc,
        appId, creds, context, p.getAppLogAggregationInitedTime());
    context.getApplications().put(appId, app);
    metrics.runningApplication();
    // 发送应用程序初始化事件
    app.handle(new ApplicationInitEvent(appId, acls, logAggregationContext));
  }

  /**
   * 恢复容器状态
   * 
   * @param rcs 恢复的容器状态
   * @throws IOException IO异常
   */
  private void recoverContainer(RecoveredContainerState rcs)
      throws IOException {
    StartContainerRequest req = rcs.getStartRequest();
    ContainerLaunchContext launchContext = req.getContainerLaunchContext();
    ContainerTokenIdentifier token;
    
    // 处理容器能力信息
    if(rcs.getCapability() != null) {
      ContainerTokenIdentifier originalToken =
          BuilderUtils.newContainerTokenIdentifier(req.getContainerToken());
      token = new ContainerTokenIdentifier(originalToken.getContainerID(),
          originalToken.getVersion(), originalToken.getNmHostAddress(),
          originalToken.getApplicationSubmitter(), rcs.getCapability(),
          originalToken.getExpiryTimeStamp(), originalToken.getMasterKeyId(),
          originalToken.getRMIdentifier(), originalToken.getPriority(),
          originalToken.getCreationTime(),
          originalToken.getLogAggregationContext(),
          originalToken.getNodeLabelExpression(),
          originalToken.getContainerType(), originalToken.getExecutionType(),
          originalToken.getAllocationRequestId(),
          originalToken.getAllcationTags());

    } else {
      token = BuilderUtils.newContainerTokenIdentifier(req.getContainerToken());
    }

    ContainerId containerId = token.getContainerID();
    ApplicationId appId =
        containerId.getApplicationAttemptId().getApplicationId();

    LOG.info("Recovering " + containerId + " in state " + rcs.getStatus()
        + " with exit code " + rcs.getExitCode());

    // 查找对应的应用程序
    Application app = context.getApplications().get(appId);
    if (app != null) {
      // 恢复活跃容器
      recoverActiveContainer(app, launchContext, token, rcs);
      // 如果需要杀死容器
      if (rcs.getRecoveryType() == RecoveredContainerType.KILL) {
        dispatcher.getEventHandler().handle(
            new ContainerKillEvent(containerId, ContainerExitStatus.ABORTED,
                "Due to invalid StateStore info container was killed"
                    + " during recovery"));
      }
    } else {
      if (rcs.getStatus() != RecoveredContainerStatus.COMPLETED) {
        LOG.warn(containerId + " has no corresponding application!");
      }
      LOG.info("Adding " + containerId + " to recently stopped containers");
      nodeStatusUpdater.addCompletedContainer(containerId);
    }
  }

  /**
   * 恢复活跃容器
   * 
   * @param app 应用程序对象
   * @param launchContext 容器启动上下文
   * @param token 容器令牌标识符
   * @param rcs 恢复的容器状态
   * @throws IOException IO异常
   */
  @SuppressWarnings("unchecked")
  protected void recoverActiveContainer(Application app,
      ContainerLaunchContext launchContext, ContainerTokenIdentifier token,
      RecoveredContainerState rcs) throws IOException {
    // 解析凭证信息
    Credentials credentials = YarnServerSecurityUtils.parseCredentials(
        launchContext);
    // 创建容器实现实例
    Container container = new ContainerImpl(getConfig(), dispatcher,
        launchContext, credentials, metrics, token, context, rcs);
    // 将容器添加到上下文
    context.getContainers().put(token.getContainerID(), container);
    // 容器调度器恢复活跃容器
    containerScheduler.recoverActiveContainer(container, rcs);
    // 发送应用程序容器初始化事件
    app.handle(new ApplicationContainerInitEvent(container));
  }

  /**
   * 等待恢复的容器完成初始化
   * 
   * @throws InterruptedException 线程中断异常
   */
  private void waitForRecoveredContainers() throws InterruptedException {
    final int sleepMsec = 100;  // 睡眠间隔（毫秒）
    int waitIterations = 100;   // 最大等待迭代次数
    List<ContainerId> newContainers = new ArrayList<ContainerId>();
    
    while (--waitIterations >= 0) {
      newContainers.clear();
      // 检查所有容器中状态为NEW的容器
      for (Container container : context.getContainers().values()) {
        if (container.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.NEW) {
          newContainers.add(container.getContainerId());
        }
      }
      // 如果没有新容器，跳出循环
      if (newContainers.isEmpty()) {
        break;
      }
      LOG.info("Waiting for containers: " + newContainers);
      Thread.sleep(sleepMsec);
    }
    
    // 如果超时，记录警告
    if (waitIterations < 0) {
      LOG.warn("Timeout waiting for recovered containers");
    }
  }

  /**
   * 创建日志处理器
   * 根据配置决定创建聚合日志服务还是非聚合日志处理器
   * 
   * @param conf 配置对象
   * @param context NodeManager上下文
   * @param deletionService 删除服务
   * @return 日志处理器实例
   */
  protected LogHandler createLogHandler(Configuration conf, Context context,
      DeletionService deletionService) {
    // 如果启用了日志聚合，创建日志聚合服务
    if (conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      return new LogAggregationService(this.dispatcher, context,
          deletionService, dirsHandler);
    } else {
      // 否则创建非聚合日志处理器
      return new NonAggregatingLogHandler(this.dispatcher, deletionService,
                                          dirsHandler,
                                          context.getNMStateStore());
    }
  }

  /**
   * 获取容器监控器
   * 
   * @return 容器监控器实例
   */
  @Override
  public ContainersMonitor getContainersMonitor() {
    return this.containersMonitor;
  }

  /**
   * 创建资源本地化服务
   * 
   * @param exec 容器执行器
   * @param deletionContext 删除服务上下文
   * @param nmContext NodeManager上下文
   * @param nmMetrics NodeManager指标收集器
   * @return 资源本地化服务实例
   */
  protected ResourceLocalizationService createResourceLocalizationService(
      ContainerExecutor exec, DeletionService deletionContext,
      Context nmContext, NodeManagerMetrics nmMetrics) {
    return new ResourceLocalizationService(this.dispatcher, exec,
        deletionContext, dirsHandler, nmContext, nmMetrics);
  }

  /**
   * 创建共享缓存上传服务
   * 
   * @return 共享缓存上传服务实例
   */
  protected SharedCacheUploadService createSharedCacheUploaderService() {
    return new SharedCacheUploadService();
  }

  /**
   * 创建NM时间线发布器
   * 
   * @param ctxt NodeManager上下文
   * @return NM时间线发布器实例
   */
  @VisibleForTesting
  protected NMTimelinePublisher createNMTimelinePublisher(Context ctxt) {
    NMTimelinePublisher nmTimelinePublisherLocal =
        new NMTimelinePublisher(ctxt);
    addIfService(nmTimelinePublisherLocal);
    return nmTimelinePublisherLocal;
  }

  /**
   * 创建容器启动器
   * 通过反射机制创建配置的容器启动器类实例
   * 
   * @param ctxt NodeManager上下文
   * @param exec 容器执行器
   * @return 容器启动器实例
   */
  protected AbstractContainersLauncher createContainersLauncher(
      Context ctxt, ContainerExecutor exec) {
    // 从配置中获取容器启动器类
    Class<? extends AbstractContainersLauncher> containersLauncherClass =
        ctxt.getConf()
            .getClass(YarnConfiguration.NM_CONTAINERS_LAUNCHER_CLASS,
                ContainersLauncher.class, AbstractContainersLauncher.class);
    AbstractContainersLauncher launcher;
    try {
      // 通过反射创建实例
      launcher = ReflectionUtils.newInstance(containersLauncherClass,
          ctxt.getConf());
      // 初始化启动器
      launcher.init(ctxt, this.dispatcher, exec, dirsHandler, this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return launcher;
  }

  /**
   * 创建应用程序事件分发器
   * 
   * @return 应用程序事件分发器实例
   */
  protected EventHandler<ApplicationEvent> createApplicationEventDispatcher() {
    return new ApplicationEventDispatcher();
  }

  /**
   * 服务启动方法
   * 初始化RPC服务器，设置安全认证，启动各个服务组件
   * 
   * @throws Exception 启动失败时抛出异常
   */
  @Override
  protected void serviceStart() throws Exception {

    Configuration conf = getConfig();
    // 获取初始地址配置
    final InetSocketAddress initialAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_PORT);
    boolean usingEphemeralPort = (initialAddress.getPort() == 0);
    
    // 检查恢复模式下不能使用临时端口
    if (context.getNMStateStore().canRecover() && usingEphemeralPort) {
      throw new IllegalArgumentException("Cannot support recovery with an "
          + "ephemeral server port. Check the setting of "
          + YarnConfiguration.NM_ADDRESS);
    }
    
    // 如果需要恢复，延迟打开RPC服务直到资源和容器恢复完成
    final boolean delayedRpcServerStart =
        context.getNMStateStore().canRecover();

    Configuration serverConf = new Configuration(conf);

    // 强制使用基于令牌的身份验证
    serverConf.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      SaslRpcServer.AuthMethod.TOKEN.toString());
    
    YarnRPC rpc = YarnRPC.create(conf);

    // 创建RPC服务器
    server =
        rpc.getServer(ContainerManagementProtocol.class, this, initialAddress, 
            serverConf, this.context.getNMTokenSecretManager(),
            conf.getInt(YarnConfiguration.NM_CONTAINER_MGR_THREAD_COUNT, 
                YarnConfiguration.DEFAULT_NM_CONTAINER_MGR_THREAD_COUNT));
    
    // 启用服务授权（如果配置了）
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, NMPolicyProvider.getInstance());
    }
    
    // 处理主机绑定配置
    String bindHost = conf.get(YarnConfiguration.NM_BIND_HOST);
    String nmAddress = conf.getTrimmed(YarnConfiguration.NM_ADDRESS);
    String hostOverride = null;
    if (bindHost != null && !bindHost.isEmpty()
        && nmAddress != null && !nmAddress.isEmpty()) {
      hostOverride = nmAddress.split(":")[0];
    }

    // 设置节点ID
    InetSocketAddress connectAddress;
    if (delayedRpcServerStart) {
      connectAddress = NetUtils.getConnectAddress(initialAddress);
    } else {
      server.start();
      connectAddress = NetUtils.getConnectAddress(server);
    }
    NodeId nodeId = buildNodeId(connectAddress, hostOverride);
    ((NodeManager.NMContext)context).setNodeId(nodeId);
    this.context.getNMTokenSecretManager().setNodeId(nodeId);
    this.context.getContainerTokenSecretManager().setNodeId(nodeId);

    // 启动剩余服务
    super.serviceStart();

    // 如果是延迟启动RPC服务器
    if (delayedRpcServerStart) {
      waitForRecoveredContainers();
      server.start();

      // 检查节点ID是否与之前广告的一致
      connectAddress = NetUtils.getConnectAddress(server);
      NodeId serverNode = buildNodeId(connectAddress, hostOverride);
      if (!serverNode.equals(nodeId)) {
        throw new IOException("Node mismatch after server started, expected '"
            + nodeId + "' but found '" + serverNode + "'");
      }
    }

    LOG.info("ContainerManager started at " + connectAddress);
    LOG.info("ContainerManager bound to " + initialAddress);
  }

  /**
   * 构建节点ID
   * 
   * @param connectAddress 连接地址
   * @param hostOverride 主机覆盖名称
   * @return 节点ID对象
   */
  private NodeId buildNodeId(InetSocketAddress connectAddress,
      String hostOverride) {
    if (hostOverride != null) {
      // 如果有主机覆盖，使用覆盖的主机名
      connectAddress = NetUtils.getConnectAddress(
          new InetSocketAddress(hostOverride, connectAddress.getPort()));
    }
    // 创建并返回节点ID
    return NodeId.newInstance(
        connectAddress.getAddress().getCanonicalHostName(),
        connectAddress.getPort());
  }

  /**
   * 刷新服务访问控制列表
   * 
   * @param configuration 配置对象
   * @param policyProvider 策略提供者
   */
  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  /**
   * 服务停止方法
   * 清理应用程序，停止服务和RPC服务器
   * 
   * @throws Exception 停止失败时抛出异常
   */
  @Override
  public void serviceStop() throws Exception {
    this.writeLock.lock();
    try {
      serviceStopped = true;
      if (context != null) {
        // 在NM关闭时清理应用程序
        cleanUpApplicationsOnNMShutDown();
      }
    } finally {
      this.writeLock.unlock();
    }
    // 注销辅助服务监听器
    if (auxiliaryServices.getServiceState() == STARTED) {
      auxiliaryServices.unregisterServiceListener(this);
    }
    // 停止RPC服务器
    if (server != null) {
      server.stop();
    }
    super.serviceStop();
  }

  /**
   * 在NodeManager关闭时清理应用程序
   * 发送应用程序完成事件并等待应用程序结束
   */
  public void cleanUpApplicationsOnNMShutDown() {
    Map<ApplicationId, Application> applications =
        this.context.getApplications();
    if (applications.isEmpty()) {
      return;
    }
    LOG.info("Applications still running : " + applications.keySet());

    // 检查恢复模式和监管模式
    if (this.context.getNMStateStore().canRecover()
        && !this.context.getDecommissioned()) {
      if (getConfig().getBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED,
          YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED)) {
        // 监督恢复模式下不清理应用程序，因为它们可以在重启时恢复
        return;
      }
    }

    // 发送应用程序完成事件
    List<ApplicationId> appIds =
        new ArrayList<ApplicationId>(applications.keySet());
    this.handle(new CMgrCompletedAppsEvent(appIds,
            CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN));

    LOG.info("Waiting for Applications to be Finished");

    // 等待应用程序完成
    long waitStartTime = System.currentTimeMillis();
    while (!applications.isEmpty()
        && System.currentTimeMillis() - waitStartTime < waitForContainersOnShutdownMillis) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        LOG.warn(
          "Interrupted while sleeping on applications finish on shutdown", ex);
      }
    }

    // 记录应用程序状态
    if (applications.isEmpty()) {
      LOG.info("All applications in FINISHED state");
    } else {
      LOG.info("Done waiting for Applications to be Finished. Still alive: "
          + applications.keySet());
    }
  }

  /**
   * 在NodeManager与ResourceManager重新同步时清理容器
   * 发送容器完成事件并等待容器结束
   */
  public void cleanupContainersOnNMResync() {
    Map<ContainerId, Container> containers = context.getContainers();
    if (containers.isEmpty()) {
      return;
    }
    LOG.info("Containers still running on "
        + CMgrCompletedContainersEvent.Reason.ON_NODEMANAGER_RESYNC + " : "
        + containers.keySet());

    // 获取所有容器ID并发送完成事件
    List<ContainerId> containerIds =
      new ArrayList<ContainerId>(containers.keySet());

    LOG.info("Waiting for containers to be killed");

    this.handle(new CMgrCompletedContainersEvent(containerIds,
      CMgrCompletedContainersEvent.Reason.ON_NODEMANAGER_RESYNC));

    /*
     * 等待所有容器状态变为COMPLETE
     * 不会从NM上下文中移除容器状态，因为这些状态在NodeManager重新注册到ResourceManager时会用到
     */
    boolean allContainersCompleted = false;
    while (!containers.isEmpty() && !allContainersCompleted) {
      allContainersCompleted = true;
      for (Entry<ContainerId, Container> container : containers.entrySet()) {
        if (((ContainerImpl) container.getValue()).getCurrentState()
            != ContainerState.COMPLETE) {
          allContainersCompleted = false;
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ex) {
            LOG.warn("Interrupted while sleeping on container kill on resync",
              ex);
          }
          break;
        }
      }
    }
    
    // 记录容器状态
    if (allContainersCompleted) {
      LOG.info("All containers in DONE state");
    } else {
      LOG.info("Done waiting for containers to be killed. Still alive: " +
        containers.keySet());
    }
  }

  /**
   * 获取对应于API调用的远程用户身份信息
   * 
   * @return 远程用户身份对象
   * @throws YarnException YARN异常
   */
  // 获取对应于API调用的远程UGI
  protected UserGroupInformation getRemoteUgi()
      throws YarnException {
    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg = "Cannot obtain the user-name. Got exception: "
          + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }
    return remoteUgi;
  }

  /**
   * 从远程用户身份中选择NM令牌标识符
   * RPC层目前只设置必需的标识符，但仍遍历所有标识符以确保找到正确的
   * 
   * @param remoteUgi 远程用户身份对象
   * @return NM令牌标识符，如果未找到则返回null
   */
  // 从远程UGI获取所需的ContainerTokenIdentifier。RPC层目前只设置必需的id，但仍遍历以确保
  @Private
  @VisibleForTesting
  protected NMTokenIdentifier selectNMTokenIdentifier(
      UserGroupInformation remoteUgi) {
    Set<TokenIdentifier> tokenIdentifiers = remoteUgi.getTokenIdentifiers();
    NMTokenIdentifier resultId = null;
    for (TokenIdentifier id : tokenIdentifiers) {
      if (id instanceof NMTokenIdentifier) {
        resultId = (NMTokenIdentifier) id;
        break;
      }
    }
    return resultId;
  }

  /**
   * 授权用户访问
   * 验证NM令牌标识符和用户身份是否匹配
   * 
   * @param remoteUgi 远程用户身份对象
   * @param nmTokenIdentifier NM令牌标识符
   * @throws YarnException 授权失败时抛出异常
   */
  protected void authorizeUser(UserGroupInformation remoteUgi,
      NMTokenIdentifier nmTokenIdentifier) throws YarnException {
    if (nmTokenIdentifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    // 验证用户名与应用尝试ID是否匹配
    if (!remoteUgi.getUserName().equals(
      nmTokenIdentifier.getApplicationAttemptId().toString())) {
      throw RPCUtil.getRemoteException("Expected applicationAttemptId: "
          + remoteUgi.getUserName() + "Found: "
          + nmTokenIdentifier.getApplicationAttemptId());
    }
  }

  /**
   * 授权启动和增加容器资源的请求
   * 验证NM令牌和容器令牌的有效性
   * 
   * @param nmTokenIdentifier NM令牌标识符
   * @param containerTokenIdentifier 容器令牌标识符
   * @param startRequest 是否为启动容器请求
   * @throws YarnException 授权失败时抛出异常
   */
  @Private
  @VisibleForTesting
  protected void authorizeStartAndResourceIncreaseRequest(
      NMTokenIdentifier nmTokenIdentifier,
      ContainerTokenIdentifier containerTokenIdentifier,
      boolean startRequest)
      throws YarnException {
    if (nmTokenIdentifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    if (containerTokenIdentifier == null) {
      throw RPCUtil.getRemoteException(INVALID_CONTAINERTOKEN_MSG);
    }
    /*
     * 检查以下内容：
     * 1. 请求来自相同的应用尝试
     * 2. 请求拥有未过期的容器令牌
     * 3. 请求拥有由已知RM授予的容器令牌
     */
    ContainerId containerId = containerTokenIdentifier.getContainerID();
    String containerIDStr = containerId.toString();
    boolean unauthorized = false;
    StringBuilder messageBuilder =
        new StringBuilder("Unauthorized request to " + (startRequest ?
            "start container." : "increase container resource."));
    // 检查应用尝试ID是否匹配
    if (!nmTokenIdentifier.getApplicationAttemptId().getApplicationId().
        equals(containerId.getApplicationAttemptId().getApplicationId())) {
      unauthorized = true;
      messageBuilder.append("\nNMToken for application attempt : ")
        .append(nmTokenIdentifier.getApplicationAttemptId())
        .append(" was used for "
            + (startRequest ? "starting " : "increasing resource of ")
            + "container with container token")
        .append(" issued for application attempt : ")
        .append(containerId.getApplicationAttemptId());
    } else if (startRequest && !this.context.getContainerTokenSecretManager()
        .isValidStartContainerRequest(containerTokenIdentifier)) {
      // 容器是否被重复启动？或者RPC层让带有旧密钥生成的令牌通过？
      unauthorized = true;
      messageBuilder.append("\n Attempt to relaunch the same ")
        .append("container with id ").append(containerIDStr).append(".");
    } else if (containerTokenIdentifier.getExpiryTimeStamp() < System
      .currentTimeMillis()) {
      // 确保令牌未过期
      unauthorized = true;
      messageBuilder.append("\nThis token is expired. current time is ")
        .append(System.currentTimeMillis()).append(" found ")
        .append(containerTokenIdentifier.getExpiryTimeStamp());
      messageBuilder.append("\nNote: System times on machines may be out of sync.")
        .append(" Check system time and time zones.");
    }
    if (unauthorized) {
      String msg = messageBuilder.toString();
      LOG.error(msg);
      throw RPCUtil.getRemoteException(msg);
    }
    // 检查容器是否来自未知的RM
    if (containerTokenIdentifier.getRMIdentifier() != nodeStatusUpdater
        .getRMIdentifier()) {
      StringBuilder sb = new StringBuilder("\nContainer ");
      sb.append(containerTokenIdentifier.getContainerID().toString())
        .append(" rejected as it is allocated by a previous RM");
      throw new InvalidContainerException(sb.toString());
    }
  }

  /**
   * 在此NodeManager上启动一组容器
   * 处理来自ApplicationMaster的启动容器请求
   * 
   * @param requests 启动容器请求列表
   * @return 启动容器响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  public StartContainersResponse startContainers(
      StartContainersRequest requests) throws YarnException, IOException {
    UserGroupInformation remoteUgi = getRemoteUgi();
    String remoteUser = remoteUgi.getUserName();
    NMTokenIdentifier nmTokenIdentifier = selectNMTokenIdentifier(remoteUgi);
    authorizeUser(remoteUgi, nmTokenIdentifier);
    List<ContainerId> succeededContainers = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedContainers =
        new HashMap<ContainerId, SerializedException>();
    // 与NodeStatusUpdaterImpl#registerWithRM同步
    // 避免在NM-RM重新同步期间出现竞态条件（由于RM重启）
    synchronized (this.context) {
      for (StartContainerRequest request : requests
          .getStartContainerRequests()) {
        ContainerId containerId = null;
        try {
          if (request.getContainerToken() == null
              || request.getContainerToken().getIdentifier() == null) {
            throw new IOException(INVALID_CONTAINERTOKEN_MSG);
          }

          // 解析容器令牌标识符
          ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
              .newContainerTokenIdentifier(request.getContainerToken());
          verifyAndGetContainerTokenIdentifier(request.getContainerToken(),
              containerTokenIdentifier);
          containerId = containerTokenIdentifier.getContainerID();

          // 如果是AM容器且AM-RM代理服务启用，初始化代理服务
          if (amrmProxyEnabled && containerTokenIdentifier.getContainerType()
              .equals(ContainerType.APPLICATION_MASTER)) {
            this.getAMRMProxyService().processApplicationStartRequest(request);
          }
          // 执行容器启动前检查
          performContainerPreStartChecks(nmTokenIdentifier, request,
              containerTokenIdentifier);
          // 启动容器内部逻辑
          startContainerInternal(containerTokenIdentifier, request,
              remoteUser);
          succeededContainers.add(containerId);
        } catch (YarnException e) {
          failedContainers.put(containerId, SerializedException.newInstance(e));
        } catch (InvalidToken ie) {
          failedContainers
              .put(containerId, SerializedException.newInstance(ie));
          throw ie;
        } catch (IOException e) {
          throw RPCUtil.getRemoteException(e);
        }
      }
      return StartContainersResponse
          .newInstance(getAuxServiceMetaData(), succeededContainers,
              failedContainers);
    }
  }

  /**
   * 执行容器启动前检查
   * 验证NM令牌、容器令牌，并检查辅助服务数据
   * 
   * @param nmTokenIdentifier NM令牌标识符
   * @param request 启动容器请求
   * @param containerTokenIdentifier 容器令牌标识符
   * @throws YarnException YARN异常
   * @throws InvalidToken 无效令牌异常
   */
  private void performContainerPreStartChecks(
      NMTokenIdentifier nmTokenIdentifier, StartContainerRequest request,
      ContainerTokenIdentifier containerTokenIdentifier)
      throws YarnException, InvalidToken {
    /*
     * 1) 应将NMToken保存到NMTokenSecretManager。这里执行而不是在RPC层执行，
     *    因为在打开/认证连接时不知道用户会进行什么RPC调用。
     *    新的NMToken仅在startContainer时颁发（一旦获得更新）。
     *
     * 2) 应验证containerToken。需要检查：
     *    a) 由正确的主密钥签名（检索密码的一部分）
     *    b) 属于正确的Node Manager（检索密码的一部分）
     *    c) 具有正确的RMIdentifier
     *    d) 未过期
     */
    // 授权启动请求
    authorizeStartAndResourceIncreaseRequest(
        nmTokenIdentifier, containerTokenIdentifier, true);
    // 更新NMToken
    updateNMTokenIdentifier(nmTokenIdentifier);

    ContainerLaunchContext launchContext = request.getContainerLaunchContext();

    // 检查辅助服务数据
    Map<String, ByteBuffer> serviceData = getAuxServiceMetaData();
    if (launchContext.getServiceData()!=null &&
        !launchContext.getServiceData().isEmpty()) {
      for (Entry<String, ByteBuffer> meta : launchContext.getServiceData()
          .entrySet()) {
        if (null == serviceData.get(meta.getKey())) {
          throw new InvalidAuxServiceException("The auxService:" + meta.getKey()
              + " does not exist");
        }
      }
    }
  }

  /**
   * 构建应用程序协议缓冲区对象
   * 将应用程序元数据序列化为Protobuf格式用于持久化存储
   * 
   * @param appId 应用程序ID
   * @param user 用户名
   * @param credentials 凭证信息
   * @param appAcls 应用程序访问控制列表
   * @param logAggregationContext 日志聚合上下文
   * @param flowContext 流上下文（时间线服务v2）
   * @return 应用程序协议缓冲区对象
   */
  private ContainerManagerApplicationProto buildAppProto(ApplicationId appId,
      String user, Credentials credentials,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, FlowContext flowContext) {

    ContainerManagerApplicationProto.Builder builder =
        ContainerManagerApplicationProto.newBuilder();
    builder.setId(((ApplicationIdPBImpl) appId).getProto());
    builder.setUser(user);

    if (logAggregationContext != null) {
      builder.setLogAggregationContext((
          (LogAggregationContextPBImpl)logAggregationContext).getProto());
    }

    builder.clearCredentials();
    if (credentials != null) {
      DataOutputBuffer dob = new DataOutputBuffer();
      try {
        credentials.writeTokenStorageToStream(dob);
        builder.setCredentials(ByteString.copyFrom(dob.getData()));
      } catch (IOException e) {
        // should not occur
        LOG.error("Cannot serialize credentials", e);
      }
    }

    builder.clearAcls();
    if (appAcls != null) {
      for (Map.Entry<ApplicationAccessType, String> acl : appAcls.entrySet()) {
        ApplicationACLMapProto p = ApplicationACLMapProto.newBuilder()
            .setAccessType(ProtoUtils.convertToProtoFormat(acl.getKey()))
            .setAcl(acl.getValue())
            .build();
        builder.addAcls(p);
      }
    }

    builder.clearFlowContext();
    if (flowContext != null && flowContext.getFlowName() != null
        && flowContext.getFlowVersion() != null) {
      FlowContextProto fcp =
          FlowContextProto.newBuilder().setFlowName(flowContext.getFlowName())
              .setFlowVersion(flowContext.getFlowVersion())
              .setFlowRunId(flowContext.getFlowRunId()).build();
      builder.setFlowContext(fcp);
    }

    return builder.build();
  }

  /**
   * 启动容器内部实现
   * 创建容器实例，处理应用程序引用，存储容器状态
   * 
   * @param containerTokenIdentifier 容器令牌标识符
   * @param request 启动容器请求
   * @param remoteUser 远程用户
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @SuppressWarnings("unchecked")
  protected void startContainerInternal(
      ContainerTokenIdentifier containerTokenIdentifier,
      StartContainerRequest request, String remoteUser)
      throws YarnException, IOException {

    ContainerId containerId = containerTokenIdentifier.getContainerID();
    String containerIdStr = containerId.toString();
    String user = containerTokenIdentifier.getApplicationSubmitter();
    Resource containerResource = containerTokenIdentifier.getResource();

    LOG.info("Start request for " + containerIdStr + " by user " + remoteUser +
        " with resource " + containerResource);

    ContainerLaunchContext launchContext = request.getContainerLaunchContext();

    // 检查本地资源的完整性
    for (Map.Entry<String, LocalResource> rsrc : launchContext
        .getLocalResources().entrySet()) {
      if (rsrc.getValue() == null || rsrc.getValue().getResource() == null) {
        throw new YarnException("Null resource URL for local resource "
            + rsrc.getKey() + " : " + rsrc.getValue());
      } else if (rsrc.getValue().getType() == null) {
        throw new YarnException("Null resource type for local resource "
            + rsrc.getKey() + " : " + rsrc.getValue());
      } else if (rsrc.getValue().getVisibility() == null) {
        throw new YarnException("Null resource visibility for local resource "
            + rsrc.getKey() + " : " + rsrc.getValue());
      }
    }

    // 解析凭证信息
    Credentials credentials =
        YarnServerSecurityUtils.parseCredentials(launchContext);

    long containerStartTime = SystemClock.getInstance().getTime();
    // 创建容器实现实例
    Container container =
        new ContainerImpl(getConfig(), this.dispatcher,
            launchContext, credentials, metrics, containerTokenIdentifier,
            context, containerStartTime);
    ApplicationId applicationID =
        containerId.getApplicationAttemptId().getApplicationId();
    
    // 检查容器是否已经存在
    if (context.getContainers().putIfAbsent(containerId, container) != null) {
      NMAuditLogger.logFailure(remoteUser, AuditConstants.START_CONTAINER,
        "ContainerManagerImpl", "Container already running on this node!",
        applicationID, containerId);
      throw RPCUtil.getRemoteException("Container " + containerIdStr
          + " already is running on this node!!");
    }

    this.readLock.lock();
    try {
      if (!isServiceStopped()) {
        // 如果应用程序不存在，创建应用程序引用
        if (!context.getApplications().containsKey(applicationID)) {
          // 从启动上下文填充流上下文（如果启用了时间线服务v2）
          FlowContext flowContext =
              getFlowContext(launchContext, applicationID);

          Application application =
              new ApplicationImpl(dispatcher, user, flowContext,
                  applicationID, credentials, context);
          if (context.getApplications().putIfAbsent(applicationID,
              application) == null) {
            metrics.runningApplication();
            LOG.info("Creating a new application reference for app "
                + applicationID);
            LogAggregationContext logAggregationContext =
                containerTokenIdentifier.getLogAggregationContext();
            Map<ApplicationAccessType, String> appAcls =
                container.getLaunchContext().getApplicationACLs();
            // 存储应用程序状态
            context.getNMStateStore().storeApplication(applicationID,
                buildAppProto(applicationID, user, credentials, appAcls,
                    logAggregationContext, flowContext));
            // 发送应用程序初始化事件
            dispatcher.getEventHandler().handle(new ApplicationInitEvent(
                applicationID, appAcls, logAggregationContext));
          }
        } else if (containerTokenIdentifier.getContainerType()
            == ContainerType.APPLICATION_MASTER) {
          // 如果是AM容器，更新流上下文
          FlowContext flowContext =
              getFlowContext(launchContext, applicationID);
          if (flowContext != null) {
            ApplicationImpl application =
                (ApplicationImpl) context.getApplications().get(applicationID);

            // 更新ApplicationImpl中的flowContext引用
            application.setFlowContext(flowContext);

            // 更新状态存储以支持恢复
            context.getNMStateStore().storeApplication(applicationID,
                buildAppProto(applicationID, user, credentials,
                    container.getLaunchContext().getApplicationACLs(),
                    containerTokenIdentifier.getLogAggregationContext(),
                    flowContext));

            LOG.info(
                "Updated application reference with flowContext " + flowContext
                    + " for app " + applicationID);
          } else {
            LOG.info("TimelineService V2.0 is not enabled. Skipping updating "
                + "flowContext for application " + applicationID);
          }
        }

        // 存储容器状态
        this.context.getNMStateStore().storeContainer(containerId,
            containerTokenIdentifier.getVersion(), containerStartTime, request);
        // 发送容器初始化事件
        dispatcher.getEventHandler().handle(
          new ApplicationContainerInitEvent(container));

        // 标记容器启动成功
        this.context.getContainerTokenSecretManager().startContainerSuccessful(
          containerTokenIdentifier);
        NMAuditLogger.logSuccess(remoteUser, AuditConstants.START_CONTAINER,
          "ContainerManageImpl", applicationID, containerId);
        // 更新指标
        metrics.launchedContainer();
        metrics.allocateContainer(containerTokenIdentifier.getResource());
      } else {
        throw new YarnException(
            "Container start failed as the NodeManager is " +
            "in the process of shutting down");
      }
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 从启动上下文中获取流上下文信息
   * 如果启用了时间线服务v2，从环境变量中提取流名称、版本和运行ID
   * 
   * @param launchContext 容器启动上下文
   * @param applicationID 应用程序ID
   * @return 流上下文对象，如果时间线服务v2未启用则返回null
   */
  private FlowContext getFlowContext(ContainerLaunchContext launchContext,
      ApplicationId applicationID) {
    FlowContext flowContext = null;
    if (timelineServiceV2Enabled) {
      String flowName = launchContext.getEnvironment()
          .get(TimelineUtils.FLOW_NAME_TAG_PREFIX);
      String flowVersion = launchContext.getEnvironment()
          .get(TimelineUtils.FLOW_VERSION_TAG_PREFIX);
      String flowRunIdStr = launchContext.getEnvironment()
          .get(TimelineUtils.FLOW_RUN_ID_TAG_PREFIX);
      long flowRunId = 0L;
      if (flowRunIdStr != null && !flowRunIdStr.isEmpty()) {
        flowRunId = Long.parseLong(flowRunIdStr);
      }
      flowContext = new FlowContext(flowName, flowVersion, flowRunId);
      LOG.debug("Flow context: {} created for an application {}",
          flowContext, applicationID);
    }
    return flowContext;
  }

  /**
   * 验证容器令牌并获取容器令牌标识符
   * 检查令牌密码是否匹配，确保令牌的有效性
   * 
   * @param token 容器令牌
   * @param containerTokenIdentifier 容器令牌标识符
   * @return 验证通过的容器令牌标识符
   * @throws YarnException YARN异常
   * @throws InvalidToken 无效令牌异常
   */
  protected ContainerTokenIdentifier verifyAndGetContainerTokenIdentifier(
      org.apache.hadoop.yarn.api.records.Token token,
      ContainerTokenIdentifier containerTokenIdentifier) throws YarnException,
      InvalidToken {
    byte[] password =
        context.getContainerTokenSecretManager().retrievePassword(
            containerTokenIdentifier);
    byte[] tokenPass = token.getPassword().array();
    if (password == null || tokenPass == null
        || !MessageDigest.isEqual(password, tokenPass)) {
      throw new InvalidToken(
        "Invalid container token used for starting container on : "
            + context.getNodeId().toString());
    }
    return containerTokenIdentifier;
  }

  /**
   * 增加容器资源（已废弃）
   * 此方法已被updateContainer方法替代
   * 
   * @param requests 增加容器资源请求
   * @return 增加容器资源响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  @Deprecated
  public IncreaseContainersResourceResponse increaseContainersResource(
      IncreaseContainersResourceRequests requests)
          throws YarnException, IOException {
    ContainerUpdateResponse resp = updateContainer(
        ContainerUpdateRequest.newInstance(requests.getContainersToIncrease()));
    return IncreaseContainersResourceResponse.newInstance(
        resp.getSuccessfullyUpdatedContainers(), resp.getFailedRequests());
  }

  /**
   * 更新容器资源
   * 处理容器资源的增加或更新请求，验证令牌并发送更新事件
   * 
   * @param request 容器更新请求
   * @return 容器更新响应，包含成功和失败的容器列表
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  public ContainerUpdateResponse updateContainer(ContainerUpdateRequest
      request) throws YarnException, IOException {
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier nmTokenIdentifier = selectNMTokenIdentifier(remoteUgi);
    authorizeUser(remoteUgi, nmTokenIdentifier);
    List<ContainerId> successfullyUpdatedContainers
        = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedContainers =
        new HashMap<ContainerId, SerializedException>();
    // Synchronize with NodeStatusUpdaterImpl#registerWithRM
    // to avoid race condition during NM-RM resync (due to RM restart) while a
    // container resource is being increased in NM, in particular when the
    // increased container has not yet been added to the increasedContainers
    // map in NMContext.
    synchronized (this.context) {
      // Process container resource increase requests
      for (org.apache.hadoop.yarn.api.records.Token token :
          request.getContainersToUpdate()) {
        ContainerId containerId = null;
        try {
          if (token.getIdentifier() == null) {
            throw new IOException(INVALID_CONTAINERTOKEN_MSG);
          }
          ContainerTokenIdentifier containerTokenIdentifier =
              BuilderUtils.newContainerTokenIdentifier(token);
          verifyAndGetContainerTokenIdentifier(token,
              containerTokenIdentifier);
          authorizeStartAndResourceIncreaseRequest(
              nmTokenIdentifier, containerTokenIdentifier, false);
          containerId = containerTokenIdentifier.getContainerID();
          // Reuse the startContainer logic to update NMToken,
          // as container resource increase request will have come with
          // an updated NMToken.
          updateNMTokenIdentifier(nmTokenIdentifier);
          updateContainerInternal(containerId, containerTokenIdentifier);
          successfullyUpdatedContainers.add(containerId);
        } catch (YarnException | InvalidToken e) {
          failedContainers.put(containerId, SerializedException.newInstance(e));
        } catch (IOException e) {
          throw RPCUtil.getRemoteException(e);
        }
      }
    }
    return ContainerUpdateResponse.newInstance(
        successfullyUpdatedContainers, failedContainers);
  }

  /**
   * 更新容器内部实现
   * 验证容器存在性、版本和资源的有效性，并发送容器更新事件
   * 
   * @param containerId 容器ID
   * @param containerTokenIdentifier 容器令牌标识符
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @SuppressWarnings("unchecked")
  private void updateContainerInternal(ContainerId containerId,
      ContainerTokenIdentifier containerTokenIdentifier)
      throws YarnException, IOException {
    Container container = context.getContainers().get(containerId);
    // Check container existence
    if (container == null) {
      if (nodeStatusUpdater.isContainerRecentlyStopped(containerId)) {
        throw RPCUtil.getRemoteException("Container " + containerId.toString()
            + " was recently stopped on node manager.");
      } else {
        throw RPCUtil.getRemoteException("Container " + containerId.toString()
            + " is not handled by this NodeManager");
      }
    }
    // Check container version.
    int currentVersion = container.getContainerTokenIdentifier().getVersion();
    if (containerTokenIdentifier.getVersion() <= currentVersion) {
      throw RPCUtil.getRemoteException("Container " + containerId.toString()
          + " has update version [" + currentVersion + "] >= requested version"
          + " [" + containerTokenIdentifier.getVersion() + "]");
    }

    // Check validity of the target resource.
    Resource currentResource = container.getResource();
    ExecutionType currentExecType =
        container.getContainerTokenIdentifier().getExecutionType();
    boolean isResourceChange = false;
    boolean isExecTypeUpdate = false;
    Resource targetResource = containerTokenIdentifier.getResource();
    ExecutionType targetExecType = containerTokenIdentifier.getExecutionType();

    // Is true if either the resources has increased or execution type
    // updated from opportunistic to guaranteed
    boolean isIncrease = false;
    if (!currentResource.equals(targetResource)) {
      isResourceChange = true;
      isIncrease = Resources.fitsIn(currentResource, targetResource)
          && !Resources.fitsIn(targetResource, currentResource);
    } else if (!currentExecType.equals(targetExecType)) {
      isExecTypeUpdate = true;
      isIncrease = currentExecType == ExecutionType.OPPORTUNISTIC &&
          targetExecType == ExecutionType.GUARANTEED;
    }
    if (isIncrease) {
      org.apache.hadoop.yarn.api.records.Container increasedContainer = null;
      if (isResourceChange) {
        increasedContainer =
            org.apache.hadoop.yarn.api.records.Container.newInstance(
                containerId, null, null, targetResource, null,
                null, currentExecType);
        if (context.getIncreasedContainers().putIfAbsent(containerId,
            increasedContainer) != null){
          throw RPCUtil.getRemoteException("Container " + containerId.toString()
              + " resource is being increased -or- " +
              "is undergoing ExecutionType promoted.");
        }
      }
    }
    this.readLock.lock();
    try {
      if (!serviceStopped) {
        // Dispatch message to Container to actually
        // make the change.
        dispatcher.getEventHandler().handle(new UpdateContainerTokenEvent(
            container.getContainerId(), containerTokenIdentifier,
            isResourceChange, isExecTypeUpdate, isIncrease));
      } else {
        throw new YarnException(
            "Unable to change container resource as the NodeManager is "
                + "in the process of shutting down");
      }
    } finally {
      this.readLock.unlock();
    }
  }

  /**
   * 更新NM令牌标识符
   * 通知NMTokenSecretManager应用尝试开始容器
   * 
   * @param nmTokenIdentifier NM令牌标识符
   * @throws InvalidToken 无效令牌异常
   */
  @Private
  @VisibleForTesting
  protected void updateNMTokenIdentifier(NMTokenIdentifier nmTokenIdentifier)
      throws InvalidToken {
    context.getNMTokenSecretManager().appAttemptStartContainer(
      nmTokenIdentifier);
  }

  /**
   * 停止在此NodeManager上运行的一组容器
   * 
   * @param requests 停止容器请求
   * @return 停止容器响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  public StopContainersResponse stopContainers(StopContainersRequest requests)
      throws YarnException, IOException {

    List<ContainerId> succeededRequests = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    String remoteUser = remoteUgi.getUserName();
    
    // 处理每个容器停止请求
    for (ContainerId id : requests.getContainerIds()) {
      try {
        Container container = this.context.getContainers().get(id);
        // 授权停止容器请求
        authorizeGetAndStopContainerRequest(id, container, true, identifier,
            remoteUser);
        // 停止容器内部逻辑
        stopContainerInternal(id, remoteUser);
        succeededRequests.add(id);
      } catch (YarnException e) {
        failedRequests.put(id, SerializedException.newInstance(e));
      }
    }
    return StopContainersResponse
      .newInstance(succeededRequests, failedRequests);
  }

  /**
   * 停止容器内部实现
   * 
   * @param containerID 容器ID
   * @param remoteUser 远程用户
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @SuppressWarnings("unchecked")
  protected void stopContainerInternal(ContainerId containerID,
      String remoteUser)
      throws YarnException, IOException {
    String containerIDStr = containerID.toString();
    Container container = this.context.getContainers().get(containerID);
    LOG.info("Stopping container with container Id: " + containerIDStr);

    if (container == null) {
      // 容器不存在，检查是否是最近停止的容器
      if (!nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " is not handled by this NodeManager");
      }
    } else {
      // 检查容器是否正在恢复中
      if (container.isRecovering()) {
        throw new NMNotYetReadyException("Container " + containerIDStr
            + " is recovering, try later");
      }
      // 存储容器被杀死的状态
      context.getNMStateStore().storeContainerKilled(containerID);
      // 发送容器杀死事件
      container.sendKillEvent(ContainerExitStatus.KILLED_BY_APPMASTER,
          "Container killed by the ApplicationMaster.");

      // 记录审计日志
      NMAuditLogger.logSuccess(remoteUser, AuditConstants.STOP_CONTAINER,
          "ContainerManageImpl",
          containerID.getApplicationAttemptId().getApplicationId(),
          containerID);
    }
  }

  /**
   * 获取在此NodeManager上运行的容器状态列表
   * 
   * @param request 获取容器状态请求
   * @return 容器状态响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  public GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest request) throws YarnException, IOException {

    List<ContainerStatus> succeededRequests = new ArrayList<ContainerStatus>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    String remoteUser = remoteUgi.getUserName();
    
    // 处理每个容器状态查询请求
    for (ContainerId id : request.getContainerIds()) {
      try {
        ContainerStatus status = getContainerStatusInternal(id, identifier,
            remoteUser);
        succeededRequests.add(status);
      } catch (YarnException e) {
        failedRequests.put(id, SerializedException.newInstance(e));
      }
    }
    return GetContainerStatusesResponse.newInstance(succeededRequests,
      failedRequests);
  }

  /**
   * 获取容器状态内部实现
   * 
   * @param containerID 容器ID
   * @param nmTokenIdentifier NM令牌标识符
   * @param remoteUser 远程用户
   * @return 容器状态
   * @throws YarnException YARN异常
   */
  protected ContainerStatus getContainerStatusInternal(ContainerId containerID,
      NMTokenIdentifier nmTokenIdentifier, String remoteUser)
      throws YarnException {
    String containerIDStr = containerID.toString();
    Container container = this.context.getContainers().get(containerID);

    LOG.info("Getting container-status for " + containerIDStr);
    // 授权获取容器状态请求
    authorizeGetAndStopContainerRequest(containerID, container, false,
        nmTokenIdentifier, remoteUser);

    if (container == null) {
      // 检查容器是否在最近停止的列表中
      if (nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " was recently stopped on node manager.");
      } else {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " is not handled by this NodeManager");
      }
    }
    // 克隆并获取容器状态
    ContainerStatus containerStatus = container.cloneAndGetContainerStatus();
    logContainerStatus("Returning ", containerStatus);
    return containerStatus;
  }

  /**
   * 记录容器状态
   * 格式化输出容器的详细状态信息用于日志记录
   * 
   * @param prefix 日志前缀
   * @param status 容器状态对象
   */
  private void logContainerStatus(String prefix, ContainerStatus status) {
    StringBuilder sb = new StringBuilder();
    sb.append(prefix);
    sb.append("ContainerStatus: [");
    sb.append("ContainerId: ");
    sb.append(status.getContainerId()).append(", ");
    sb.append("ExecutionType: ");
    sb.append(status.getExecutionType()).append(", ");
    sb.append("State: ");
    sb.append(status.getState()).append(", ");
    sb.append("Capability: ");
    sb.append(status.getCapability()).append(", ");
    sb.append("Diagnostics: ");
    sb.append(LOG.isDebugEnabled() ? status.getDiagnostics() : "...");
    sb.append(", ");
    sb.append("ExitStatus: ");
    sb.append(status.getExitStatus()).append(", ");
    sb.append("IP: ");
    sb.append(status.getIPs()).append(", ");
    sb.append("Host: ");
    sb.append(status.getHost()).append(", ");
    sb.append("ExposedPorts: ");
    sb.append(status.getExposedPorts()).append(", ");
    sb.append("ContainerSubState: ");
    sb.append(status.getContainerSubState());
    sb.append("]");
    LOG.info(sb.toString());
  }

  /**
   * 授权获取和停止容器请求
   * 验证用户是否有权限访问指定的容器
   * 
   * @param containerId 容器ID
   * @param container 容器对象
   * @param stopRequest 是否为停止容器请求
   * @param identifier NM令牌标识符
   * @param remoteUser 远程用户
   * @throws YarnException 授权失败时抛出异常
   */
  @Private
  @VisibleForTesting
  protected void authorizeGetAndStopContainerRequest(ContainerId containerId,
      Container container, boolean stopRequest, NMTokenIdentifier identifier,
      String remoteUser)
      throws YarnException {
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    /*
     * 对于获取/停止容器状态，需要验证：
     * 1) 用户(NMToken)的应用尝试只能访问已启动的容器
     * 2) 请求的containerId属于使用的同一应用尝试(NMToken)
     *    (这将防止用户了解其他应用的容器)
     */
    ApplicationId nmTokenAppId =
        identifier.getApplicationAttemptId().getApplicationId();
    
    if ((!nmTokenAppId.equals(containerId.getApplicationAttemptId().getApplicationId()))
        || (container != null && !nmTokenAppId.equals(container
            .getContainerId().getApplicationAttemptId().getApplicationId()))) {
      String msg;
      if (stopRequest) {
        msg = identifier.getApplicationAttemptId()
            + " attempted to stop non-application container : "
            + containerId;
        NMAuditLogger.logFailure(remoteUser, AuditConstants.STOP_CONTAINER,
            "ContainerManagerImpl", "Trying to stop unknown container!",
            nmTokenAppId, containerId);
      } else {
        msg = identifier.getApplicationAttemptId()
            + " attempted to get status for non-application container : "
            + containerId;
      }
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }
  }

  /**
   * 容器事件分发器
   * 实现EventHandler接口，处理容器相关事件
   */
  class ContainerEventDispatcher implements EventHandler<ContainerEvent> {
    @Override
    public void handle(ContainerEvent event) {
      Map<ContainerId,Container> containers =
        ContainerManagerImpl.this.context.getContainers();
      Container c = containers.get(event.getContainerID());
      if (c != null) {
        // 将事件分发给对应的容器处理
        c.handle(event);
        // 如果启用了NM指标发布器，发布容器事件
        if (nmMetricsPublisher != null) {
          nmMetricsPublisher.publishContainerEvent(event);
        }
      } else {
        LOG.warn("Event " + event + " sent to absent container " +
            event.getContainerID());
      }
    }
  }

  /**
   * 应用程序事件分发器
   * 实现EventHandler接口，处理应用程序相关事件
   */
  class ApplicationEventDispatcher implements EventHandler<ApplicationEvent> {
    @Override
    public void handle(ApplicationEvent event) {
      Application app =
          ContainerManagerImpl.this.context.getApplications().get(
              event.getApplicationID());
      if (app != null) {
        // 将事件分发给对应的应用程序处理
        app.handle(event);
        // 如果启用了NM指标发布器，发布应用程序事件
        if (nmMetricsPublisher != null) {
          nmMetricsPublisher.publishApplicationEvent(event);
        }
      } else {
        LOG.warn("Event " + event + " sent to absent application "
            + event.getApplicationID());
      }
    }
  }

  /**
   * 本地化事件处理包装器
   * 包装原始的本地化事件处理器，并添加时间线发布功能
   */
  private static final class LocalizationEventHandlerWrapper implements
      EventHandler<LocalizationEvent> {

    private EventHandler<LocalizationEvent> origLocalizationEventHandler;
    private NMTimelinePublisher timelinePublisher;

    LocalizationEventHandlerWrapper(EventHandler<LocalizationEvent> handler,
        NMTimelinePublisher publisher) {
      this.origLocalizationEventHandler = handler;
      this.timelinePublisher = publisher;
    }

    @Override
    public void handle(LocalizationEvent event) {
      // 调用原始的本地化事件处理器
      origLocalizationEventHandler.handle(event);
      // 如果启用了时间线发布器，发布本地化事件
      if (timelinePublisher != null) {
        timelinePublisher.publishLocalizationEvent(event);
      }
    }
  }

  /**
   * 辅助本地路径处理器实现类
   * 实现AuxiliaryLocalPathHandler接口，将NodeManager的LocalDirsHandlerService链接到辅助服务
   */
  static class AuxiliaryLocalPathHandlerImpl
      implements AuxiliaryLocalPathHandler {
    private LocalDirsHandlerService dirhandlerService;
    
    AuxiliaryLocalPathHandlerImpl(
        LocalDirsHandlerService dirhandlerService) {
      this.dirhandlerService = dirhandlerService;
    }

    /**
     * 获取用于读取的本地路径
     */
    @Override
    public Path getLocalPathForRead(String path) throws IOException {
      return dirhandlerService.getLocalPathForRead(path);
    }

    /**
     * 获取用于写入的本地路径
     */
    @Override
    public Path getLocalPathForWrite(String path) throws IOException {
      return dirhandlerService.getLocalPathForWrite(path);
    }

    /**
     * 获取用于写入的本地路径（带大小限制）
     */
    @Override
    public Path getLocalPathForWrite(String path, long size)
        throws IOException {
      return dirhandlerService.getLocalPathForWrite(path, size, false);
    }

    /**
     * 获取所有可用于读取的本地路径
     */
    @Override
    public Iterable<Path> getAllLocalPathsForRead(String path) throws IOException {
      return dirhandlerService.getAllLocalPathsForRead(path);
    }
  }

  /**
   * 处理容器管理器事件
   * 根据不同的事件类型执行相应的处理逻辑
   * 
   * @param event 容器管理器事件
   */
  @SuppressWarnings("unchecked")
  @Override
  public void handle(ContainerManagerEvent event) {
    switch (event.getType()) {
    case FINISH_APPS:
      // 处理应用程序完成事件
      CMgrCompletedAppsEvent appsFinishedEvent =
          (CMgrCompletedAppsEvent) event;
      for (ApplicationId appID : appsFinishedEvent.getAppsToCleanup()) {
        Application app = this.context.getApplications().get(appID);
        if (app == null) {
          LOG.info("couldn't find application " + appID + " while processing"
              + " FINISH_APPS event. The ResourceManager allocated resources"
              + " for this application to the NodeManager but no active"
              + " containers were found to process.");
          continue;
        }

        boolean shouldDropEvent = false;
        // 检查是否有容器正在恢复中
        for (Container container : app.getContainers().values()) {
          if (container.isRecovering()) {
            LOG.info("drop FINISH_APPS event to " + appID + " because "
                + "container " + container.getContainerId()
                + " is recovering");
            shouldDropEvent = true;
            break;
          }
        }
        if (shouldDropEvent) {
          continue;
        }

        // 设置诊断信息
        String diagnostic = "";
        if (appsFinishedEvent.getReason() == CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN) {
          diagnostic = "Application killed on shutdown";
        } else if (appsFinishedEvent.getReason() == CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER) {
          diagnostic = "Application killed by ResourceManager";
        }
        // 发送应用程序完成事件
        this.dispatcher.getEventHandler().handle(
            new ApplicationFinishEvent(appID,
                diagnostic));
      }
      break;
    case FINISH_CONTAINERS:
      // 处理容器完成事件
      CMgrCompletedContainersEvent containersFinishedEvent =
          (CMgrCompletedContainersEvent) event;
      for (ContainerId containerId : containersFinishedEvent
          .getContainersToCleanup()) {
        ApplicationId appId =
            containerId.getApplicationAttemptId().getApplicationId();
        Application app = this.context.getApplications().get(appId);
        if (app == null) {
          LOG.warn("couldn't find app " + appId + " while processing"
              + " FINISH_CONTAINERS event");
          continue;
        }

        Container container = app.getContainers().get(containerId);
        if (container == null) {
          LOG.warn("couldn't find container " + containerId
              + " while processing FINISH_CONTAINERS event");
          continue;
        }

        if (container.isRecovering()) {
          LOG.info("drop FINISH_CONTAINERS event to " + containerId
              + " because container is recovering");
          continue;
        }

        // 发送容器杀死事件
        this.dispatcher.getEventHandler().handle(
              new ContainerKillEvent(containerId,
                  ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
                  "Container Killed by ResourceManager"));
      }
      break;
    case UPDATE_CONTAINERS:
      // 处理容器更新事件
      CMgrUpdateContainersEvent containersDecreasedEvent =
          (CMgrUpdateContainersEvent) event;
      for (org.apache.hadoop.yarn.api.records.Container container
          : containersDecreasedEvent.getContainersToUpdate()) {
        try {
          ContainerTokenIdentifier containerTokenIdentifier =
              BuilderUtils.newContainerTokenIdentifier(
                  container.getContainerToken());
          updateContainerInternal(container.getId(),
              containerTokenIdentifier);
        } catch (YarnException e) {
          LOG.error("Unable to decrease container resource", e);
        } catch (IOException e) {
          LOG.error("Unable to update container resource in store", e);
        }
      }
      break;
    case SIGNAL_CONTAINERS:
      // 处理容器信号事件
      CMgrSignalContainersEvent containersSignalEvent =
          (CMgrSignalContainersEvent) event;
      for (SignalContainerRequest request : containersSignalEvent
          .getContainersToSignal()) {
        internalSignalToContainer(request, "ResourceManager");
      }
      break;
    default:
        throw new YarnRuntimeException(
            "Got an unknown ContainerManagerEvent type: " + event.getType());
    }
  }

  /**
   * 服务状态变化回调方法
   * 当依赖的服务状态发生变化时被调用
   * 
   * @param service 状态发生变化的服务
   */
  @Override
  public void stateChanged(Service service) {
    // TODO Auto-generated method stub
  }
  
  /**
   * 获取NodeManager上下文
   * 
   * @return NodeManager上下文对象
   */
  public Context getContext() {
    return this.context;
  }

  /**
   * 获取辅助服务元数据
   * 
   * @return 辅助服务元数据映射
   */
  public Map<String, ByteBuffer> getAuxServiceMetaData() {
    return this.auxiliaryServices.getMetaData();
  }

  /**
   * 获取AM-RM代理服务
   * 
   * @return AM-RM代理服务实例
   */
  @Private
  public AMRMProxyService getAMRMProxyService() {
    return this.amrmProxyService;
  }

  /**
   * 设置AM-RM代理服务
   * 
   * @param amrmProxyService AM-RM代理服务实例
   */
  @Private
  protected void setAMRMProxyService(AMRMProxyService amrmProxyService) {
    this.amrmProxyService = amrmProxyService;
  }

  /**
   * 检查服务是否已停止
   * 
   * @return 如果服务已停止返回true，否则返回false
   */
  protected boolean isServiceStopped() {
    return serviceStopped;
  }

  /**
   * 获取机会容器状态
   * 
   * @return 机会容器状态对象
   */
  @Override
  public OpportunisticContainersStatus getOpportunisticContainersStatus() {
    return this.containerScheduler.getOpportunisticContainersStatus();
  }

  /**
   * 更新队列限制
   * 
   * @param queuingLimit 容器队列限制
   */
  @Override
  public void updateQueuingLimit(ContainerQueuingLimit queuingLimit) {
    this.containerScheduler.updateQueuingLimit(queuingLimit);
  }

  /**
   * 发送信号到容器
   * 处理来自ApplicationMaster的容器信号请求
   * 
   * @param request 信号容器请求
   * @return 信号容器响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @SuppressWarnings("unchecked")
  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    internalSignalToContainer(request, "Application Master");
    return new SignalContainerResponsePBImpl();
  }

  /**
   * 本地化资源
   * 为容器本地化额外的资源
   * 
   * @param request 资源本地化请求
   * @return 资源本地化响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  @SuppressWarnings("unchecked")
  public ResourceLocalizationResponse localize(
      ResourceLocalizationRequest request) throws YarnException, IOException {

    ContainerId containerId = request.getContainerId();
    Container container = preReInitializeOrLocalizeCheck(containerId,
        ReInitOp.LOCALIZE);
    try {
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
          container.getResourceSet().addResources(request.getLocalResources());
      if (req != null && !req.isEmpty()) {
        dispatcher.getEventHandler()
            .handle(new ContainerLocalizationRequestEvent(container, req));
      }
    } catch (URISyntaxException e) {
      LOG.info("Error when parsing local resource URI for " + containerId, e);
      throw new YarnException(e);
    }

    return ResourceLocalizationResponse.newInstance();
  }

  /**
   * 重新初始化容器
   * 使用新的启动上下文重新初始化容器
   * 
   * @param request 重新初始化容器请求
   * @return 重新初始化容器响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  public ReInitializeContainerResponse reInitializeContainer(
      ReInitializeContainerRequest request) throws YarnException, IOException {
    reInitializeContainer(request.getContainerId(),
        request.getContainerLaunchContext(), request.getAutoCommit());
    return ReInitializeContainerResponse.newInstance();
  }

  /**
   * 重启容器
   * 重启指定的容器（使用原有的启动上下文）
   * 
   * @param containerId 容器ID
   * @return 重启容器响应
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  public RestartContainerResponse restartContainer(ContainerId containerId)
      throws YarnException, IOException {
    reInitializeContainer(containerId, null, true);
    return RestartContainerResponse.newInstance();
  }

  /**
   * 使用新的启动上下文重新初始化容器
   * 如果未提供retryFailureContext，容器将在失败时终止
   * 注意：autoCommit默认为true，这意味着回滚上下文会在发送启动新进程的命令后立即清除
   * （容器移至RUNNING状态）
   * 
   * @param containerId 容器ID
   * @param reInitLaunchContext 目标启动上下文
   * @param autoCommit 自动提交标志
   * @throws YarnException YARN异常
   */
  public void reInitializeContainer(ContainerId containerId,
      ContainerLaunchContext reInitLaunchContext, boolean autoCommit)
      throws YarnException {
    LOG.debug("{} requested reinit", containerId);
    Container container = preReInitializeOrLocalizeCheck(containerId,
        ReInitOp.RE_INIT);
    ResourceSet resourceSet = new ResourceSet();
    try {
      if (reInitLaunchContext != null) {
        resourceSet.addResources(reInitLaunchContext.getLocalResources());
      }
      dispatcher.getEventHandler().handle(
          new ContainerReInitEvent(containerId, reInitLaunchContext,
              resourceSet, autoCommit));
      container.setIsReInitializing(true);
    } catch (URISyntaxException e) {
      LOG.info("Error when parsing local resource URI for upgrade of" +
          "Container [" + containerId + "]", e);
      throw new YarnException(e);
    }
  }

  /**
   * 回滚上次重新初始化（如果可能）
   * 
   * @param containerId 容器ID
   * @return 回滚响应
   * @throws YarnException YARN异常
   */
  @Override
  public RollbackResponse rollbackLastReInitialization(ContainerId containerId)
      throws YarnException {
    Container container = preReInitializeOrLocalizeCheck(containerId,
        ReInitOp.ROLLBACK);
    if (container.canRollback()) {
      dispatcher.getEventHandler().handle(
          new ContainerEvent(containerId, ContainerEventType.ROLLBACK_REINIT));
      container.setIsReInitializing(true);
    } else {
      throw new YarnException("Nothing to rollback to !!");
    }
    return RollbackResponse.newInstance();
  }

  /**
   * 提交上次重新初始化，之后将无法回滚
   * 
   * @param containerId 容器ID
   * @return 提交响应
   * @throws YarnException YARN异常
   */
  @Override
  public CommitResponse commitLastReInitialization(ContainerId containerId)
      throws YarnException {
    Container container = preReInitializeOrLocalizeCheck(containerId,
        ReInitOp.COMMIT);
    if (container.canRollback()) {
      container.commitUpgrade();
    } else {
      throw new YarnException("Nothing to Commit !!");
    }
    return CommitResponse.newInstance();
  }

  /**
   * 在重新初始化或本地化前执行检查
   * 验证用户权限、容器存在性和容器状态
   * 
   * @param containerId 容器ID
   * @param op 操作类型（RE_INIT, COMMIT, ROLLBACK, LOCALIZE）
   * @return 容器对象
   * @throws YarnException 检查失败时抛出异常
   */
  private Container preReInitializeOrLocalizeCheck(ContainerId containerId,
      ReInitOp op) throws YarnException {
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier nmTokenIdentifier = selectNMTokenIdentifier(remoteUgi);
    authorizeUser(remoteUgi, nmTokenIdentifier);
    if (!nmTokenIdentifier.getApplicationAttemptId().getApplicationId()
        .equals(containerId.getApplicationAttemptId().getApplicationId())) {
      throw new YarnException("ApplicationMaster not authorized to perform " +
          "["+ op + "] on Container [" + containerId + "]!!");
    }
    Container container = context.getContainers().get(containerId);
    if (container == null) {
      throw new YarnException("Specified " + containerId + " does not exist!");
    }
    if (!container.isRunning() || container.isReInitializing()
        || container.getContainerTokenIdentifier().getExecutionType()
        == ExecutionType.OPPORTUNISTIC) {
      throw new YarnException("Cannot perform " + op + " on [" + containerId
          + "]. Current state is [" + container.getContainerState() + ", " +
          "isReInitializing=" + container.isReInitializing() + "]. Container"
          + " Execution Type is [" + container.getContainerTokenIdentifier()
          .getExecutionType() + "].");
    }
    return container;
  }

  /**
   * 内部信号到容器的实现
   * 发送信号命令到指定的容器
   * 
   * @param request 信号容器请求
   * @param sentBy 发送信号的来源（ResourceManager或Application Master）
   */
  @SuppressWarnings("unchecked")
  private void internalSignalToContainer(SignalContainerRequest request,
      String sentBy) {
    ContainerId containerId = request.getContainerId();
    Container container = this.context.getContainers().get(containerId);
    if (container != null) {
      LOG.info(containerId + " signal request " + request.getCommand()
            + " by " + sentBy);
      this.dispatcher.getEventHandler().handle(
          new SignalContainersLauncherEvent(container,
              request.getCommand()));
    } else {
      LOG.info("Container " + containerId + " no longer exists");
    }
  }

  /**
   * 获取容器调度器
   * 
   * @return 容器调度器实例
   */
  @Override
  public ContainerScheduler getContainerScheduler() {
    return this.containerScheduler;
  }

  /**
   * 处理凭证更新
   * 检查日志处理器中的无效令牌应用并发送令牌更新事件
   */
  @Override
  public void handleCredentialUpdate() {
    Set<ApplicationId> invalidApps = logHandler.getInvalidTokenApps();
    if (!invalidApps.isEmpty()) {
      dispatcher.getEventHandler().handle(new LogHandlerTokenUpdatedEvent());
    }
  }

  /**
   * 获取本地化状态列表
   * 查询指定容器的资源本地化状态
   * 
   * @param request 获取本地化状态请求
   * @return 本地化状态响应，包含成功和失败的请求
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  @Override
  public GetLocalizationStatusesResponse getLocalizationStatuses(
      GetLocalizationStatusesRequest request) throws YarnException,
      IOException {
    Map<ContainerId, List<LocalizationStatus>> allStatuses = new HashMap<>();
    Map<ContainerId, SerializedException> failedRequests = new HashMap<>();

    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    String remoteUser = remoteUgi.getUserName();
    for (ContainerId id : request.getContainerIds()) {
      try {
        List<LocalizationStatus> statuses = getLocalizationStatusesInternal(id,
            identifier, remoteUser);
        allStatuses.put(id, statuses);
      } catch (YarnException e) {
        failedRequests.put(id, SerializedException.newInstance(e));
      }
    }
    return GetLocalizationStatusesResponse.newInstance(allStatuses,
        failedRequests);
  }

  /**
   * 获取本地化状态内部实现
   * 验证权限并返回容器的本地化状态
   * 
   * @param containerID 容器ID
   * @param nmTokenIdentifier NM令牌标识符
   * @param remoteUser 远程用户
   * @return 本地化状态列表
   * @throws YarnException YARN异常
   */
  private List<LocalizationStatus> getLocalizationStatusesInternal(
      ContainerId containerID,
      NMTokenIdentifier nmTokenIdentifier, String remoteUser)
      throws YarnException {
    Container container = this.context.getContainers().get(containerID);

    LOG.info("Getting localization status for {}", containerID);
    authorizeGetAndStopContainerRequest(containerID, container, false,
        nmTokenIdentifier, remoteUser);

    String containerIDStr = containerID.toString();
    if (container == null) {
      if (nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
            + " was recently stopped on node manager.");
      } else {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
            + " is not handled by this NodeManager");
      }
    }
    return container.getLocalizationStatuses();
  }

  /**
   * 获取资源本地化服务
   * 
   * @return 资源本地化服务实例
   */
  public ResourceLocalizationService getResourceLocalizationService() {
    return rsrcLocalizationSrvc;
  }

  /**
   * 获取异步事件分发器
   * 
   * @return 事件分发器实例
   */
  public AsyncDispatcher getDispatcher() {
    return dispatcher;
  }
}

// 这个文件已经全部加上中文注释

