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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerSubState;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.UpdateContainerSchedulerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceSet;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStartMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStopMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEventType;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN NodeManager 容器核心实现，基于状态机管理容器整个生命周期，处理容器从初始化、本地化、启动、运行到退出的全流程状态转换
 */
public class ContainerImpl implements Container {
  /**
   * 资源本地化计数器，统计本地化过程中缓存命中/未命中情况
   */
  private enum LocalizationCounter {
    // 1-to-1 correspondence with MR TaskCounter.LOCALIZED_*
    BYTES_MISSED,
    BYTES_CACHED,
    FILES_MISSED,
    FILES_CACHED,
    MILLIS;
  }

  /**
   * 容器重新初始化上下文，保存新旧上下文用于支持升级回滚
   */
  private static final class ReInitializationContext {
    private final ContainerLaunchContext newLaunchContext;
    private final ResourceSet newResourceSet;

    // Rollback state
    private final ContainerLaunchContext oldLaunchContext;
    private final ResourceSet oldResourceSet;

    private boolean isRollback = false;

    private ReInitializationContext(ContainerLaunchContext newLaunchContext,
        ResourceSet newResourceSet,
        ContainerLaunchContext oldLaunchContext,
        ResourceSet oldResourceSet) {
      this.newLaunchContext = newLaunchContext;
      this.newResourceSet = newResourceSet;
      this.oldLaunchContext = oldLaunchContext;
      this.oldResourceSet = oldResourceSet;
    }

    /**
     * 判断是否支持回滚
     */
    private boolean canRollback() {
      return (oldLaunchContext != null);
    }

    /**
     * 合并当前资源集和新资源集，处理回滚场景特殊逻辑
     */
    private ResourceSet mergedResourceSet(ResourceSet current) {
      if (isRollback) {
        // No merging should be done for rollback
        return newResourceSet;
      }
      if (current == newResourceSet) {
        // This happens during a restart
        return current;
      }
      return ResourceSet.merge(current, newResourceSet);
    }

    /**
     * 创建回滚专用上下文
     */
    private ReInitializationContext createContextForRollback() {
      ReInitializationContext cntxt = new ReInitializationContext(
          oldLaunchContext, oldResourceSet, null, null);
      cntxt.isRollback = true;
      return cntxt;
    }
  }

  private final SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private final Lock readLock;
  private final Lock writeLock;
  private final Dispatcher dispatcher;
  private final NMStateStoreService stateStore;
  private final Credentials credentials;
  private final NodeManagerMetrics metrics;
  private final long[] localizationCounts =
      new long[LocalizationCounter.values().length];

  private volatile ContainerLaunchContext launchContext;
  private volatile ContainerTokenIdentifier containerTokenIdentifier;
  private final ContainerId containerId;
  private final String user;
  private int version;
  private int exitCode = ContainerExitStatus.INVALID;
  private final StringBuilder diagnostics;
  private final int diagnosticsMaxSize;
  private boolean wasLaunched;
  private boolean wasPaused;
  private long containerLocalizationStartTime;
  private long containerLaunchStartTime;
  private ContainerMetrics containerMetrics;
  private static Clock clock = SystemClock.getInstance();

  private ContainerRetryContext containerRetryContext;
  private SlidingWindowRetryPolicy.RetryContext windowRetryContext;
  private SlidingWindowRetryPolicy retryPolicy;

  private String csiVolumesRootDir;
  private String workDir;
  private String logDir;
  private String host;
  private String ips;
  private String exposedPorts;
  private volatile ReInitializationContext reInitContext;
  private volatile boolean isReInitializing = false;
  private volatile boolean isMarkeForKilling = false;
  private Object containerRuntimeData;

  /** The NM-wide configuration - not specific to this container */
  private final Configuration daemonConf;
  private final long startTime;

  private static final Logger LOG =
       LoggerFactory.getLogger(ContainerImpl.class);

  // whether container has been recovered after a restart
  private RecoveredContainerStatus recoveredStatus =
      RecoveredContainerStatus.REQUESTED;
  // whether container was marked as killed after recovery
  private boolean recoveredAsKilled = false;
  private Context context;
  private ResourceSet resourceSet;
  private ResourceMappings resourceMappings;

  /**
   * 构造新容器实例
   */
  public ContainerImpl(Configuration conf, Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics,
      ContainerTokenIdentifier containerTokenIdentifier, Context context) {
    this(conf, dispatcher, launchContext, creds, metrics,
        containerTokenIdentifier, context, SystemClock.getInstance().getTime());
  }

  /**
   * 构造容器实例，指定启动时间戳
   */
  public ContainerImpl(Configuration conf, Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics,
      ContainerTokenIdentifier containerTokenIdentifier, Context context,
      long startTs) {
    this.startTime = startTs;
    this.daemonConf = conf;
    this.dispatcher = dispatcher;
    this.stateStore = context.getNMStateStore();
    this.version = containerTokenIdentifier.getVersion();
    this.launchContext = launchContext;

    // 从配置读取诊断信息最大长度
    this.diagnosticsMaxSize = conf.getInt(
        YarnConfiguration.NM_CONTAINER_DIAGNOSTICS_MAXIMUM_SIZE,
        YarnConfiguration.DEFAULT_NM_CONTAINER_DIAGNOSTICS_MAXIMUM_SIZE);
    this.containerTokenIdentifier = containerTokenIdentifier;
    this.containerId = containerTokenIdentifier.getContainerID();
    this.diagnostics = new StringBuilder();
    this.credentials = creds;
    this.metrics = metrics;
    user = containerTokenIdentifier.getApplicationSubmitter();
    // 初始化读写锁，保证状态机操作线程安全
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    this.context = context;
    // 根据配置决定是否启用容器指标采集
    boolean containerMetricsEnabled =
        conf.getBoolean(YarnConfiguration.NM_CONTAINER_METRICS_ENABLE,
            YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_ENABLE);

    if (containerMetricsEnabled) {
      long flushPeriod =
          conf.getLong(YarnConfiguration.NM_CONTAINER_METRICS_PERIOD_MS,
              YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_PERIOD_MS);
      long unregisterDelay = conf.getLong(
          YarnConfiguration.NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS,
          YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS);
      containerMetrics = ContainerMetrics
          .forContainer(containerId, flushPeriod, unregisterDelay);
      containerMetrics.recordStartTime(clock.getTime());
    }

    // Configure the Retry Context
    this.containerRetryContext = configureRetryContext(
        conf, launchContext, this.containerId);
    this.windowRetryContext = new SlidingWindowRetryPolicy
        .RetryContext(containerRetryContext);
    this.retryPolicy = new SlidingWindowRetryPolicy(clock);

    // 创建状态机实例，初始状态为NEW
    stateMachine = stateMachineFactory.make(this, ContainerState.NEW,
        context.getContainerStateTransitionListener());
    this.context = context;
    this.resourceSet = new ResourceSet();
    this.resourceMappings = new ResourceMappings();
  }

  /**
   * 配置容器重试上下文，应用配置的最小重启间隔
   */
  private static ContainerRetryContext configureRetryContext(
      Configuration conf, ContainerLaunchContext launchContext,
      ContainerId containerId) {
    ContainerRetryContext context;
    // 优先使用启动上下文中的重试配置
    if (launchContext != null
        && launchContext.getContainerRetryContext() != null) {
      context = launchContext.getContainerRetryContext();
    } else {
      context = ContainerRetryContext.NEVER_RETRY_CONTEXT;
    }
    // 如果配置的重试间隔小于最小要求，覆盖为最小值
    int minimumRestartInterval = conf.getInt(
        YarnConfiguration.NM_CONTAINER_RETRY_MINIMUM_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_CONTAINER_RETRY_MINIMUM_INTERVAL_MS);
    if (context.getRetryPolicy() != ContainerRetryPolicy.NEVER_RETRY
        && context.getRetryInterval() < minimumRestartInterval) {
      LOG.info("Set restart interval to minimum value " + minimumRestartInterval
          + "ms for container " + containerId);
      context.setRetryInterval(minimumRestartInterval);
    }
    return context;
  }

  /**
   * 从NM恢复状态中构造容器实例，用于NodeManager重启后恢复容器
   */
  public ContainerImpl(Configuration conf, Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics,
      ContainerTokenIdentifier containerTokenIdentifier, Context context,
      RecoveredContainerState rcs) {
    this(conf, dispatcher, launchContext, creds, metrics,
        containerTokenIdentifier, context, rcs.getStartTime());
    this.recoveredStatus = rcs.getStatus();
    this.exitCode = rcs.getExitCode();
    this.recoveredAsKilled = rcs.getKilled();
    this.diagnostics.append(rcs.getDiagnostics());
    this.version = rcs.getVersion();
    this.windowRetryContext.setRemainingRetries(
        rcs.getRemainingRetryAttempts());
    this.windowRetryContext.setRestartTimes(rcs.getRestartTimes());
    this.workDir = rcs.getWorkDir();
    this.logDir = rcs.getLogDir();
    this.resourceMappings = rcs.getResourceMappings();
  }

  private static final ContainerDiagnosticsUpdateTransition UPDATE_DIAGNOSTICS_TRANSITION =
      new ContainerDiagnosticsUpdateTransition();

  // State Machine for each container.
  // 定义容器全生命周期状态机，包含所有状态转换规则
  private static StateMachineFactory
           <ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>
        stateMachineFactory =
      new StateMachineFactory<ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>(ContainerState.NEW)
    // From NEW State
    .addTransition(ContainerState.NEW,
        EnumSet.of(ContainerState.LOCALIZING,
            ContainerState.SCHEDULED,
            ContainerState.LOCALIZATION_FAILED,
            ContainerState.DONE),
        ContainerEventType.INIT_CONTAINER, new RequestResourcesTransition())
    .addTransition(ContainerState.NEW, ContainerState.NEW,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.NEW, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER, new KillOnNewTransition())
    .addTransition(ContainerState.NEW, ContainerState.NEW,
        ContainerEventType.UPDATE_CONTAINER_TOKEN, new UpdateTransition())

    // From LOCALIZING State
    .addTransition(ContainerState.LOCALIZING,
        EnumSet.of(ContainerState.LOCALIZING, ContainerState.SCHEDULED),
        ContainerEventType.RESOURCE_LOCALIZED, new LocalizedTransition())
    .addTransition(ContainerState.LOCALIZING,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_FAILED,