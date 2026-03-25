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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.security.token.SecretManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
完成
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;


import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * NodeManager 日志聚合服务，负责将本节点上容器运行产生的日志聚合上传到远端存储系统。
 * 支持滚动日志聚合（应用运行中定期聚合日志）和应用结束后全量聚合两种模式，
 * 是 YARN 日志收集体系的核心服务端组件。
 */
public class LogAggregationService extends AbstractService implements
    LogHandler {

  private static final Logger LOG =
       LoggerFactory.getLogger(LogAggregationService.class);
  // This configuration is for debug and test purpose. By setting
  // this configuration as true. We can break the lower bound of
  // NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS.
  private static final String NM_LOG_AGGREGATION_DEBUG_ENABLED
      = YarnConfiguration.NM_PREFIX + "log-aggregation.debug-enabled";
  /** 滚动日志聚合监控间隔（秒） */
  private long rollingMonitorInterval;

  private final Context context;
  private final DeletionService deletionService;
  private final Dispatcher dispatcher;

  private LocalDirsHandlerService dirsHandler;
  private NodeId nodeId;

  /** 存储每个应用对应的日志聚合器，并发安全 */
  private final ConcurrentMap<ApplicationId, AppLogAggregator> appLogAggregators;

  // Holds applications whose aggregation is disable due to invalid Token
  /** 存储因Token无效被禁用日志聚合的应用列表 */
  private final Set<ApplicationId> invalidTokenApps;

  @VisibleForTesting
  /** 日志聚合线程池，执行异步聚合任务 */
  ExecutorService threadPool;
  
  /**
   * 构造日志聚合服务实例
   * @param dispatcher 事件分发器
   * @param context NodeManager 上下文
   * @param deletionService 删除服务
   * @param dirsHandler 本地目录处理器
   */
  public LogAggregationService(Dispatcher dispatcher, Context context,
      DeletionService deletionService, LocalDirsHandlerService dirsHandler) {
    super(LogAggregationService.class.getName());
    this.dispatcher = dispatcher;
    this.context = context;
    this.deletionService = deletionService;
    this.dirsHandler = dirsHandler;
    this.appLogAggregators =
        new ConcurrentHashMap<ApplicationId, AppLogAggregator>();
    this.invalidTokenApps = ConcurrentHashMap.newKeySet();
  }

  /**
   * 根据配置计算滚动监控间隔，处理最小间隔限制
   * @param conf 配置对象
   * @return 计算后的滚动监控间隔（秒）
   */
  private static long calculateRollingMonitorInterval(Configuration conf) {
    long interval = conf.getLong(
        YarnConfiguration.NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS,
        YarnConfiguration.
            DEFAULT_NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS);

    if (interval <= 0) {
      LOG.info("rollingMonitorInterval is set as " + interval
          + ". The log rolling monitoring interval is disabled. "
          + "The logs will be aggregated after this application is finished.");
    } else {
      boolean logAggregationDebugMode =
          conf.getBoolean(NM_LOG_AGGREGATION_DEBUG_ENABLED, false);
      long minRollingMonitorInterval = conf.getLong(
          YarnConfiguration.MIN_LOG_ROLLING_INTERVAL_SECONDS,
          YarnConfiguration.MIN_LOG_ROLLING_INTERVAL_SECONDS_DEFAULT);

      boolean warnHardMinLimitLowerThanDefault = minRollingMonitorInterval <
          YarnConfiguration.MIN_LOG_ROLLING_INTERVAL_SECONDS_DEFAULT &&
          !logAggregationDebugMode;
      if (warnHardMinLimitLowerThanDefault) {
        LOG.warn("{} has been set to {}, which is less than the default "
            + "minimum value {}. This may impact NodeManager's performance.",
            YarnConfiguration.MIN_LOG_ROLLING_INTERVAL_SECONDS,
            minRollingMonitorInterval,
            YarnConfiguration.MIN_LOG_ROLLING_INTERVAL_SECONDS_DEFAULT);
      }
      boolean lowerThanHardLimit = interval < minRollingMonitorInterval;
      if (lowerThanHardLimit) {
        if (logAggregationDebugMode) {
          LOG.info("Log aggregation debug mode enabled. " +
              "Skipped checking minimum limit.");
        } else {
          LOG.warn("rollingMonitorInterval should be more than " +
              "or equal to {} seconds. Using {} seconds instead.",
              minRollingMonitorInterval, minRollingMonitorInterval);
          interval = minRollingMonitorInterval;
        }
      }
    }
    return interval;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 获取聚合线程池大小
    int threadPoolSize = getAggregatorThreadPoolSize(conf);
    // 创建固定大小线程池执行日志聚合任务
    this.threadPool = HadoopExecutors.newFixedThreadPool(threadPoolSize,
        new ThreadFactoryBuilder()
            .setNameFormat("LogAggregationService #%d")
            .build());
    // 计算滚动监控间隔
    rollingMonitorInterval = calculateRollingMonitorInterval(conf);
    LOG.info("rollingMonitorInterval is set as {}. The logs will be " +
        "aggregated every {} seconds", rollingMonitorInterval,
        rollingMonitorInterval);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // NodeId only available during start, cannot be moved anywhere else.
    // 从上下文获取本节点ID，只能在启动阶段获取
    this.nodeId = this.context.getNodeId();
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    LOG.info(this.getName() + " waiting for pending aggregation during exit");
    // 停止所有聚合任务，等待完成
    stopAggregators();
    super.serviceStop();
  }
   
  /**
   * 停止所有正在进行的日志聚合任务，根据NM恢复配置决定是否中止未完成任务
   */
  private void stopAggregators() {
    threadPool.shutdown();
    boolean supervised = getConfig().getBoolean(
        YarnConfiguration.NM_RECOVERY_SUPERVISED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED);
    // if recovery on restart is supported then leave outstanding aggregations
    // to the next restart
    // 如果支持重启恢复且节点未退役，则留给下一次启动处理未完成聚合
    boolean shouldAbort = context.getNMStateStore().canRecover()
        && !context.getDecommissioned() && supervised;
    // politely ask to finish
    for (AppLogAggregator aggregator : appLogAggregators.values()) {
      if (shouldAbort) {
        aggregator.abortLogAggregation();
      } else {
        aggregator.finishLogAggregation();
      }
    }
    // 等待所有线程执行完成
    while (!threadPool.isTerminated()) {
      for (ApplicationId appId : appLogAggregators.keySet()) {
        LOG.info("Waiting for aggregation to complete for " + appId);
      }
      try {
        if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
          // 超时后发送中断催促任务完成
          threadPool.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.warn("Aggregation stop interrupted!");
        break;
      }
    }
    // 打印未完成聚合的应用警告
    for (ApplicationId appId : appLogAggregators.keySet()) {
      LOG.warn("Some logs may not have been aggregated for " + appId);
    }
  }

  @SuppressWarnings("unchecked")
  /**
   * 初始化应用日志聚合，初始化完成后发送事件通知
   * @param appId 应用ID
   * @param user 对应用户
   * @param credentials 用户凭证
   * @param appAcls 应用访问控制列表
   * @param logAggregationContext 日志聚合上下文
   * @param recoveredLogInitedTime 恢复后的日志初始化时间
   */
  private void initApp(final ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext,
      long recoveredLogInitedTime) {
    ApplicationEvent eventResponse;
    try {
      initAppAggregator(appId, user, credentials, appAcls,
          logAggregationContext, recoveredLogInitedTime);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED);
    } catch (YarnRuntimeException e) {
      LOG.warn("Application failed to init aggregation", e);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED);
    }
    this.dispatcher.getEventHandler().handle(eventResponse);
  }
  
  /**
   * 获取本地文件系统上下文
   * @param conf 配置对象
   * @return 本地文件系统上下文
   */
  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }

  /**
   * 初始化应用级日志聚合器，创建远端应用日志目录，提交聚合任务到线程池
   * @param appId 应用ID
   * @param user 对应用户
   * @param credentials 用户凭证
   * @param appAcls 应用访问控制列表
   * @param logAggregationContext 日志聚合上下文
   * @param recoveredLogInitedTime 恢复后的日志初始化时间
   */
  protected void initAppAggregator(final ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext,
      long recoveredLogInitedTime) {

    // Get user's FileSystem credentials
    // 创建对应用户的UGI，加载用户凭证
    final UserGroupInformation userUgi =
        UserGroupInformation.createRemoteUser(user);
    if (credentials != null) {
      userUgi.addCredentials(credentials);
    }

    // 获取日志聚合文件控制器
    LogAggregationFileController logAggregationFileController =
        getLogAggregationFileController(getConfig());
    // 验证并创建远端根日志目录
    logAggregationFileController.verifyAndCreateRemoteLogDir();
    // New application
    // 创建应用聚合器实例
    final AppLogAggregator appLogAggregator =
        new AppLogAggregatorImpl(this.dispatcher, this.deletionService,
            getConfig(), appId, userUgi, this.nodeId, dirsHandler,
            logAggregationFileController.getRemoteNodeLogFileForApp(appId,
                user, nodeId), appAcls, logAggregationContext, this.context,
            getLocalFileContext(getConfig()), this.rollingMonitorInterval,
            recoveredLogInitedTime, logAggregationFileController);
    // 并发防重检查，避免重复初始化
    if (this.appLogAggregators.putIfAbsent(appId, appLogAggregator) != null) {
      throw new YarnRuntimeException("Duplicate initApp for " + appId);
    }
    // wait until check for existing aggregator to create dirs
    YarnRuntimeException appDirException = null;
    try {
      // Create the app dir
      // 创建远端应用日志目录
      logAggregationFileController.createAppDir(user, appId, userUgi);
    } catch (Exception e) {
      // 创建失败，禁用该应用聚合
      appLogAggregator.disableLogAggregation();

      // add to disabled aggregators if due to InvalidToken
      // 因Token无效导致的失败，加入禁用列表，等待后续Token更新重试
      if (e.getCause() instanceof SecretManager.InvalidToken) {
        invalidTokenApps.add(appId);
      }
      if (!(e instanceof YarnRuntimeException)) {
        appDirException = new YarnRuntimeException(e);
      } else {
        appDirException = (YarnRuntimeException)e;
      }
    }

    // TODO Get the user configuration for the list of containers that need log
    // aggregation.

    // Schedule the aggregator.
    // 包装聚合任务，提交到线程池执行
    Runnable aggregatorWrapper = new Runnable() {
      public void run() {
        try {
          appLogAggregator.run();
        } finally {
          // 执行完成后从聚合器映射移除，关闭文件系统
          appLogAggregators.remove(appId);
          closeFileSystems(userUgi);
        }
      }
    };
    this.threadPool.execute(aggregatorWrapper);

    if (appDirException != null) {
      throw appDirException;
    }
  }

  /**
   * 关闭用户对应所有文件系统，释放资源
   * @param userUgi 用户UGI
   */
  protected void closeFileSystems(final UserGroupInformation userUgi) {
    try {
      FileSystem.closeAllForUGI(userUgi);
    } catch (IOException e) {
      LOG.warn("Failed to close filesystems: ", e);
    }
  }

  // for testing only
  @Private
  int getNumAggregators() {
    return this.appLogAggregators.size();
  }

  /**
   * 处理容器完成事件，触发该容器日志聚合
   * @param containerId 容器ID
   * @param containerType 容器类型
   * @param exitCode 容器退出码
   */
  private void stopContainer(ContainerId containerId,
      ContainerType containerType, int exitCode) {

    // A container is complete. Put this containers' logs up for aggregation if
    // this containers' logs are needed.
    AppLogAggregator aggregator = this.appLogAggregators.get(
        containerId.getApplicationAttemptId().getApplicationId());
    if (aggregator == null) {
      LOG.warn("Log aggregation is not initialized for " + containerId
          + ", did it fail to start?");
      return;
    }
    aggregator.startContainerLogAggregation(
        new ContainerLogContext(containerId, containerType, exitCode));
  }

  @SuppressWarnings("unchecked")
  /**
   * 处理应用完成事件，结束应用日志聚合
   * @param appId 应用ID