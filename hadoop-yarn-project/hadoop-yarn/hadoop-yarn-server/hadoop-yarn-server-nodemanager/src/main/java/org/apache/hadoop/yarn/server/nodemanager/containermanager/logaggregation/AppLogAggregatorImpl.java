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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationDFSException;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.api.ContainerLogAggregationPolicy;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.security.NMDelegationTokenManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier.HDFS_DELEGATION_KIND;

/**
 * 应用日志聚合器实现，负责将当前NM节点上单个应用的所有容器日志聚合上传到远程文件系统。
 * 支持滚动日志聚合（应用运行时定期上传已滚动的日志）和应用结束后全量聚合两种模式。
 */
public class AppLogAggregatorImpl implements AppLogAggregator {

  private static final Logger LOG =
       LoggerFactory.getLogger(AppLogAggregatorImpl.class);
  // 非滚动模式下线程等待间隔
  private static final int THREAD_SLEEP_TIME = 1000;

  private final LocalDirsHandlerService dirsHandler;
  private final Dispatcher dispatcher;
  private final ApplicationId appId;
  private final String applicationId;
  private final boolean enableLocalCleanup;
  private boolean logAggregationDisabled = false;
  private final Configuration conf;
  private final DeletionService delService;
  private final UserGroupInformation userUgi;
  private final Path remoteNodeLogFileForApp;
  private final Path remoteNodeTmpLogFileForApp;

  // 待聚合日志的容器ID队列
  private final BlockingQueue<ContainerId> pendingContainers;
  private final AtomicBoolean appFinishing = new AtomicBoolean();
  private final AtomicBoolean appAggregationFinished = new AtomicBoolean();
  private final AtomicBoolean aborted = new AtomicBoolean();
  private final Map<ApplicationAccessType, String> appAcls;
  private final FileContext lfs;
  private final LogAggregationContext logAggregationContext;
  private final Context context;
  private final NodeId nodeId;
  private final LogAggregationFileControllerContext logControllerContext;

  // 这些变量仅用于单元测试
  private final AtomicBoolean waiting = new AtomicBoolean(false);
  private int logAggregationTimes = 0;
  private long logFileSizeThreshold;
  private boolean renameTemporaryLogFileFailed = false;

  // 当前未完成聚合的容器日志聚合器映射表
  private final Map<ContainerId, ContainerLogAggregator> containerLogAggregators =
      new HashMap<ContainerId, ContainerLogAggregator>();
  private final ContainerLogAggregationPolicy logAggPolicy;

  private final LogAggregationFileController logAggregationFileController;

  private NMDelegationTokenManager delegationTokenManager;

  /**
   * 从状态存储恢复的应用日志初始化时间，用于日志保留策略判断。
   * 如果未恢复则为-1，超过保留时间的日志不会上传而是直接清理。
   */
  private final long recoveredLogInitedTime;

  /**
   * 构造AppLogAggregatorImpl，默认恢复时间为-1。
   */
  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs, long rollingMonitorInterval) {
    this(dispatcher, deletionService, conf, appId, userUgi, nodeId,
        dirsHandler, remoteNodeLogFileForApp, appAcls,
        logAggregationContext, context, lfs, rollingMonitorInterval, -1, null);
  }

  /**
   * 构造AppLogAggregatorImpl，指定恢复的日志初始化时间。
   */
  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs, long rollingMonitorInterval,
      long recoveredLogInitedTime) {
    this(dispatcher, deletionService, conf, appId, userUgi, nodeId,
        dirsHandler, remoteNodeLogFileForApp, appAcls,
        logAggregationContext, context, lfs, rollingMonitorInterval,
        recoveredLogInitedTime, null);
  }

  /**
   * 完整构造方法，支持注入日志聚合文件控制器，用于测试。
   */
  public AppLogAggregatorImpl(Dispatcher dispatcher,
      DeletionService deletionService, Configuration conf,
      ApplicationId appId, UserGroupInformation userUgi, NodeId nodeId,
      LocalDirsHandlerService dirsHandler, Path remoteNodeLogFileForApp,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, Context context,
      FileContext lfs, long rollingMonitorInterval,
      long recoveredLogInitedTime,
      LogAggregationFileController logAggregationFileController) {
    this.dispatcher = dispatcher;
    this.conf = conf;
    this.delService = deletionService;
    this.appId = appId;
    this.applicationId = appId.toString();
    this.userUgi = userUgi;
    this.dirsHandler = dirsHandler;
    this.pendingContainers = new LinkedBlockingQueue<ContainerId>();
    this.appAcls = appAcls;
    this.lfs = lfs;
    this.logAggregationContext = logAggregationContext;
    this.context = context;
    this.nodeId = nodeId;
    this.enableLocalCleanup =
        conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLE_LOCAL_CLEANUP,
            YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLE_LOCAL_CLEANUP);
    if (!this.enableLocalCleanup) {
      LOG.warn("{} is only for testing and not for any production system ",
          YarnConfiguration.LOG_AGGREGATION_ENABLE_LOCAL_CLEANUP);
    }
    this.logAggPolicy = getLogAggPolicy(conf);
    this.recoveredLogInitedTime = recoveredLogInitedTime;
    this.logFileSizeThreshold =
        conf.getLong(YarnConfiguration.LOG_AGGREGATION_DEBUG_FILESIZE,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_DEBUG_FILESIZE);
    if (logAggregationFileController == null) {
      // 默认使用TFile格式控制器
      this.logAggregationFileController = new LogAggregationTFileController();
      this.logAggregationFileController.initialize(conf, "TFile");
      this.logAggregationFileController.verifyAndCreateRemoteLogDir();
      this.logAggregationFileController.createAppDir(
          this.userUgi.getShortUserName(), appId, userUgi);
      this.remoteNodeLogFileForApp = this.logAggregationFileController
          .getRemoteNodeLogFileForApp(appId,
              this.userUgi.getShortUserName(), nodeId);
      this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    } else {
      this.logAggregationFileController = logAggregationFileController;
      this.remoteNodeLogFileForApp = remoteNodeLogFileForApp;
      this.remoteNodeTmpLogFileForApp = getRemoteNodeTmpLogFileForApp();
    }
    // 判断是否开启滚动日志聚合
    boolean logAggregationInRolling =
        rollingMonitorInterval > 0 && this.logAggregationContext != null
            && this.logAggregationContext.getRolledLogsIncludePattern() != null
            && !this.logAggregationContext.getRolledLogsIncludePattern()
                .isEmpty();
    if (logAggregationInRolling) {
      LOG.info("Rolling mode is turned on with include pattern {}",
          this.logAggregationContext.getRolledLogsIncludePattern());
    } else {
      LOG.debug("Rolling mode is turned off");
    }
    // 初始化日志聚合控制器上下文
    logControllerContext = new LogAggregationFileControllerContext(
            this.remoteNodeLogFileForApp,
            this.remoteNodeTmpLogFileForApp,
            logAggregationInRolling,
            rollingMonitorInterval,
            this.appId, this.appAcls, this.nodeId, this.userUgi);
    // 初始化Delegation Token管理器
    delegationTokenManager = new NMDelegationTokenManager(conf);
  }

  /**
   * 获取容器日志聚合策略，优先使用应用指定策略，否则使用集群默认配置。
   */
  private ContainerLogAggregationPolicy getLogAggPolicy(Configuration conf) {
    ContainerLogAggregationPolicy policy = getLogAggPolicyInstance(conf);
    String params = getLogAggPolicyParameters(conf);
    if (params != null) {
      policy.parseParameters(params);
    }
    return policy;
  }

  /**
   * 获取日志聚合策略实例，优先读取应用上下文指定的策略类，否则读取集群默认配置。
   */
  private ContainerLogAggregationPolicy getLogAggPolicyInstance(
      Configuration conf) {
    Class<? extends ContainerLogAggregationPolicy> policyClass = null;
    if (this.logAggregationContext != null) {
      String className =
          this.logAggregationContext.getLogAggregationPolicyClassName();
      if (className != null) {
        try {
          Class<?> policyFromContext = conf.getClassByName(className);
          if (ContainerLogAggregationPolicy.class.isAssignableFrom(
              policyFromContext)) {
            policyClass = policyFromContext.asSubclass(
                ContainerLogAggregationPolicy.class);
          } else {
            LOG.warn(this.appId + " specified invalid log aggregation policy " +
                className);
          }
        } catch (ClassNotFoundException cnfe) {
          // 策略类无效不会导致应用失败，降级使用默认策略
          LOG.warn(this.appId + " specified invalid log aggregation policy " +
              className);
        }
      }
    }
    // 如果应用未指定，读取集群默认配置
    if (policyClass == null) {
      policyClass = conf.getClass(YarnConfiguration.NM_LOG_AGG_POLICY_CLASS,
          AllContainerLogAggregationPolicy.class,
              ContainerLogAggregationPolicy.class);
    } else {
      LOG.info(this.appId + " specifies ContainerLogAggregationPolicy of "
          + policyClass);
    }
    return ReflectionUtils.newInstance(policyClass, conf);
  }

  /**
   * 获取日志聚合策略参数，优先读取应用上下文指定参数，否则读取集群默认配置。
   */
  private String getLogAggPolicyParameters(Configuration conf) {
    String params = null;
    if (this.logAggregationContext != null) {
      params = this.logAggregationContext.getLogAggregationPolicyParameters();
    }
    if (params == null) {
      params = conf.get(YarnConfiguration.NM_LOG_AGG_POLICY_CLASS_PARAMETERS);
    }
    return params;
  }

  /**
   * 为本次聚合周期上传所有待处理容器的日志。
   * @param appFinished 应用是否已结束
   * @throws LogAggregationDFSException 远程DFS操作失败抛出异常
   */
  private void uploadLogsForContainers(boolean appFinished)
      throws LogAggregationDFSException {
    if (this.logAggregationDisabled) {
      return;
    }

    // 添加系统凭证到用户UGI
    addCredentials();
    try {
      // 移除过期的Delegation Token，刷新有效Token
      removeExpiredDelegationTokens();
    } catch (IOException | InterruptedException e) {
      LOG.warn("Removing expired delegation tokens failed for " + appId, e);
    }
    // 收集本次聚合周期需要处理的容器：
    // 1. 所有已完成并满足聚合策略的待处理容器
    // 2. 滚动聚合模式下正在运行、且满足聚合策略的容器
    Set<ContainerId> pendingContainerInThisCycle = new HashSet<ContainerId>();
    this.pendingContainers.drainTo(pendingContainerInThisCycle);
    Set<ContainerId> finishedContainers =
        new HashSet<ContainerId>(pendingContainerInThisCycle);
    // 遍历当前应用所有正在运行的容器，检查是否需要聚合
    if (this.context.getApplications().get(this.appId) != null) {
      for (Container container : this.context.getApplications()
        .get(this.appId).getContainers().values()) {
        ContainerType containerType =
            container.getContainerTokenIdentifier().getContainerType();
        if (shouldUploadLogs(new ContainerLogContext(
            container.getContainerId(), containerType, 0))) {
          pendingContainerInThisCycle.add(container.getContainerId());
        }
      }
    }

    if (pendingContainerInThisCycle.isEmpty()) {
      LOG.debug("No pending container in this cycle");
      sendLogAggregationReport(true, "", appFinished);
      return;
    }

    logAggregationTimes++;
    LOG.debug("Cycle #{} of log aggregator", logAggregationTimes);
    String diagnosticMessage = "";
    boolean logAggregationSucceedInThisCycle = true;
    DeletionTask deletionTask = null;
    try {
      try {
        // 初始化远程日志写入器
        logAggregationFileController.initializeWriter(logControllerContext);
      } catch (IOException e1) {
        logAggregationSucceedInThisCycle = false;
        LOG.error("Cannot create writer for app " + this.applicationId
            + ". Skip log upload this time. ", e1);
        return;
      }

      boolean uploadedLogsInThisCycle = false;
      // 遍历处理每个待聚合容器
      for (ContainerId container : pendingContainerInThisCycle) {
        ContainerLogAggregator aggregator = null;
        if (containerLogAggregators.containsKey(container