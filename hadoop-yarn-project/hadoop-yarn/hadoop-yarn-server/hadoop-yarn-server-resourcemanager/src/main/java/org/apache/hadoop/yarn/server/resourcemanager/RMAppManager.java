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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.security.ConfiguredYarnAuthorizer;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.server.resourcemanager.federation.FederationStateStoreService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;

import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRecoverEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Times;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.yarn.util.StringHelper;

import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.apache.commons.lang.StringUtils.isNotEmpty;

/**
 * RM应用管理器，负责ResourceManager中所有应用程序的生命周期管理，包括提交、恢复、完成清理、队列迁移等核心操作。
 */
public class RMAppManager implements EventHandler<RMAppManagerEvent>,
                                        Recoverable {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMAppManager.class);

  private int maxCompletedAppsInMemory;
  private int maxCompletedAppsInStateStore;
  protected int completedAppsInStateStore = 0;
  private LinkedList<ApplicationId> completedApps = new LinkedList<>();

  private final RMContext rmContext;
  private final ApplicationMasterService masterService;
  private final YarnScheduler scheduler;
  private final ApplicationACLsManager applicationACLsManager;
  private Configuration conf;
  private YarnAuthorizationProvider authorizer;
  private boolean timelineServiceV2Enabled;
  private boolean nodeLabelsEnabled;
  private Set<String> exclusiveEnforcedPartitions;
  private String amDefaultNodeLabel;
  private FederationStateStoreService federationStateStoreService;

  private static final String USER_ID_PREFIX = "userid=";

  /**
   * 构造RM应用管理器实例，初始化配置参数。
   * @param context RM上下文对象
   * @param scheduler YARN调度器实例
   * @param masterService AM服务实例
   * @param applicationACLsManager 应用ACL管理器
   * @param conf 配置对象
   */
  public RMAppManager(RMContext context,
      YarnScheduler scheduler, ApplicationMasterService masterService,
      ApplicationACLsManager applicationACLsManager, Configuration conf) {
    this.rmContext = context;
    this.scheduler = scheduler;
    this.masterService = masterService;
    this.applicationACLsManager = applicationACLsManager;
    this.conf = conf;
    this.maxCompletedAppsInMemory = conf.getInt(
        YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS,
        YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS);
    this.maxCompletedAppsInStateStore =
        conf.getInt(
          YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS,
          this.maxCompletedAppsInMemory);
    if (this.maxCompletedAppsInStateStore > this.maxCompletedAppsInMemory) {
      this.maxCompletedAppsInStateStore = this.maxCompletedAppsInMemory;
    }
    this.authorizer = YarnAuthorizationProvider.getInstance(conf);
    this.timelineServiceV2Enabled = YarnConfiguration.
        timelineServiceV2Enabled(conf);
    this.nodeLabelsEnabled = YarnConfiguration
        .areNodeLabelsEnabled(rmContext.getYarnConfiguration());
    this.exclusiveEnforcedPartitions = YarnConfiguration
        .getExclusiveEnforcedPartitions(rmContext.getYarnConfiguration());
    this.amDefaultNodeLabel = conf
        .get(YarnConfiguration.AM_DEFAULT_NODE_LABEL, null);
  }

  /**
   *  This class is for logging the application summary.
   */
  static class ApplicationSummary {
    static final Logger LOG = LoggerFactory.
        getLogger(ApplicationSummary.class);

    // Escape sequences
    static final char EQUALS = '=';
    static final char[] charsToEscape =
      {StringUtils.COMMA, EQUALS, StringUtils.ESCAPE_CHAR};

    static class SummaryBuilder {
      final StringBuilder buffer = new StringBuilder();

      // A little optimization for a very common case
      SummaryBuilder add(String key, long value) {
        return _add(key, Long.toString(value));
      }

      <T> SummaryBuilder add(String key, T value) {
        String escapedString = StringUtils.escapeString(String.valueOf(value),
            StringUtils.ESCAPE_CHAR, charsToEscape).replaceAll("\n", "\\\\n")
            .replaceAll("\r", "\\\\r");
        return _add(key, escapedString);
      }

      SummaryBuilder add(SummaryBuilder summary) {
        if (buffer.length() > 0) buffer.append(StringUtils.COMMA);
        buffer.append(summary.buffer);
        return this;
      }

      SummaryBuilder _add(String key, String value) {
        if (buffer.length() > 0) buffer.append(StringUtils.COMMA);
        buffer.append(key).append(EQUALS).append(value);
        return this;
      }

      @Override public String toString() {
        return buffer.toString();
      }
    }

    /**
     * create a summary of the application's runtime.
     *
     * @param app {@link RMApp} whose summary is to be created, cannot
     *            be <code>null</code>.
     */
    public static SummaryBuilder createAppSummary(RMApp app) {
      String trackingUrl = "N/A";
      String host = "N/A";
      RMAppAttempt attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        trackingUrl = attempt.getTrackingUrl();
        Container masterContainer = attempt.getMasterContainer();
        if (masterContainer != null) {
          NodeId nodeId = masterContainer.getNodeId();
          if (nodeId != null) {
            String amHost = nodeId.getHost();
            if (amHost != null) {
              host = amHost;
            }
          }
        }
      }
      RMAppMetrics metrics = app.getRMAppMetrics();
      SummaryBuilder summary = new SummaryBuilder()
          .add("appId", app.getApplicationId())
          .add("name", app.getName())
          .add("user", app.getUser())
          .add("queue", app.getQueue())
          .add("state", app.getState())
          .add("trackingUrl", trackingUrl)
          .add("appMasterHost", host)
          .add("submitTime", app.getSubmitTime())
          .add("startTime", app.getStartTime())
          .add("launchTime", app.getLaunchTime())
          .add("finishTime", app.getFinishTime())
          .add("finalStatus", app.getFinalApplicationStatus())
          .add("memorySeconds", metrics.getMemorySeconds())
          .add("vcoreSeconds", metrics.getVcoreSeconds())
          .add("preemptedMemorySeconds", metrics.getPreemptedMemorySeconds())
          .add("preemptedVcoreSeconds", metrics.getPreemptedVcoreSeconds())
          .add("preemptedAMContainers", metrics.getNumAMContainersPreempted())
          .add("preemptedNonAMContainers", metrics.getNumNonAMContainersPreempted())
          .add("preemptedResources", metrics.getResourcePreempted())
          .add("applicationType", app.getApplicationType())
          .add("resourceSeconds", StringHelper
              .getResourceSecondsString(metrics.getResourceSecondsMap()))
          .add("preemptedResourceSeconds", StringHelper
              .getResourceSecondsString(
                  metrics.getPreemptedResourceSecondsMap()))
          .add("applicationTags", StringHelper.CSV_JOINER.join(
              app.getApplicationTags() != null ? new TreeSet<>(
                  app.getApplicationTags()) : Collections.<String>emptySet()))
          .add("applicationNodeLabel",
              app.getApplicationSubmissionContext().getNodeLabelExpression()
                  == null
                  ? ""
                  : app.getApplicationSubmissionContext()
                      .getNodeLabelExpression())
          .add("diagnostics", app.getDiagnostics())
          .add("totalAllocatedContainers",
              metrics.getTotalAllocatedContainers());
      return summary;
    }

    /**
     * Log a summary of the application's runtime.
     *
     * @param app {@link RMApp} whose summary is to be logged
     */
    public static void logAppSummary(RMApp app) {
      if (app != null) {
        LOG.info(createAppSummary(app).toString());
      }
    }
  }

  @VisibleForTesting
  public void logApplicationSummary(ApplicationId appId) {
    ApplicationSummary.logAppSummary(rmContext.getRMApps().get(appId));
  }

  private static <V> V getChecked(Future<V> future) throws YarnException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new YarnException(e);
    } catch (ExecutionException e) {
      throw new YarnException(e);
    }
  }

  protected synchronized int getCompletedAppsListSize() {
    return this.completedApps.size();
  }

  /**
   * 处理应用完成后的收尾工作，包括更新令牌、记录完成状态、写入审计日志。
   * @param applicationId 已完成的应用ID
   */
  protected synchronized void finishApplication(ApplicationId applicationId) {
    if (applicationId == null) {
      LOG.error("RMAppManager received completed appId of null, skipping");
    } else {
      // 通知令牌续订器应用已完成
      if (UserGroupInformation.isSecurityEnabled()) {
        rmContext.getDelegationTokenRenewer().applicationFinished(applicationId);
      }

      completedApps.add(applicationId);
      completedAppsInStateStore++;
      writeAuditLog(applicationId);
    }
  }

  protected void writeAuditLog(ApplicationId appId) {
    RMApp app = rmContext.getRMApps().get(appId);
    String operation = "UNKNOWN";
    boolean success = false;
    // 根据应用最终状态确定审计日志操作类型
    switch (app.getState()) {
      case FAILED:
        operation = AuditConstants.FINISH_FAILED_APP;
        break;
      case FINISHED:
        operation = AuditConstants.FINISH_SUCCESS_APP;
        success = true;
        break;
      case KILLED:
        operation = AuditConstants.FINISH_KILLED_APP;
        success = true;
        break;
      default:
        break;
    }

    // 写入对应结果的审计日志
    if (success) {
      RMAuditLogger.logSuccess(app.getUser(), operation,
          "RMAppManager", app.getApplicationId());
    } else {
      StringBuilder diag = app.getDiagnostics();
      String msg = diag == null ? null : diag.toString();
      RMAuditLogger.logFailure(app.getUser(), operation, msg, "RMAppManager",
          "App failed with state: " + app.getState(), appId);
    }
  }

  /*
   * check to see if hit the limit for max # completed apps kept
   */
  protected synchronized void checkAppNumCompletedLimit() {
    // 先检查状态存储中已完成应用数量，超出限制则移除最早的应用
    while (completedAppsInStateStore > this.maxCompletedAppsInStateStore) {
      ApplicationId removeId =
          completedApps.get(completedApps.size() - completedAppsInStateStore);
      RMApp removeApp = rmContext.getRMApps().get(removeId);
      LOG.info("Max number of completed apps kept in state store met:"
          + " maxCompletedAppsInStateStore = " + maxCompletedAppsInStateStore
          + ", removing app " + removeApp.getApplicationId()
          + " from state store.");
      rmContext.getStateStore().removeApplication(removeApp);
      removeApplicationIdFromStateStore(removeId);
      completedAppsInStateStore--;
    }

    // 再检查内存中已完成应用数量，超出限制则移除最早的应用
    while (completedApps.size() > this.maxCompletedAppsInMemory) {
      ApplicationId removeId = completedApps.remove();
      LOG.info("Application should be expired, max number of completed apps"
          + " kept in memory met: maxCompletedAppsInMemory = "
          + this.maxCompletedAppsInMemory + ", removing app " + removeId
          + " from memory: ");
      rmContext.getRMApps().remove(removeId);
      removeApplicationIdFromStateStore(removeId);
      this.applicationACLsManager.removeApplication(removeId);
    }
  }

  @VisibleForTesting
  @Deprecated
  protected void submitApplication(
      ApplicationSubmissionContext submissionContext, long submitTime,
      String user) throws YarnException {
    submitApplication(submissionContext, submitTime,
        UserGroupInformation.createRemoteUser(user));
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  protected void submitApplication(
      ApplicationSubmissionContext submissionContext, long submitTime,
      UserGroupInformation userUgi) throws YarnException {
    ApplicationId applicationId = submissionContext.getApplicationId();

    // Passing start time as -1. It will be eventually set in RMAppImpl
    // constructor.
    RMAppImpl application = createAndPopulateNewRMApp(
        submissionContext, submitTime, userUgi, false, -1, null);
    try {
      if (UserGroupInformation.isSecurityEnabled()) {
        this.rmContext.getDelegationTokenRenewer()
            .addApplicationAsync(applicationId,
                BuilderUtils.parseCredentials(sub