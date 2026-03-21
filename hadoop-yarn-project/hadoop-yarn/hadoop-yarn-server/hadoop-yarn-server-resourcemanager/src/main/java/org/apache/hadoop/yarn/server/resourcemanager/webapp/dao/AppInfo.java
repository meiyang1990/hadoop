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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.DeSelectFields;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.DeSelectFields.DeSelectType;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

/**
 * YARN ResourceManager Web UI 应用信息数据访问对象，封装应用完整信息用于Web响应序列化
 * 存储YARN应用的各类元数据、资源使用、抢占信息，支持按配置反选不需要的字段
 */
@XmlRootElement(name = "app")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

  @XmlTransient
  protected String appIdNum;
  @XmlTransient
  protected boolean trackingUrlIsNotReady;
  @XmlTransient
  protected String trackingUrlPretty;
  @XmlTransient
  protected boolean amContainerLogsExist = false;
  @XmlTransient
  protected ApplicationId applicationId;
  @XmlTransient
  private String schemePrefix;
  @XmlTransient
  private SubClusterIdInfo subClusterId;

  // 公开可见字段，所有用户都可访问
  protected String id;
  protected String user;
  private String name;
  protected String queue;
  private YarnApplicationState state;
  protected FinalApplicationStatus finalStatus;
  protected float progress;
  protected String trackingUI;
  protected String trackingUrl;
  protected String diagnostics;
  protected long clusterId;
  protected String rmClusterId;
  protected String applicationType;
  protected String applicationTags = "";
  protected int priority;

  // 需ACL权限验证后才可访问的敏感字段
  protected long startedTime;
  private long launchTime;
  protected long finishedTime;
  protected long elapsedTime;
  protected String amContainerLogs;
  protected String amHostHttpAddress;
  private String amRPCAddress;
  private String masterNodeId;
  private long allocatedMB;
  private long allocatedVCores;
  private long reservedMB;
  private long reservedVCores;
  private int runningContainers;
  private long memorySeconds;
  private long vcoreSeconds;
  protected float queueUsagePercentage;
  protected float clusterUsagePercentage;
  protected Map<String, Long> resourceSecondsMap;

  // 资源抢占信息字段
  private long preemptedResourceMB;
  private long preemptedResourceVCores;
  private int numNonAMContainerPreempted;
  private int numAMContainerPreempted;
  private long preemptedMemorySeconds;
  private long preemptedVcoreSeconds;
  protected Map<String, Long> preemptedResourceSecondsMap;

  // 待处理资源请求列表
  @XmlElement(name = "resourceRequests")
  private List<ResourceRequestInfo> resourceRequests =
      new ArrayList<ResourceRequestInfo>();

  protected LogAggregationStatus logAggregationStatus;
  protected boolean unmanagedApplication;
  protected String appNodeLabelExpression;
  protected String amNodeLabelExpression;

  protected ResourcesInfo resourceInfo = null;
  private AppTimeoutsInfo timeouts;

  public AppInfo() {
  } // JAXB needs this

  /**
   * 从RMApp构建应用信息对象
   * @param rm ResourceManager实例
   * @param app RM应用对象
   * @param hasAccess 当前用户是否拥有应用访问权限
   * @param schemePrefix URL协议前缀(http/https)
   */
  public AppInfo(ResourceManager rm, RMApp app, Boolean hasAccess,
      String schemePrefix) {
    this(rm, app, hasAccess, schemePrefix, new DeSelectFields());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  /**
   * 从RMApp构建应用信息对象，支持反选不需要返回的字段
   * @param rm ResourceManager实例
   * @param app RM应用对象
   * @param hasAccess 当前用户是否拥有应用访问权限
   * @param schemePrefix URL协议前缀(http/https)
   * @param deSelects 需要反选不返回的字段配置
   */
  public AppInfo(ResourceManager rm, RMApp app, Boolean hasAccess,
      String schemePrefix, DeSelectFields deSelects) {
    this.schemePrefix = schemePrefix;
    if (app != null) {
      String trackingUrl = app.getTrackingUrl();
      // 设置应用当前状态
      this.state = app.createApplicationState();
      // 判断跟踪URL是否已就绪
      this.trackingUrlIsNotReady = trackingUrl == null || trackingUrl.isEmpty()
          || YarnApplicationState.NEW == this.state
          || YarnApplicationState.NEW_SAVING == this.state
          || YarnApplicationState.SUBMITTED == this.state
          || YarnApplicationState.ACCEPTED == this.state;
      // 设置跟踪UI类型
      this.trackingUI = this.trackingUrlIsNotReady ? "UNASSIGNED"
          : (app.getFinishTime() == 0 ? "ApplicationMaster" : "History");
      // 拼接完整跟踪URL
      if (!trackingUrlIsNotReady) {
        this.trackingUrl =
            WebAppUtils.getURLWithScheme(schemePrefix, trackingUrl);
        this.trackingUrlPretty = this.trackingUrl;
      } else {
        this.trackingUrlPretty = "UNASSIGNED";
      }
      this.applicationId = app.getApplicationId();
      this.applicationType = app.getApplicationType();
      this.appIdNum = String.valueOf(app.getApplicationId().getId());
      this.id = app.getApplicationId().toString();
      this.user = app.getUser().toString();
      this.name = app.getName().toString();
      this.queue = app.getQueue().toString();
      this.priority = 0;
      this.masterNodeId = "";

      // 设置应用优先级
      if (app.getApplicationPriority() != null) {
        this.priority = app.getApplicationPriority().getPriority();
      }
      // 转换进度为百分比
      this.progress = app.getProgress() * 100;
      this.diagnostics = app.getDiagnostics().toString();
      // 处理空诊断信息
      if (diagnostics == null || diagnostics.isEmpty()) {
        this.diagnostics = "";
      }
      // 将应用标签拼接为逗号分隔字符串
      if (app.getApplicationTags() != null
          && !app.getApplicationTags().isEmpty()) {
        this.applicationTags = Joiner.on(',').join(app.getApplicationTags());
      }
      this.finalStatus = app.getFinalApplicationStatus();
      this.clusterId = ResourceManager.getClusterTimeStamp();
      // 权限验证通过，填充敏感字段
      if (hasAccess) {
        // 获取并设置子集群ID
        if (rm != null && rm.getConfig() != null) {
          try {
            Configuration yarnConfig = rm.getConfig();
            subClusterId = new SubClusterIdInfo(YarnConfiguration.getClusterId(yarnConfig));
            rmClusterId = this.subClusterId.toId().toString();
          } catch (HadoopIllegalArgumentException e) {
            subClusterId = null;
            rmClusterId = null;
          }
        }
        // 设置各类时间信息
        this.startedTime = app.getStartTime();
        this.launchTime = app.getLaunchTime();
        this.finishedTime = app.getFinishTime();
        this.elapsedTime =
            Times.elapsed(app.getStartTime(), app.getFinishTime());
        // 设置日志聚合状态
        this.logAggregationStatus = app.getLogAggregationStatusForAppReport();
        RMAppAttempt attempt = app.getCurrentAppAttempt();
        // 获取当前应用尝试信息
        if (attempt != null) {
          Container masterContainer = attempt.getMasterContainer();
          // 填充AM容器相关信息
          if (masterContainer != null) {
            this.amContainerLogsExist = true;
            this.amContainerLogs = WebAppUtils.getRunningLogURL(
                schemePrefix + masterContainer.getNodeHttpAddress(),
                masterContainer.getId().toString(), app.getUser());
            this.amHostHttpAddress = masterContainer.getNodeHttpAddress();
            this.masterNodeId = masterContainer.getNodeId().toString();
          }

          this.amRPCAddress = getAmRPCAddressFromRMAppAttempt(attempt);

          // 获取资源使用报告填充资源信息
          ApplicationResourceUsageReport resourceReport =
              attempt.getApplicationResourceUsageReport();
          if (resourceReport != null) {
            Resource usedResources = resourceReport.getUsedResources();
            Resource reservedResources = resourceReport.getReservedResources();
            allocatedMB = usedResources.getMemorySize();
            allocatedVCores = usedResources.getVirtualCores();
            reservedMB = reservedResources.getMemorySize();
            reservedVCores = reservedResources.getVirtualCores();
            runningContainers = resourceReport.getNumUsedContainers();
            queueUsagePercentage = resourceReport.getQueueUsagePercentage();
            clusterUsagePercentage = resourceReport.getClusterUsagePercentage();
          }

          /*
           * When the deSelects parameter contains "resourceRequests", it skips
           * returning massive ResourceRequest objects and vice versa. Default
           * behavior is no skipping. (YARN-6280)
           */
          // 未配置反选资源请求，加载待处理资源请求
          if (!deSelects.contains(DeSelectType.RESOURCE_REQUESTS)) {
            List<ResourceRequest> resourceRequestsRaw = rm.getRMContext()
                .getScheduler().getPendingResourceRequestsForAttempt(
                    attempt.getAppAttemptId());

            if (resourceRequestsRaw != null) {
              for (ResourceRequest req : resourceRequestsRaw) {
                resourceRequests.add(new ResourceRequestInfo(req));
              }
            }

            // 加载待处理调度请求
            List<SchedulingRequest> schedulingRequestsRaw = rm.getRMContext()
                .getScheduler().getPendingSchedulingRequestsForAttempt(
                    attempt.getAppAttemptId());
            if (schedulingRequestsRaw != null) {
              for (SchedulingRequest req : schedulingRequestsRaw) {
                resourceRequests.add(new ResourceRequestInfo(req));
              }
            }
          }
        }
      }

      // 复制抢占统计信息
      RMAppMetrics appMetrics = app.getRMAppMetrics();
      numAMContainerPreempted = appMetrics.getNumAMContainersPreempted();
      preemptedResourceMB = appMetrics.getResourcePreempted().getMemorySize();
      numNonAMContainerPreempted = appMetrics.getNumNonAMContainersPreempted();
      preemptedResourceVCores =
          appMetrics.getResourcePreempted().getVirtualCores();
      memorySeconds = appMetrics.getMemorySeconds();
      vcoreSeconds = appMetrics.getVcoreSeconds();
      resourceSecondsMap = appMetrics.getResourceSecondsMap();
      preemptedMemorySeconds = appMetrics.getPreemptedMemorySeconds();
      preemptedVcoreSeconds = appMetrics.getPreemptedVcoreSeconds();
      preemptedResourceSecondsMap = appMetrics.getPreemptedResourceSecondsMap();
      // 获取应用提交上下文
      ApplicationSubmissionContext appSubmissionContext =
          app.getApplicationSubmissionContext();
      unmanagedApplication = appSubmissionContext.getUnmanagedAM();
      appNodeLabelExpression =
          app.getApplicationSubmissionContext().getNodeLabelExpression();
      /*
       * When the deSelects parameter contains "amNodeLabelExpression", objects
       * pertaining to the amNodeLabelExpression are not returned. By default,
       * this is not skipped. (YARN-6871)
       */
      // 未反选AM节点标签表达式，填充该字段
      if(!deSelects.contains(DeSelectType.AM_NODE_LABEL_EXPRESSION)) {
        amNodeLabelExpression = (unmanagedApplication) ?
            null :
            app.getAMResourceRequests().get(0).getNodeLabelExpression();
      }
      /*
       * When the deSelects parameter contains "appNodeLabelExpression", objects
       * pertaining to the appNodeLabelExpression are not returned. By default,
       * this is not skipped. (YARN-6871)
       */
      // 未反选应用节点标签表达式，填充该字段
      if (!deSelects.contains(DeSelectType.APP_NODE_LABEL_EXPRESSION)) {
        appNodeLabelExpression =
            app.getApplicationSubmissionContext().getNodeLabelExpression();
      }
      /*
       * When the deSelects parameter contains "amNodeLabelExpression", objects
       * pertaining to the amNodeLabelExpression are not returned. By default,
       * this is not skipped. (YARN-6871)
       */
      // 再次确认未反选AM节点标签表达式，重新填充避免覆盖
      if (!deSelects.contains(DeSelectType.AM_NODE_LABEL_EXPRESSION)) {
        amNodeLabelExpression = (unmanagedApplication) ?
            null :
            app.getAMResourceRequests().get(0).getNodeLabelExpression();
      }

      /*
       * When the deSelects parameter contains "resourceInfo", ResourceInfo
       * objects are not returned. Default behavior is no skipping. (YARN-6871)
       */
      // Setting partition based resource usage of application
      // 未反选资源信息，获取容量调度器分区资源使用情况
      if (!deSelects.contains(DeSelectType.RESOURCE_INFO)) {
        ResourceScheduler scheduler = rm.getRMContext().getScheduler();
        if (scheduler instanceof CapacityScheduler) {
          RMAppAttempt attempt = app.getCurrentAppAttempt();
          if (null != attempt) {
            FiCaSchedulerApp ficaAppAttempt = ((CapacityScheduler) scheduler)
                .getApplicationAttempt(attempt.getAppAttemptId());
            resourceInfo = null != ficaAppAttempt ?
                new ResourcesInfo(ficaAppAttempt.getSchedulingResourceUsage()) :
                null;
          }
        }
      }

      /*
       * When the deSelects parameter contains "appTimeouts", objects pertaining
       * to app timeouts are not returned. By default, this is not skipped.
       * (YARN-6871)
       */
      // 未反选超时信息，填充应用超时配置
      if (!deSelects.contains(DeSelectType.TIMEOUTS)) {
        Map<ApplicationTimeoutType, Long> applicationTimeouts =
            app.getApplicationTimeouts();
        timeouts = new AppTimeoutsInfo();
        if (applicationTimeouts.isEmpty()) {
          // 未设置超时，默认添加无限生命周期超时配置
          AppTimeoutInfo timeoutInfo = new AppTimeoutInfo();
          timeoutInfo.setTimeoutType(ApplicationTimeoutType.LIFETIME);
          timeouts.add(timeoutInfo);
        } else {
          // 遍历所有超时类型构建超时信息
          for (Map.Entry<ApplicationTimeoutType, Long> entry : app
              .getApplicationTimeouts().entrySet()) {
            AppTimeoutInfo timeout = new AppTimeoutInfo();
            timeout.setTimeoutType(entry.getKey());
            long timeoutInMillis = entry.getValue();
            timeout.setExpiryTime(Times.formatISO8601(timeoutInMillis));
            // 应用已完成，剩余时间设为0
            if (app.isAppInCompletedStates()) {
              timeout.setRemainingTime(0);
            } else {
              // 计算剩余秒数，最小为0
              timeout.setRemainingTime(Math.max(
                  (timeoutInMillis - System.currentTimeMillis()) /