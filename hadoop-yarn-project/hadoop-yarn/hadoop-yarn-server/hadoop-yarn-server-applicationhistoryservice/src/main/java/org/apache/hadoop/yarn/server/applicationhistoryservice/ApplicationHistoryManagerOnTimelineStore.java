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
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Timeline Store 的应用历史管理器实现。
 * 
 * 该类从 Timeline 服务读取应用、应用尝试和容器的历史数据，
 * 并将其转换为 YARN API 标准的报告格式。支持 ACL 访问控制。
 */
public class ApplicationHistoryManagerOnTimelineStore extends AbstractService
    implements
    ApplicationHistoryManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(ApplicationHistoryManagerOnTimelineStore.class);

  @VisibleForTesting
  static final String UNAVAILABLE = "N/A";

  // Timeline 数据管理器，用于读取历史数据
  private TimelineDataManager timelineDataManager;
  // ACL 管理器，用于访问控制检查
  private ApplicationACLsManager aclsManager;
  // 服务器 HTTP 地址，用于构建日志 URL
  private String serverHttpAddress;
  // 最大加载应用数
  private long maxLoadedApplications;

  public ApplicationHistoryManagerOnTimelineStore(
      TimelineDataManager timelineDataManager,
      ApplicationACLsManager aclsManager) {
    super(ApplicationHistoryManagerOnTimelineStore.class.getName());
    this.timelineDataManager = timelineDataManager;
    this.aclsManager = aclsManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    serverHttpAddress = WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
    maxLoadedApplications =
        conf.getLong(YarnConfiguration.APPLICATION_HISTORY_MAX_APPS,
          YarnConfiguration.DEFAULT_APPLICATION_HISTORY_MAX_APPS);
    super.serviceInit(conf);
  }

  /**
   * 获取指定应用的报告
   */
  @Override
  public ApplicationReport getApplication(ApplicationId appId)
      throws YarnException, IOException {
    return getApplication(appId, ApplicationReportField.ALL).appReport;
  }

  /**
   * 获取指定时间范围内的应用列表
   */
  @Override
  public Map<ApplicationId, ApplicationReport> getApplications(long appsNum,
      long appStartedTimeBegin, long appStartedTimeEnd) throws YarnException,
      IOException {
    // 从 Timeline 服务查询应用实体
    TimelineEntities entities =
        timelineDataManager.getEntities(
          ApplicationMetricsConstants.ENTITY_TYPE, null, null,
          appStartedTimeBegin, appStartedTimeEnd, null, null,
          appsNum == Long.MAX_VALUE ? this.maxLoadedApplications : appsNum,
          EnumSet.allOf(Field.class), UserGroupInformation.getLoginUser());
    Map<ApplicationId, ApplicationReport> apps =
        new LinkedHashMap<ApplicationId, ApplicationReport>();
    if (entities != null && entities.getEntities() != null) {
      for (TimelineEntity entity : entities.getEntities()) {
        try {
          ApplicationReportExt app =
              generateApplicationReport(entity, ApplicationReportField.ALL);
          apps.put(app.appReport.getApplicationId(), app.appReport);
        } catch (Exception e) {
          LOG.error("Error on generating application report for " +
              entity.getEntityId(), e);
        }
      }
    }
    return apps;
  }

  /**
   * 获取指定应用的所有尝试列表
   */
  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptReport>
      getApplicationAttempts(ApplicationId appId)
          throws YarnException, IOException {
    // 先检查访问权限
    ApplicationReportExt app = getApplication(
        appId, ApplicationReportField.USER_AND_ACLS);
    checkAccess(app);
    // 从 Timeline 服务查询应用尝试实体
    TimelineEntities entities = timelineDataManager.getEntities(
        AppAttemptMetricsConstants.ENTITY_TYPE,
        new NameValuePair(
            AppAttemptMetricsConstants.PARENT_PRIMARY_FILTER, appId
                .toString()), null, null, null, null, null,
        Long.MAX_VALUE, EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    Map<ApplicationAttemptId, ApplicationAttemptReport> appAttempts =
        new LinkedHashMap<ApplicationAttemptId, ApplicationAttemptReport>();
    for (TimelineEntity entity : entities.getEntities()) {
      ApplicationAttemptReport appAttempt =
          convertToApplicationAttemptReport(entity);
      appAttempts.put(appAttempt.getApplicationAttemptId(), appAttempt);
    }
    return appAttempts;
  }

  /**
   * 获取指定应用尝试的报告
   */
  @Override
  public ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    return getApplicationAttempt(appAttemptId, true);
  }

  /**
   * 获取应用尝试报告，可选是否检查 ACL
   */
  private ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId, boolean checkACLs)
      throws YarnException, IOException {
    if (checkACLs) {
      ApplicationReportExt app = getApplication(
          appAttemptId.getApplicationId(),
          ApplicationReportField.USER_AND_ACLS);
      checkAccess(app);
    }
    TimelineEntity entity = timelineDataManager.getEntity(
        AppAttemptMetricsConstants.ENTITY_TYPE,
        appAttemptId.toString(), EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    if (entity == null) {
      throw new ApplicationAttemptNotFoundException(
          "The entity for application attempt " + appAttemptId +
          " doesn't exist in the timeline store");
    } else {
      return convertToApplicationAttemptReport(entity);
    }
  }

  /**
   * 获取指定容器的报告
   */
  @Override
  public ContainerReport getContainer(ContainerId containerId)
      throws YarnException, IOException {
    ApplicationReportExt app = getApplication(
        containerId.getApplicationAttemptId().getApplicationId(),
        ApplicationReportField.USER_AND_ACLS);
    checkAccess(app);
    TimelineEntity entity = timelineDataManager.getEntity(
        ContainerMetricsConstants.ENTITY_TYPE,
        containerId.toString(), EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    if (entity == null) {
      throw new ContainerNotFoundException(
          "The entity for container " + containerId +
          " doesn't exist in the timeline store");
    } else {
      return convertToContainerReport(
          entity, serverHttpAddress, app.appReport.getUser());
    }
  }

  /**
   * 获取指定应用尝试的 AM 容器报告
   */
  @Override
  public ContainerReport getAMContainer(ApplicationAttemptId appAttemptId)
      throws YarnException, IOException {
    ApplicationAttemptReport appAttempt =
        getApplicationAttempt(appAttemptId, false);
    return getContainer(appAttempt.getAMContainerId());
  }

  /**
   * 获取指定应用尝试的所有容器列表
   */
  @Override
  public Map<ContainerId, ContainerReport> getContainers(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    ApplicationReportExt app = getApplication(
        appAttemptId.getApplicationId(), ApplicationReportField.USER_AND_ACLS);
    checkAccess(app);
    // 从 Timeline 服务查询容器实体
    TimelineEntities entities = timelineDataManager.getEntities(
        ContainerMetricsConstants.ENTITY_TYPE,
        new NameValuePair(
            ContainerMetricsConstants.PARENT_PRIMARIY_FILTER,
            appAttemptId.toString()), null, null, null,
        null, null, Long.MAX_VALUE, EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    Map<ContainerId, ContainerReport> containers =
        new LinkedHashMap<ContainerId, ContainerReport>();
    if (entities != null && entities.getEntities() != null) {
      for (TimelineEntity entity : entities.getEntities()) {
        ContainerReport container = convertToContainerReport(
            entity, serverHttpAddress, app.appReport.getUser());
        containers.put(container.getContainerId(), container);
      }
    }
    return containers;
  }

  /**
   * 将 Timeline 实体转换为应用报告
   */
  private static ApplicationReportExt convertToApplicationReport(
      TimelineEntity entity, ApplicationReportField field) {
    String user = null;
    String queue = null;
    String name = null;
    String type = null;
    boolean unmanagedApplication = false;
    long createdTime = 0;
    long launchTime = 0;
    long submittedTime = 0;
    long finishedTime = 0;
    float progress = 0.0f;
    int applicationPriority = 0;
    ApplicationAttemptId latestApplicationAttemptId = null;
    String diagnosticsInfo = null;
    FinalApplicationStatus finalStatus = FinalApplicationStatus.UNDEFINED;
    YarnApplicationState state = YarnApplicationState.ACCEPTED;
    ApplicationResourceUsageReport appResources = null;
    Set<String> appTags = null;
    Map<ApplicationAccessType, String> appViewACLs =
        new HashMap<ApplicationAccessType, String>();
    String appNodeLabelExpression = null;
    String amNodeLabelExpression = null;
    
    // 从实体信息中提取应用元数据
    Map<String, Object> entityInfo = entity.getOtherInfo();
    if (entityInfo != null) {
      if (entityInfo.containsKey(ApplicationMetricsConstants.USER_ENTITY_INFO)) {
        user =
            entityInfo.get(ApplicationMetricsConstants.USER_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO)) {
        String appViewACLsStr = entityInfo.get(
            ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO).toString();
        if (appViewACLsStr.length() > 0) {
          appViewACLs.put(ApplicationAccessType.VIEW_APP, appViewACLsStr);
        }
      }
      // 如果只需要用户和 ACL 信息，提前返回
      if (field == ApplicationReportField.USER_AND_ACLS) {
        return new ApplicationReportExt(ApplicationReport.newInstance(
            ApplicationId.fromString(entity.getEntityId()),
            latestApplicationAttemptId, user, queue, name, null, -1, null,
            state, diagnosticsInfo, null, createdTime, submittedTime, 0,
            finishedTime, finalStatus, null, null, progress, type, null,
            appTags, unmanagedApplication, Priority.newInstance(
            applicationPriority), appNodeLabelExpression,
            amNodeLabelExpression), appViewACLs);
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)) {
        queue =
            entityInfo.get(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.NAME_ENTITY_INFO)) {
        name =
            entityInfo.get(ApplicationMetricsConstants.NAME_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.TYPE_ENTITY_INFO)) {
        type =
            entityInfo.get(ApplicationMetricsConstants.TYPE_ENTITY_INFO)
                .toString();
      }
      if (entityInfo.containsKey(ApplicationMetricsConstants.TYPE_ENTITY_INFO)) {
        type =
            entityInfo.get(ApplicationMetricsConstants.TYPE_ENTITY_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO)) {
        unmanagedApplication =
            Boolean.parseBoolean(entityInfo.get(
                ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO)
                .toString());
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO)) {
        applicationPriority = Integer.parseInt(entityInfo.get(
            ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO).toString());
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION)) {
        appNodeLabelExpression = entityInfo
            .get(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION).toString();
      }
      if (entityInfo
          .containsKey(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION)) {
        amNodeLabelExpression =
            entityInfo.get(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION)
                .toString();
      }
      submittedTime = parseLong(entityInfo,
          ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO);

      // 解析资源使用指标
      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_CPU_METRICS)) {
        long vcoreSeconds = parseLong(entityInfo,
            ApplicationMetricsConstants.APP_CPU_METRICS);
        long memorySeconds = parseLong(entityInfo,
            ApplicationMetricsConstants.APP_MEM_METRICS);
        long preemptedMemorySeconds = parseLong(entityInfo,
            ApplicationMetricsConstants.APP_MEM_PREEMPT_METRICS);
        long preemptedVcoreSeconds = parseLong(entityInfo,
            ApplicationMetricsConstants.APP_CPU_PREEMPT_METRICS);
        Map<String, Long> resourceSecondsMap = new HashMap<>();
        Map<String, Long> preemptedResourceSecondsMap = new HashMap<>();
        resourceSecondsMap
            .put(ResourceInformation.MEMORY_MB.getName(), memorySeconds);
        resourceSecondsMap
            .put(ResourceInformation.VCORES.getName(), vcoreSeconds);
        preemptedResourceSecondsMap.put(ResourceInformation.MEMORY_MB.getName(),
            preemptedMemorySeconds);
        preemptedResourceSecondsMap
            .put(ResourceInformation.VCORES.getName(), preemptedVcoreSeconds);

        appResources = ApplicationResourceUsageReport
            .newInstance(0, 0, null, null, null, resourceSecondsMap, 0, 0,
                preemptedResourceSecondsMap);
      }

      // 解析应用标签
      if (entityInfo.containsKey(ApplicationMetricsConstants.APP_TAGS_INFO)) {
        appTags = new HashSet<String>();
        Object obj = entityInfo.get(ApplicationMetricsConstants.APP_TAGS_INFO);
        if (obj != null && obj instanceof Collection<?>) {
          for(Object o : (Collection<?>)obj) {
            if (o != null) {
              appTags.add(o.toString());
            }
          }
        }
      }
    }
    
    // 解析事件信息
    List<TimelineEvent> events = entity.getEvents();
    long updatedTimeStamp = 0L;
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getEventType().equals(
            ApplicationMetricsConstants.CREATED_EVENT_TYPE)) {
          createdTime = event.getTimestamp();
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.LAUNCHED_EVENT_TYPE)) {
          launchTime = event.getTimestamp();
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.UPDATED_EVENT_TYPE)) {
          // UPDATE 事件按时间戳降序解析，需要比较时间戳避免旧数据覆盖新数据
          if (event.getTimestamp() > updatedTimeStamp) {
            updatedTimeStamp = event.getTimestamp();
          } else {
            continue;
          }

          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          applicationPriority = Integer
              .parseInt(eventInfo.get(
                  ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO)
                  .toString());
          queue = eventInfo.get(ApplicationMetricsConstants.QUEUE_ENTITY_INFO)
              .toString();
        } else if (event.getEventType().equals(
              ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE)) {
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo.containsKey(
              ApplicationMetricsConstants.STATE_EVENT_INFO)) {
            if (!Apps.isApplicationFinalState(state)) {
              state = YarnApplicationState.valueOf(eventInfo.get(
                  ApplicationMetricsConstants.STATE_EVENT_INFO).toString());
            }
          }
        } else if (event.getEventType().equals(
            ApplicationMetricsConstants.FINISHED_EVENT_TYPE)) {
          progress=1.0F;
          finishedTime = event.getTimestamp();
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO)) {
            latestApplicationAttemptId = ApplicationAttemptId.fromString(
                eventInfo.get(
                    ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO)
                    .toString());
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)) {
            diagnosticsInfo =
                eventInfo.get(
                    ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO)) {
            finalStatus =
                FinalApplicationStatus.valueOf(eventInfo.get(
                    ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO)
                    .toString());
          }
          if (eventInfo
              .containsKey(ApplicationMetricsConstants.STATE_EVENT_INFO)) {
            state =
                YarnApplicationState.valueOf(eventInfo.get(
                    ApplicationMetricsConstants.STATE_EVENT_INFO).toString());
          }
        }
      }
    }
    return new ApplicationReportExt(ApplicationReport.newInstance(
        ApplicationId.fromString(entity.getEntityId()),
        latestApplicationAttemptId, user, queue, name, null, -1, null, state,
        diagnosticsInfo, null, createdTime,
        submittedTime, launchTime, finishedTime,
        finalStatus, appResources, null, progress, type, null, appTags,
        unmanagedApplication, Priority.newInstance(applicationPriority),
        appNodeLabelExpression, amNodeLabelExpression), appViewACLs);
  }

  /**
   * 从实体信息中解析长整型值
   */
  private static long parseLong(Map<String, Object> entityInfo,
      String infoKey) {
    long result = 0;
    Object infoValue = entityInfo.get(infoKey);
    if (infoValue != null) {
      result = Long.parseLong(infoValue.toString());
    }
    return result;
  }

  /**
   * 将 Timeline 实体转换为应用尝试报告
   */
  private static ApplicationAttemptReport convertToApplicationAttemptReport(
      TimelineEntity entity) {
    String host = null;
    int rpcPort = -1;
    ContainerId amContainerId = null;
    String trackingUrl = null;
    String originalTrackingUrl = null;
    String diagnosticsInfo = null;
    YarnApplicationAttemptState state = null;
    List<TimelineEvent> events = entity.getEvents();
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getEventType().equals(
            AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE)) {
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo.containsKey(AppAttemptMetricsConstants.HOST_INFO)) {
            host =
                eventInfo.get(AppAttemptMetricsConstants.HOST_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.RPC_PORT_INFO)) {
            rpcPort = (Integer) eventInfo.get(
                    AppAttemptMetricsConstants.RPC_PORT_INFO);
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)) {
            amContainerId =
                ContainerId.fromString(eventInfo.get(
                    AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)
                    .toString());
          }
        } else if (event.getEventType().equals(
            AppAttemptMetricsConstants.FINISHED_EVENT_TYPE)) {
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.TRACKING_URL_INFO)) {
            trackingUrl =
                eventInfo.get(
                    AppAttemptMetricsConstants.TRACKING_URL_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(
                  AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO)) {
            originalTrackingUrl =
                eventInfo
                    .get(
                        AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.DIAGNOSTICS_INFO)) {
            diagnosticsInfo =
                eventInfo.get(
                    AppAttemptMetricsConstants.DIAGNOSTICS_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.STATE_INFO)) {
            state =
                YarnApplicationAttemptState.valueOf(eventInfo.get(
                    AppAttemptMetricsConstants.STATE_INFO)
                    .toString());
          }
          if (eventInfo
              .containsKey(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)) {
            amContainerId =
                ContainerId.fromString(eventInfo.get(
                    AppAttemptMetricsConstants.MASTER_CONTAINER_INFO)
                    .toString());
          }
        }
      }
    }
    return ApplicationAttemptReport.newInstance(
        ApplicationAttemptId.fromString(entity.getEntityId()),
        host, rpcPort, trackingUrl, originalTrackingUrl, diagnosticsInfo,
        state, amContainerId);
  }

  /**
   * 将 Timeline 实体转换为容器报告
   */
  private static ContainerReport convertToContainerReport(
      TimelineEntity entity, String serverHttpAddress, String user) {
    int allocatedMem = 0;
    int allocatedVcore = 0;
    String allocatedHost = null;
    int allocatedPort = -1;
    int allocatedPriority = 0;
    long createdTime = 0;
    long finishedTime = 0;
    String diagnosticsInfo = null;
    int exitStatus = ContainerExitStatus.INVALID;
    ContainerState state = null;
    String nodeHttpAddress = null;
    Map<String, List<Map<String, String>>> exposedPorts = null;

    // 从实体信息中提取容器分配信息
    Map<String, Object> entityInfo = entity.getOtherInfo();
    if (entityInfo != null) {
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO)) {
        allocatedMem = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_MEMORY_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_VCORE_INFO)) {
        allocatedVcore = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_VCORE_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_HOST_INFO)) {
        allocatedHost =
            entityInfo
                .get(ContainerMetricsConstants.ALLOCATED_HOST_INFO)
                .toString();
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_PORT_INFO)) {
        allocatedPort = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_PORT_INFO);
      }
      if (entityInfo
          .containsKey(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO)) {
        allocatedPriority = (Integer) entityInfo.get(
                ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO);
      }
      if (entityInfo.containsKey(
          ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO)) {
        nodeHttpAddress =
            (String) entityInfo
              .get(ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO);
      }
      if (entityInfo.containsKey(
          ContainerMetricsConstants.ALLOCATED_EXPOSED_PORTS)) {
        exposedPorts =
            (Map<String, List<Map<String, String>>>) entityInfo
                .get(ContainerMetricsConstants.ALLOCATED_EXPOSED_PORTS);
      }
    }
    
    // 解析容器生命周期事件
    List<TimelineEvent> events = entity.getEvents();
    if (events != null) {
      for (TimelineEvent event : events) {
        if (event.getEventType().equals(
            ContainerMetricsConstants.CREATED_EVENT_TYPE)) {
          createdTime = event.getTimestamp();
        } else if (event.getEventType().equals(
            ContainerMetricsConstants.FINISHED_EVENT_TYPE)) {
          finishedTime = event.getTimestamp();
          Map<String, Object> eventInfo = event.getEventInfo();
          if (eventInfo == null) {
            continue;
          }
          if (eventInfo
              .containsKey(ContainerMetricsConstants.DIAGNOSTICS_INFO)) {
            diagnosticsInfo =
                eventInfo.get(
                    ContainerMetricsConstants.DIAGNOSTICS_INFO)
                    .toString();
          }
          if (eventInfo
              .containsKey(ContainerMetricsConstants.EXIT_STATUS_INFO)) {
            exitStatus = (Integer) eventInfo.get(
                    ContainerMetricsConstants.EXIT_STATUS_INFO);
          }
          if (eventInfo
              .containsKey(ContainerMetricsConstants.STATE_INFO)) {
            state =
                ContainerState.valueOf(eventInfo.get(
                    ContainerMetricsConstants.STATE_INFO).toString());
          }
        }
      }
    }
    ContainerId containerId =
        ContainerId.fromString(entity.getEntityId());
    String logUrl = null;
    NodeId allocatedNode = null;
    // 生成聚合日志 URL
    if (allocatedHost != null) {
      allocatedNode = NodeId.newInstance(allocatedHost, allocatedPort);
      logUrl = WebAppUtils.getAggregatedLogURL(
          serverHttpAddress,
          allocatedNode.toString(),
          containerId.toString(),
          containerId.toString(),
          user);
    }
    ContainerReport container = ContainerReport.newInstance(
        ContainerId.fromString(entity.getEntityId()),
        Resource.newInstance(allocatedMem, allocatedVcore), allocatedNode,
        Priority.newInstance(allocatedPriority),
        createdTime, finishedTime, diagnosticsInfo, logUrl, exitStatus, state,
        nodeHttpAddress);
    container.setExposedPorts(exposedPorts);

    return container;
  }

  /**
   * 生成应用报告，包含完整的尝试信息
   */
  private ApplicationReportExt generateApplicationReport(TimelineEntity entity,
      ApplicationReportField field) throws YarnException, IOException {
    ApplicationReportExt app = convertToApplicationReport(entity, field);
    // 如果只是为了检查 ACL 获取用户信息，直接返回
    if (field == ApplicationReportField.USER_AND_ACLS) {
      return app;
    }
    try {
      checkAccess(app);
      // 补充最新尝试的运行时信息
      if (app.appReport.getCurrentApplicationAttemptId() != null) {
        ApplicationAttemptReport appAttempt = getApplicationAttempt(
            app.appReport.getCurrentApplicationAttemptId(), false);
        app.appReport.setHost(appAttempt.getHost());
        app.appReport.setRpcPort(appAttempt.getRpcPort());
        app.appReport.setTrackingUrl(appAttempt.getTrackingUrl());
        app.appReport.setOriginalTrackingUrl(appAttempt.getOriginalTrackingUrl());
      }
    } catch (AuthorizationException | ApplicationAttemptNotFoundException e) {
      // 无权限或尝试不存在时的处理
      if (e instanceof AuthorizationException) {
        LOG.warn("Failed to authorize when generating application report for "
            + app.appReport.getApplicationId()
            + ". Use a placeholder for its latest attempt id. ", e);
      } else {
        LOG.info("No application attempt found for "
            + app.appReport.getApplicationId()
            + ". Use a placeholder for its latest attempt id. ", e);
      }
      // 应用可能在第一次尝试创建前就结束了，使用占位符
      app.appReport.setDiagnostics(null);
      app.appReport.setCurrentApplicationAttemptId(null);
    }
    // 设置默认值避免空指针
    if (app.appReport.getCurrentApplicationAttemptId() == null) {
      app.appReport.setCurrentApplicationAttemptId(
          ApplicationAttemptId.newInstance(app.appReport.getApplicationId(), -1));
    }
    if (app.appReport.getHost() == null) {
      app.appReport.setHost(UNAVAILABLE);
    }
    if (app.appReport.getRpcPort() < 0) {
      app.appReport.setRpcPort(-1);
    }
    if (app.appReport.getTrackingUrl() == null) {
      app.appReport.setTrackingUrl(UNAVAILABLE);
    }
    if (app.appReport.getOriginalTrackingUrl() == null) {
      app.appReport.setOriginalTrackingUrl(UNAVAILABLE);
    }
    if (app.appReport.getDiagnostics() == null) {
      app.appReport.setDiagnostics("");
    }
    return app;
  }

  /**
   * 从 Timeline 服务获取应用实体并生成报告
   */
  private ApplicationReportExt getApplication(ApplicationId appId,
      ApplicationReportField field) throws YarnException, IOException {
    TimelineEntity entity = timelineDataManager.getEntity(
        ApplicationMetricsConstants.ENTITY_TYPE,
        appId.toString(), EnumSet.allOf(Field.class),
        UserGroupInformation.getLoginUser());
    if (entity == null) {
      throw new ApplicationNotFoundException("The entity for application " +
          appId + " doesn't exist in the timeline store");
    } else {
      return generateApplicationReport(entity, field);
    }
  }

  /**
   * 检查当前用户是否有权限访问该应用
   */
   private void checkAccess(ApplicationReportExt app)
           throws YarnException, IOException {
     if (app.appViewACLs != null) {
       aclsManager.addApplication(
           app.appReport.getApplicationId(), app.appViewACLs);
       try {
         if (!aclsManager.checkAccess(UserGroupInformation.getCurrentUser(),
             ApplicationAccessType.VIEW_APP, app.appReport.getUser(),
             app.appReport.getApplicationId())) {
           throw new AuthorizationException("User "
               + UserGroupInformation.getCurrentUser().getShortUserName()
               + " does not have privilege to see this application "
               + app.appReport.getApplicationId());
         }
       } finally {
         aclsManager.removeApplication(app.appReport.getApplicationId());
       }
     }
   }

  /**
   * 应用报告字段选择枚举
   */
  private enum ApplicationReportField {
    ALL, // 获取所有字段
    USER_AND_ACLS // 仅获取用户和 ACL 信息
  }

  /**
   * 应用报告扩展类，包含 ACL 信息
   */
  private static class ApplicationReportExt {
     private ApplicationReport appReport;
     private Map<ApplicationAccessType, String> appViewACLs;

     public ApplicationReportExt(
         ApplicationReport appReport,
         Map<ApplicationAccessType, String> appViewACLs) {
       this.appReport = appReport;
       this.appViewACLs = appViewACLs;
     }
   }
}
