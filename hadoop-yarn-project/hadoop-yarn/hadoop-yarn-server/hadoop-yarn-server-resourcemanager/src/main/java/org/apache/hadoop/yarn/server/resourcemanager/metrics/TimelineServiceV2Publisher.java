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

package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationAttemptEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity.Identifier;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollector;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 负责向Timeline Service V2发布应用、应用尝试和容器的生命周期相关事件，为YARN集群提供应用运行指标采集能力。
 */
@Private
@Unstable
public class TimelineServiceV2Publisher extends AbstractSystemMetricsPublisher {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineServiceV2Publisher.class);
  // RM端Timeline采集器管理器，用于获取对应应用的采集器实例
  private RMTimelineCollectorManager rmTimelineCollectorManager;
  // 是否发布容器生命周期事件的配置开关
  private boolean publishContainerEvents;

  /**
   * 构造Timeline Service V2指标发布器
   * @param timelineCollectorManager RM端Timeline采集器管理器
   */
  public TimelineServiceV2Publisher(
      RMTimelineCollectorManager timelineCollectorManager) {
    super("TimelineserviceV2Publisher");
    rmTimelineCollectorManager = timelineCollectorManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 注册Timeline V2事件处理器
    getDispatcher().register(SystemMetricsEventType.class,
        new TimelineV2EventHandler());
    // 从配置读取是否开启容器事件发布，使用默认值
    publishContainerEvents = getConfig().getBoolean(
        YarnConfiguration.RM_PUBLISH_CONTAINER_EVENTS_ENABLED,
        YarnConfiguration.DEFAULT_RM_PUBLISH_CONTAINER_EVENTS_ENABLED);
  }

  @VisibleForTesting
  boolean isPublishContainerEvents() {
    return publishContainerEvents;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appCreated(RMApp app, long createdTime) {
    // 创建应用实体对象
    ApplicationEntity entity = createApplicationEntity(app.getApplicationId());
    entity.setQueue(app.getQueue());
    entity.setCreatedTime(createdTime);

    // 填充应用基础信息
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.NAME_ENTITY_INFO, app.getName());
    entityInfo.put(ApplicationMetricsConstants.TYPE_ENTITY_INFO,
        app.getApplicationType());
    entityInfo.put(ApplicationMetricsConstants.USER_ENTITY_INFO, app.getUser());
    entityInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO,
        app.getQueue());
    entityInfo.put(ApplicationMetricsConstants.SUBMITTED_TIME_ENTITY_INFO,
        app.getSubmitTime());
    entityInfo.put(ApplicationMetricsConstants.APP_TAGS_INFO,
        app.getApplicationTags());
    entityInfo.put(
        ApplicationMetricsConstants.UNMANAGED_APPLICATION_ENTITY_INFO,
        app.getApplicationSubmissionContext().getUnmanagedAM());
    entityInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO,
        app.getApplicationPriority().getPriority());
    entity.getConfigs().put(
        ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION,
        app.getAmNodeLabelExpression());
    entity.getConfigs().put(
        ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION,
        app.getAppNodeLabelExpression());
    // 填充调用上下文信息（如果存在）
    if (app.getCallerContext() != null) {
      if (app.getCallerContext().isContextValid()) {
        entityInfo.put(ApplicationMetricsConstants.YARN_APP_CALLER_CONTEXT,
            app.getCallerContext().getContext());
      }
      if (app.getCallerContext().getSignature() != null) {
        entityInfo.put(ApplicationMetricsConstants.YARN_APP_CALLER_SIGNATURE,
            app.getCallerContext().getSignature());
      }
    }
    ContainerLaunchContext amContainerSpec =
        app.getApplicationSubmissionContext().getAMContainerSpec();
    entityInfo.put(ApplicationMetricsConstants.AM_CONTAINER_LAUNCH_COMMAND,
        amContainerSpec.getCommands());
    entityInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        RMServerUtils.createApplicationState(app.getState()).toString());

    entity.setInfo(entityInfo);
    // 创建应用创建事件并添加到实体
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(createdTime);
    entity.addEvent(tEvent);

    // 发布事件到事件Dispatcher
    getDispatcher().getEventHandler().handle(new TimelineV2PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appLaunched(RMApp app, long launchTime) {
    ApplicationEntity entity =
        createApplicationEntity(app.getApplicationId());
    // 创建应用启动事件
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ApplicationMetricsConstants.LAUNCHED_EVENT_TYPE);
    tEvent.setTimestamp(launchTime);
    entity.addEvent(tEvent);

    // 发布事件
    getDispatcher().getEventHandler().handle(new TimelineV2PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appFinished(RMApp app, RMAppState state, long finishedTime) {
    ApplicationEntity entity = createApplicationEntity(app.getApplicationId());

    // 创建应用完成事件
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(finishedTime);
    entity.addEvent(tEvent);

    // 填充应用完成相关信息
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        app.getDiagnostics().toString());
    entityInfo.put(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO,
        app.getFinalApplicationStatus().toString());
    entityInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        RMServerUtils.createApplicationState(state).toString());
    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt() == null
        ? null : app.getCurrentAppAttempt().getAppAttemptId();
    if (appAttemptId != null) {
      entityInfo.put(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO,
          appAttemptId.toString());
    }
    entity.setInfo(entityInfo);

    // 填充应用运行指标
    RMAppMetrics appMetrics = app.getRMAppMetrics();
    Set<TimelineMetric> entityMetrics =
        getTimelinelineAppMetrics(appMetrics, finishedTime);
    entity.setMetrics(entityMetrics);

    // 发布应用完成事件，发布完成后会停止采集器
    getDispatcher().getEventHandler().handle(
        new ApplicationFinishPublishEvent(SystemMetricsEventType.
            PUBLISH_APPLICATION_FINISHED_ENTITY, entity, app));
  }

  /**
   * 将RM应用指标转换为Timeline Service指标集合
   * @param appMetrics RM应用指标对象
   * @param timestamp 指标时间戳
   * @return 转换后的Timeline指标集合
   */
  private Set<TimelineMetric> getTimelinelineAppMetrics(
      RMAppMetrics appMetrics, long timestamp) {
    Set<TimelineMetric> entityMetrics = new HashSet<TimelineMetric>();

    entityMetrics.add(getTimelineMetric(
        ApplicationMetricsConstants.APP_CPU_METRICS, timestamp,
        appMetrics.getVcoreSeconds()));
    entityMetrics.add(getTimelineMetric(
        ApplicationMetricsConstants.APP_MEM_METRICS, timestamp,
        appMetrics.getMemorySeconds()));
    entityMetrics.add(getTimelineMetric(
            ApplicationMetricsConstants.APP_MEM_PREEMPT_METRICS, timestamp,
            appMetrics.getPreemptedMemorySeconds()));
    entityMetrics.add(getTimelineMetric(
            ApplicationMetricsConstants.APP_CPU_PREEMPT_METRICS, timestamp,
            appMetrics.getPreemptedVcoreSeconds()));
    entityMetrics.add(getTimelineMetric(
        ApplicationMetricsConstants.APP_RESOURCE_PREEMPTED_CPU, timestamp,
        appMetrics.getResourcePreempted().getVirtualCores()));
    entityMetrics.add(getTimelineMetric(
        ApplicationMetricsConstants.APP_RESOURCE_PREEMPTED_MEM, timestamp,
        appMetrics.getResourcePreempted().getMemorySize()));
    entityMetrics.add(getTimelineMetric(
        ApplicationMetricsConstants.APP_NON_AM_CONTAINER_PREEMPTED, timestamp,
        appMetrics.getNumNonAMContainersPreempted()));
    entityMetrics.add(getTimelineMetric(
        ApplicationMetricsConstants.APP_AM_CONTAINER_PREEMPTED, timestamp,
        appMetrics.getNumAMContainersPreempted()));

    return entityMetrics;
  }

  /**
   * 创建单个Timeline指标对象
   * @param name 指标名称
   * @param timestamp 指标时间戳
   * @param value 指标值
   * @return 构建好的Timeline指标对象
   */
  private TimelineMetric getTimelineMetric(String name, long timestamp,
      Number value) {
    TimelineMetric metric = new TimelineMetric();
    metric.setId(name);
    metric.addValue(timestamp, value);
    return metric;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appStateUpdated(RMApp app, YarnApplicationState appState,
      long updatedTime) {
    ApplicationEntity entity =
        createApplicationEntity(app.getApplicationId());
    // 创建状态更新事件
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        appState);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(updatedTime);
    tEvent.setInfo(eventInfo);
    entity.addEvent(tEvent);

    // 同时更新实体信息，方便过滤查询
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO, appState);
    entity.setInfo(entityInfo);

    // 发布更新事件
    getDispatcher().getEventHandler().handle(new TimelineV2PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appACLsUpdated(RMApp app, String appViewACLs, long updatedTime) {
    ApplicationEntity entity = createApplicationEntity(app.getApplicationId());
    TimelineEvent tEvent = new TimelineEvent();
    // 更新ACL信息到实体
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO,
        (appViewACLs == null) ? "" : appViewACLs);
    entity.setInfo(entityInfo);
    // 创建ACL更新事件
    tEvent.setId(ApplicationMetricsConstants.ACLS_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(updatedTime);
    entity.addEvent(tEvent);

    // 发布更新事件
    getDispatcher().getEventHandler().handle(new TimelineV2PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appUpdated(RMApp app, long currentTimeMillis) {
    ApplicationEntity entity = createApplicationEntity(app.getApplicationId());
    // 创建应用更新事件，填充更新的队列和优先级信息
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO,
        app.getQueue());
    eventInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO,
        app.getApplicationPriority().getPriority());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(ApplicationMetricsConstants.UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(currentTimeMillis);
    tEvent.setInfo(eventInfo);
    entity.addEvent(tEvent);
    // 发布更新事件
    getDispatcher().getEventHandler().handle(new TimelineV2PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  private static ApplicationEntity createApplicationEntity(
      ApplicationId applicationId) {
    ApplicationEntity entity = new ApplicationEntity();
    entity.setId(applicationId.toString());
    return entity;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appAttemptRegistered(RMAppAttempt appAttempt,
      long registeredTime) {
    ApplicationAttemptId attemptId = appAttempt.getAppAttemptId();
    TimelineEntity entity = createAppAttemptEntity(attemptId);
    entity.setCreatedTime(registeredTime);

    // 创建应用尝试注册事件
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE);
    tEvent.setTimestamp(registeredTime);
    entity.addEvent(tEvent);

    // 填充应用尝试基础信息
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(AppAttemptMetricsConstants.TRACKING_URL_INFO,
        appAttempt.getTrackingUrl());
    entityInfo.put(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO,
        appAttempt.getOriginalTrackingUrl());
    entityInfo.put(AppAttemptMetricsConstants.HOST_INFO,
        appAttempt.getHost());
    entityInfo.put(AppAttemptMetricsConstants.RPC_PORT_INFO,
        appAttempt.getRpcPort());
    if (appAttempt.getMasterContainer() != null) {
      entityInfo.put(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO,
          appAttempt.getMasterContainer().getId().toString());
      entityInfo.put(AppAttemptMetricsConstants.MASTER_NODE_ADDRESS,
          appAttempt.getMasterContainer().getNodeHttpAddress());
      entityInfo.put(AppAttemptMetricsConstants.MASTER_NODE_ID,
          appAttempt.getMasterContainer().getNodeId().toString());
    }
    entity.setInfo(entityInfo);
    entity.setIdPrefix(
        TimelineServiceHelper.invertLong(attemptId.getAttemptId()));

    // 发布注册事件
    getDispatcher().getEventHandler().handle(
        new TimelineV2PublishEvent(SystemMetricsEventType.PUBLISH_ENTITY,
            entity, appAttempt.getAppAttemptId().getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appAttemptFinished(RMAppAttempt appAttempt,
      RMAppAttemptState appAttemtpState, RMApp app,