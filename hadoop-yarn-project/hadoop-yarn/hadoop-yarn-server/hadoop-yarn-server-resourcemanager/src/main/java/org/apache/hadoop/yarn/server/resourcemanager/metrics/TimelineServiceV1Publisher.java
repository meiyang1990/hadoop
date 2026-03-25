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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.metrics.AppAttemptMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * 负责将应用、应用尝试和容器的生命周期相关事件发布到 Timeline Service V1。
 */
public class TimelineServiceV1Publisher extends AbstractSystemMetricsPublisher {

  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineServiceV1Publisher.class);

  public TimelineServiceV1Publisher() {
    super("TimelineserviceV1Publisher");
  }

  private TimelineClient client;
  private LinkedBlockingQueue<TimelineEntity> entityQueue;
  private ExecutorService sendEventThreadPool;
  private int dispatcherPoolSize;
  private int dispatcherBatchSize;
  private int putEventInterval;
  private boolean isTimeLineServerBatchEnabled;
  private volatile boolean stopped = false;
  private PutEventThread putEventThread;
  private Object sendEntityLock;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置读取是否开启批量发布
    isTimeLineServerBatchEnabled =
        conf.getBoolean(
            YarnConfiguration.RM_TIMELINE_SERVER_V1_PUBLISHER_BATCH_ENABLED,
            YarnConfiguration.DEFAULT_RM_TIMELINE_SERVER_V1_PUBLISHER_BATCH_ENABLED);
    if (isTimeLineServerBatchEnabled) {
      // 读取批量发布间隔，转换为毫秒
      putEventInterval =
          conf.getInt(YarnConfiguration.RM_TIMELINE_SERVER_V1_PUBLISHER_INTERVAL,
              YarnConfiguration.DEFAULT_RM_TIMELINE_SERVER_V1_PUBLISHER_INTERVAL)
              * 1000;
      if (putEventInterval <= 0) {
        throw new IllegalArgumentException(
            "RM_TIMELINE_SERVER_V1_PUBLISHER_INTERVAL should be greater than 0");
      }
      // 读取发送线程池大小
      dispatcherPoolSize = conf.getInt(
          YarnConfiguration.RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE,
          YarnConfiguration.
              DEFAULT_RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE);
      if (dispatcherPoolSize <= 0) {
        throw new IllegalArgumentException(
            "RM_SYSTEM_METRICS_PUBLISHER_DISPATCHER_POOL_SIZE should be greater than 0");
      }
      // 读取批量发送大小
      dispatcherBatchSize = conf.getInt(
          YarnConfiguration.RM_TIMELINE_SERVER_V1_PUBLISHER_DISPATCHER_BATCH_SIZE,
          YarnConfiguration.
              DEFAULT_RM_TIMELINE_SERVER_V1_PUBLISHER_DISPATCHER_BATCH_SIZE);
      if (dispatcherBatchSize <= 1) {
        throw new IllegalArgumentException(
            "RM_TIMELINE_SERVER_V1_PUBLISHER_DISPATCHER_BATCH_SIZE should be greater than 1");
      }
      // 初始化批量发布相关组件
      putEventThread = new PutEventThread();
      sendEventThreadPool = Executors.newFixedThreadPool(dispatcherPoolSize);
      entityQueue = new LinkedBlockingQueue<>(dispatcherBatchSize + 1);
      sendEntityLock = new Object();
      LOG.info("Timeline service v1 batch publishing enabled");
    } else {
      LOG.info("Timeline service v1 batch publishing disabled");
    }
    // 创建Timeline客户端并注册到服务体系
    client = TimelineClient.createTimelineClient();
    addIfService(client);
    super.serviceInit(conf);
    // 注册事件处理器
    getDispatcher().register(SystemMetricsEventType.class,
        new TimelineV1EventHandler());
  }

  @Override
  protected void serviceStart() throws Exception {
    if (isTimeLineServerBatchEnabled) {
      stopped = false;
      putEventThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    if (isTimeLineServerBatchEnabled) {
      stopped = true;
      putEventThread.interrupt();
      try {
        putEventThread.join();
        // 发送队列中剩余的实体
        SendEntity task = new SendEntity();
        if (!task.buffer.isEmpty()) {
          LOG.info("Initiating final putEntities, remaining entities left in entityQueue: {}",
              task.buffer.size());
          sendEventThreadPool.submit(task);
        }
      } finally {
        // 优雅关闭线程池
        sendEventThreadPool.shutdown();
        if (!sendEventThreadPool.awaitTermination(3, TimeUnit.SECONDS)) {
          sendEventThreadPool.shutdownNow();
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appCreated(RMApp app, long createdTime) {
    // 创建应用实体
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    // 填充应用基本信息
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
    entityInfo.put(ApplicationMetricsConstants.AM_NODE_LABEL_EXPRESSION,
        app.getAmNodeLabelExpression());
    entityInfo.put(ApplicationMetricsConstants.APP_NODE_LABEL_EXPRESSION,
        app.getAppNodeLabelExpression());
    // 填充调用上下文信息
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

    entity.setOtherInfo(entityInfo);
    // 创建创建事件
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    tEvent.setTimestamp(createdTime);

    entity.addEvent(tEvent);
    // 发布事件
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @Override
  public void appLaunched(RMApp app, long launchTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.LAUNCHED_EVENT_TYPE);
    tEvent.setTimestamp(launchTime);
    entity.addEvent(tEvent);

    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @Override
  public void appFinished(RMApp app, RMAppState state, long finishedTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(finishedTime);
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.DIAGNOSTICS_INFO_EVENT_INFO,
        app.getDiagnostics().toString());
    eventInfo.put(ApplicationMetricsConstants.FINAL_STATUS_EVENT_INFO,
        app.getFinalApplicationStatus().toString());
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        RMServerUtils.createApplicationState(state).toString());
    String latestApplicationAttemptId = app.getCurrentAppAttempt() == null
        ? null : app.getCurrentAppAttempt().getAppAttemptId().toString();
    if (latestApplicationAttemptId != null) {
      eventInfo.put(ApplicationMetricsConstants.LATEST_APP_ATTEMPT_EVENT_INFO,
          latestApplicationAttemptId);
    }
    // 填充应用metrics信息
    RMAppMetrics appMetrics = app.getRMAppMetrics();
    entity.addOtherInfo(ApplicationMetricsConstants.APP_CPU_METRICS,
        appMetrics.getVcoreSeconds());
    entity.addOtherInfo(ApplicationMetricsConstants.APP_MEM_METRICS,
        appMetrics.getMemorySeconds());
    entity.addOtherInfo(ApplicationMetricsConstants.APP_MEM_PREEMPT_METRICS,
            appMetrics.getPreemptedMemorySeconds());
    entity.addOtherInfo(ApplicationMetricsConstants.APP_CPU_PREEMPT_METRICS,
            appMetrics.getPreemptedVcoreSeconds());
    tEvent.setEventInfo(eventInfo);

    entity.addEvent(tEvent);

    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appUpdated(RMApp app, long updatedTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.QUEUE_ENTITY_INFO,
        app.getQueue());
    eventInfo.put(ApplicationMetricsConstants.APPLICATION_PRIORITY_INFO,
        app.getApplicationPriority().getPriority());
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(updatedTime);
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appStateUpdated(RMApp app, YarnApplicationState appState,
      long updatedTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(ApplicationMetricsConstants.STATE_EVENT_INFO,
        appState);
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(ApplicationMetricsConstants.STATE_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(updatedTime);
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appACLsUpdated(RMApp app, String appViewACLs, long updatedTime) {
    TimelineEntity entity = createApplicationEntity(app.getApplicationId());
    TimelineEvent tEvent = new TimelineEvent();
    Map<String, Object> entityInfo = new HashMap<String, Object>();
    entityInfo.put(ApplicationMetricsConstants.APP_VIEW_ACLS_ENTITY_INFO,
        (appViewACLs == null) ? "" : appViewACLs);
    entity.setOtherInfo(entityInfo);
    tEvent.setEventType(ApplicationMetricsConstants.ACLS_UPDATED_EVENT_TYPE);
    tEvent.setTimestamp(updatedTime);

    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(new TimelineV1PublishEvent(
        SystemMetricsEventType.PUBLISH_ENTITY, entity, app.getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void appAttemptRegistered(RMAppAttempt appAttempt,
      long registeredTime) {
    TimelineEntity entity =
        createAppAttemptEntity(appAttempt.getAppAttemptId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(AppAttemptMetricsConstants.REGISTERED_EVENT_TYPE);
    tEvent.setTimestamp(registeredTime);
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(AppAttemptMetricsConstants.TRACKING_URL_INFO,
        appAttempt.getTrackingUrl());
    eventInfo.put(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO,
        appAttempt.getOriginalTrackingUrl());
    eventInfo.put(AppAttemptMetricsConstants.HOST_INFO,
        appAttempt.getHost());
    eventInfo.put(AppAttemptMetricsConstants.RPC_PORT_INFO,
        appAttempt.getRpcPort());
    if (appAttempt.getMasterContainer() != null) {
      eventInfo.put(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO,
          appAttempt.getMasterContainer().getId().toString());
    }
    tEvent.setEventInfo(eventInfo);
    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(
        new TimelineV1PublishEvent(SystemMetricsEventType.PUBLISH_ENTITY,
            entity, appAttempt.getAppAttemptId().getApplicationId()));

  }

  @SuppressWarnings("unchecked")
  @Override
  public void appAttemptFinished(RMAppAttempt appAttempt,
      RMAppAttemptState appAttemptState, RMApp app, long finishedTime) {
    TimelineEntity entity =
        createAppAttemptEntity(appAttempt.getAppAttemptId());

    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setEventType(AppAttemptMetricsConstants.FINISHED_EVENT_TYPE);
    tEvent.setTimestamp(finishedTime);
    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put(AppAttemptMetricsConstants.TRACKING_URL_INFO,
        appAttempt.getTrackingUrl());
    eventInfo.put(AppAttemptMetricsConstants.ORIGINAL_TRACKING_URL_INFO,
        appAttempt.getOriginalTrackingUrl());
    eventInfo.put(AppAttemptMetricsConstants.DIAGNOSTICS_INFO,
        appAttempt.getDiagnostics());
    eventInfo.put(AppAttemptMetricsConstants.FINAL_STATUS_INFO,
        app.getFinalApplicationStatus().toString());
    eventInfo.put(AppAttemptMetricsConstants.STATE_INFO, RMServerUtils
        .createApplicationAttemptState(appAttemptState).toString());
    if (appAttempt.getMasterContainer() != null) {
      eventInfo.put(AppAttemptMetricsConstants.MASTER_CONTAINER_INFO,
          appAttempt.getMasterContainer().getId().toString());
    }
    tEvent.setEventInfo(eventInfo);

    entity.addEvent(tEvent);
    getDispatcher().getEventHandler().handle(
        new TimelineV1PublishEvent(SystemMetricsEventType.PUBLISH_ENTITY,
            entity, appAttempt.getAppAttemptId().getApplicationId()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void containerCreated(RMContainer container, long createdTime) {
    TimelineEntity entity = createContainerEntity(container