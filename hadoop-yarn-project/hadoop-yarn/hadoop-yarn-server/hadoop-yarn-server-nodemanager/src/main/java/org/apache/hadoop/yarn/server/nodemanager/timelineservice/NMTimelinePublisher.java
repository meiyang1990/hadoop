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

package org.apache.hadoop.yarn.server.nodemanager.timelineservice;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerPauseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResumeEvent;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity.Identifier;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.metrics.ContainerMetricsConstants;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl.ContainerMetric;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * NodeManager端时间线服务V2指标发布服务，将容器事件和资源使用指标发布到时间线服务。
 * 仅当时间线服务V2启用且系统允许发布事件指标时生效。
 */
public class NMTimelinePublisher extends CompositeService {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMTimelinePublisher.class);

  private Dispatcher dispatcher;

  private Context context;

  private NodeId nodeId;

  private String httpAddress;
  private String httpPort;

  private UserGroupInformation nmLoginUGI;

  private final Map<ApplicationId, TimelineV2Client> appToClientMap;

  private boolean publishNMContainerEvents = true;

  /**
   * 构造NM时间线发布器，持有NodeManager上下文引用。
   * @param context NodeManager上下文
   */
  public NMTimelinePublisher(Context context) {
    super(NMTimelinePublisher.class.getName());
    this.context = context;
    appToClientMap = new ConcurrentHashMap<>();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 创建事件分发器
    dispatcher = createDispatcher();
    // 注册时间线事件处理器
    dispatcher.register(NMTimelineEventType.class,
        new ForwardingEventHandler());
    // 将分发器添加为子服务
    addIfService(dispatcher);
    // 初始化NodeManager登录用户UGI，用于安全认证
    this.nmLoginUGI =  UserGroupInformation.isSecurityEnabled() ?
        UserGroupInformation.getLoginUser() :
        UserGroupInformation.getCurrentUser();
    LOG.info("Initialized NMTimelinePublisher UGI to " + nmLoginUGI);

    // 解析NM Web服务端口
    String webAppURLWithoutScheme =
        WebAppUtils.getNMWebAppURLWithoutScheme(conf);
    if (webAppURLWithoutScheme.contains(":")) {
      httpPort = webAppURLWithoutScheme.split(":")[1];
    }

    // 读取配置确定是否发布容器事件
    publishNMContainerEvents = conf.getBoolean(
        YarnConfiguration.NM_PUBLISH_CONTAINER_EVENTS_ENABLED,
        YarnConfiguration.DEFAULT_NM_PUBLISH_CONTAINER_EVENTS_ENABLED);
    super.serviceInit(conf);
  }

  /**
   * 创建异步事件分发器，专门处理时间线发布事件。
   * @return 异步分发器实例
   */
  protected AsyncDispatcher createDispatcher() {
    return new AsyncDispatcher("NM Timeline dispatcher");
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // context在ContainerManager启动后才会更新节点信息，所以在此处获取
    this.nodeId = context.getNodeId();
    this.httpAddress = nodeId.getHost() + ":" + httpPort;
  }

  @Override
  protected void serviceStop() throws Exception {
    // 停止所有应用的时间线客户端
    for(ApplicationId app : appToClientMap.keySet()) {
      stopTimelineClient(app);
    }
    super.serviceStop();
  }

  @VisibleForTesting
  Map<ApplicationId, TimelineV2Client> getAppToClientMap() {
    return appToClientMap;
  }

  /**
   * 处理NM时间线事件，根据事件类型分发处理。
   * @param event 时间线事件
   */
  protected void handleNMTimelineEvent(NMTimelineEvent event) {
    switch (event.getType()) {
    case TIMELINE_ENTITY_PUBLISH:
      // 发布时间线实体
      putEntity(((TimelinePublishEvent) event).getTimelineEntityToPublish(),
          ((TimelinePublishEvent) event).getApplicationId());
      break;
    case STOP_TIMELINE_CLIENT:
      // 停止并移除时间线客户端
      removeAndStopTimelineClient(event.getApplicationId());
      break;
    default:
      LOG.error("Unknown NMTimelineEvent type: " + event.getType());
    }
  }

  /**
   * 上报容器资源使用指标到时间线服务。
   * @param container 容器实例
   * @param pmemUsage 物理内存使用量
   * @param cpuUsagePercentPerCore CPU使用率（按核心）
   */
  public void reportContainerResourceUsage(Container container, Long pmemUsage,
      Float cpuUsagePercentPerCore) {
    if (publishNMContainerEvents) {
      // 只要内存或CPU有一个可用就发布
      if (pmemUsage != ResourceCalculatorProcessTree.UNAVAILABLE
          || cpuUsagePercentPerCore !=
          ResourceCalculatorProcessTree.UNAVAILABLE) {
        // 创建容器实体对象
        ContainerEntity entity =
            createContainerEntity(container.getContainerId());
        long currentTimeMillis = System.currentTimeMillis();
        // 添加内存指标
        if (pmemUsage != ResourceCalculatorProcessTree.UNAVAILABLE) {
          TimelineMetric memoryMetric = new TimelineMetric();
          memoryMetric.setId(ContainerMetric.MEMORY.toString());
          memoryMetric.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
          memoryMetric.addValue(currentTimeMillis, pmemUsage);
          entity.addMetric(memoryMetric);
        }
        // 添加CPU指标
        if (cpuUsagePercentPerCore !=
            ResourceCalculatorProcessTree.UNAVAILABLE) {
          TimelineMetric cpuMetric = new TimelineMetric();
          cpuMetric.setId(ContainerMetric.CPU.toString());
          // TODO: support average
          cpuMetric.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
          cpuMetric.addValue(currentTimeMillis,
              Math.round(cpuUsagePercentPerCore));
          entity.addMetric(cpuMetric);
        }
        // 获取对应应用的时间线客户端异步发布指标
        ApplicationId appId = container.getContainerId().
            getApplicationAttemptId().getApplicationId();
        try {
          // 时间线客户端内部已有排队机制，无需额外处理
          TimelineV2Client timelineClient = getTimelineClient(appId);
          if (timelineClient != null) {
            timelineClient.putEntitiesAsync(entity);
          } else {
            LOG.error("Seems like client has been removed before the container"
                + " metric could be published for " +
                container.getContainerId());
          }
        } catch (IOException e) {
          LOG.error(
              "Failed to publish Container metrics for container " +
                  container.getContainerId());
          LOG.debug("Failed to publish Container metrics for container {}",
              container.getContainerId(), e);
        } catch (YarnException e) {
          LOG.error(
              "Failed to publish Container metrics for container " +
                  container.getContainerId(), e.getMessage());
          LOG.debug("Failed to publish Container metrics for container {}",
              container.getContainerId(), e);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerCreatedEvent(ContainerEvent event) {
    if (publishNMContainerEvents) {
      ContainerId containerId = event.getContainerID();
      ContainerEntity entity = createContainerEntity(containerId);
      Container container = context.getContainers().get(containerId);
      Resource resource = container.getResource();

      // 填充容器分配信息
      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_MEMORY_INFO,
          resource.getMemorySize());
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_VCORE_INFO,
          resource.getVirtualCores());
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_HOST_INFO,
          nodeId.getHost());
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_PORT_INFO,
          nodeId.getPort());
      entityInfo.put(ContainerMetricsConstants.ALLOCATED_PRIORITY_INFO,
          container.getPriority().toString());
      entityInfo.put(
          ContainerMetricsConstants.ALLOCATED_HOST_HTTP_ADDRESS_INFO,
          httpAddress);
      entity.setInfo(entityInfo);

      // 添加容器创建事件
      TimelineEvent tEvent = new TimelineEvent();
      tEvent.setId(ContainerMetricsConstants.CREATED_EVENT_TYPE);
      tEvent.setTimestamp(event.getTimestamp());

      long containerStartTime = container.getContainerStartTime();
      entity.addEvent(tEvent);
      entity.setCreatedTime(containerStartTime);
      // 发布事件到时间线
      dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
          containerId.getApplicationAttemptId().getApplicationId()));
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerResumedEvent(
      ContainerEvent event) {
    if (publishNMContainerEvents) {
      ContainerResumeEvent resumeEvent = (ContainerResumeEvent) event;
      ContainerId containerId = resumeEvent.getContainerID();
      ContainerEntity entity = createContainerEntity(containerId);

      // 填充诊断信息
      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
          resumeEvent.getDiagnostic());
      entity.setInfo(entityInfo);

      Container container = context.getContainers().get(containerId);
      if (container != null) {
        // 添加容器恢复事件
        TimelineEvent tEvent = new TimelineEvent();
        tEvent.setId(ContainerMetricsConstants.RESUMED_EVENT_TYPE);
        tEvent.setTimestamp(event.getTimestamp());
        entity.addEvent(tEvent);
        // 发布事件到时间线
        dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
            containerId.getApplicationAttemptId().getApplicationId()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerPausedEvent(
      ContainerEvent event) {
    if (publishNMContainerEvents) {
      ContainerPauseEvent pauseEvent = (ContainerPauseEvent) event;
      ContainerId containerId = pauseEvent.getContainerID();
      ContainerEntity entity = createContainerEntity(containerId);

      // 填充诊断信息
      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
          pauseEvent.getDiagnostic());
      entity.setInfo(entityInfo);

      Container container = context.getContainers().get(containerId);
      if (container != null) {
        // 添加容器暂停事件
        TimelineEvent tEvent = new TimelineEvent();
        tEvent.setId(ContainerMetricsConstants.PAUSED_EVENT_TYPE);
        tEvent.setTimestamp(event.getTimestamp());
        entity.addEvent(tEvent);
        // 发布事件到时间线
        dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
            containerId.getApplicationAttemptId().getApplicationId()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerKilledEvent(
      ContainerEvent event) {
    if (publishNMContainerEvents) {
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      ContainerId containerId = killEvent.getContainerID();
      ContainerEntity entity = createContainerEntity(containerId);

      // 填充诊断信息和退出状态
      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
          killEvent.getDiagnostic());
      entityInfo.put(ContainerMetricsConstants.EXIT_STATUS_INFO,
          killEvent.getContainerExitStatus());
      entity.setInfo(entityInfo);

      Container container = context.getContainers().get(containerId);
      if (container != null) {
        // 添加容器杀死事件
        TimelineEvent tEvent = new TimelineEvent();
        tEvent.setId(ContainerMetricsConstants.KILLED_EVENT_TYPE);
        tEvent.setTimestamp(event.getTimestamp());
        entity.addEvent(tEvent);
        // 发布事件到时间线
        dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
            containerId.getApplicationAttemptId().getApplicationId()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void publishContainerFinishedEvent(ContainerStatus containerStatus,
      long containerFinishTime, long containerStartTime) {
    if (publishNMContainerEvents) {
      ContainerId containerId = containerStatus.getContainerId();
      TimelineEntity entity = createContainerEntity(containerId);

      // 填充容器完成信息
      Map<String, Object> entityInfo = new HashMap<String, Object>();
      entityInfo.put(ContainerMetricsConstants.DIAGNOSTICS_INFO,
          containerStatus.getDiagnostics());
      entityInfo.put(ContainerMetricsConstants.EXIT_STATUS_INFO,
          containerStatus.getExitStatus());
      entityInfo.put(ContainerMetricsConstants.STATE_INFO,
          ContainerState.COMPLETE.toString());
      entityInfo.put(ContainerMetricsConstants.CONTAINER_FINISHED_TIME,
          containerFinishTime);
      entity.setInfo(entityInfo);

      // 添加容器完成事件
      TimelineEvent tEvent = new TimelineEvent();
      tEvent.setId(ContainerMetricsConstants.FINISHED_EVENT_TYPE);
      tEvent.setTimestamp(containerFinishTime);
      entity.addEvent(tEvent);

      // 发布事件到时间线
      dispatcher.getEventHandler().handle(new TimelinePublishEvent(entity,
          containerId.getApplicationAttemptId().getApplicationId()));
    }
  }

  private void publishContainerLocalizationEvent(
      ContainerLocalizationEvent event, String eventType) {
    if (publishNMContainerEvents) {
      Container container = event.getContainer();
      ContainerId containerId = container.getContainerId();
      TimelineEntity entity = createContainerEntity(containerId);

      // 添加本地化事件
      TimelineEvent tEvent = new TimelineEvent();
      tEvent.setId(eventType);
      tEvent.setTimestamp(event.getTimestamp());
      entity.addEvent(tEvent);

      // 获取对应应用的时间线客户端异步发布事件
      ApplicationId appId = container.getContainerId().
          getApplicationAttemptId().getApplicationId();
      try {
        // 时间线客户端内部已有排队机制，无需额外处理
        TimelineV2Client timelineClient = getTimelineClient(appId);
        if (timelineClient != null) {
          timelineClient.putEntitiesAsync(entity);