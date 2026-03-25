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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.FlowContextProto;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.LogAggregationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/**
 * NodeManager中应用程序实例的状态机实现，管理单个应用在本节点上的完整生命周期。
 */
public class ApplicationImpl implements Application {

  final Dispatcher dispatcher;
  final String user;
  // 仅在Timeline Service V2启用时设置流上下文信息
  private FlowContext flowContext;
  final ApplicationId appId;
  final Credentials credentials;
  Map<ApplicationAccessType, String> applicationACLs;
  final ApplicationACLsManager aclsManager;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private final Context context;

  private static final Logger LOG =
       LoggerFactory.getLogger(ApplicationImpl.class);

  private LogAggregationContext logAggregationContext;

  Map<ContainerId, Container> containers =
      new ConcurrentHashMap<>();

  /**
   * The timestamp when the log aggregation has started for this application.
   * Used to determine the age of application log files during log aggregation.
   * When logAggregationRentention policy is enabled, log files older than
   * the retention policy will not be uploaded but scheduled for deletion.
   */
  private long applicationLogInitedTimestamp = -1;
  private final NMStateStoreService appStateStore;

  /**
   * 构造不包含流上下文的应用实例。
   */
  public ApplicationImpl(Dispatcher dispatcher, String user,
      ApplicationId appId, Credentials credentials, Context context) {
    this(dispatcher, user, null, appId, credentials, context, -1L);
  }

  /**
   * 构造完整的应用实例，支持恢复已有日志初始化时间戳。
   */
  public ApplicationImpl(Dispatcher dispatcher, String user,
      FlowContext flowContext, ApplicationId appId, Credentials credentials,
      Context context, long recoveredLogInitedTime) {
    this.dispatcher = dispatcher;
    this.user = user;
    this.appId = appId;
    this.credentials = credentials;
    this.aclsManager = context.getApplicationACLsManager();
    Configuration conf = context.getConf();
    // 启用Timeline Service V2时需要初始化流上下文和时间线客户端
    if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
      if (flowContext == null) {
        throw new IllegalArgumentException("flow context cannot be null");
      }
      this.flowContext = flowContext;
      if (YarnConfiguration.systemMetricsPublisherEnabled(conf)) {
        context.getNMTimelinePublisher().createTimelineClient(appId);
      }
    }
    this.context = context;
    this.appStateStore = context.getNMStateStore();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    // 初始化应用状态机
    stateMachine = stateMachineFactory.make(this);
    setAppLogInitedTimestamp(recoveredLogInitedTime);
  }

  /**
   * 构造包含流上下文的新应用实例。
   */
  public ApplicationImpl(Dispatcher dispatcher, String user,
      FlowContext flowContext, ApplicationId appId,
      Credentials credentials, Context context) {
    this(dispatcher, user, flowContext, appId, credentials,
      context, -1);
  }

  /**
   * 存储应用流上下文信息，用于Timeline Service V2指标追踪。
   */
  public static class FlowContext {
    private final String flowName;
    private final String flowVersion;
    private final long flowRunId;

    public FlowContext(String flowName, String flowVersion, long flowRunId) {
      this.flowName = flowName;
      this.flowVersion = flowVersion;
      this.flowRunId = flowRunId;
    }

    public String getFlowName() {
      return flowName;
    }

    public String getFlowVersion() {
      return flowVersion;
    }

    public long getFlowRunId() {
      return flowRunId;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("{");
      sb.append("Flow Name=").append(getFlowName());
      sb.append(" Flow Versioin=").append(getFlowVersion());
      sb.append(" Flow Run Id=").append(getFlowRunId()).append(" }");
      return sb.toString();
    }
  }

  @Override
  public String getUser() {
    return user.toString();
  }

  @Override
  public ApplicationId getAppId() {
    return appId;
  }

  @Override
  public ApplicationState getApplicationState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<ContainerId, Container> getContainers() {
    this.readLock.lock();
    try {
      return this.containers;
    } finally {
      this.readLock.unlock();
    }
  }

  private static final ContainerDoneTransition CONTAINER_DONE_TRANSITION =
      new ContainerDoneTransition();

  private static final InitContainerTransition INIT_CONTAINER_TRANSITION =
      new InitContainerTransition();

  // 应用状态机定义，描述所有状态和转移规则
  private static StateMachineFactory<ApplicationImpl, ApplicationState,
          ApplicationEventType, ApplicationEvent> stateMachineFactory =
      new StateMachineFactory<ApplicationImpl, ApplicationState,
          ApplicationEventType, ApplicationEvent>(ApplicationState.NEW)

           // NEW状态转移
           .addTransition(ApplicationState.NEW, ApplicationState.INITING,
               ApplicationEventType.INIT_APPLICATION, new AppInitTransition())
           .addTransition(ApplicationState.NEW, ApplicationState.NEW,
               ApplicationEventType.INIT_CONTAINER,
               INIT_CONTAINER_TRANSITION)

           // INITING状态转移
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.INIT_CONTAINER,
               INIT_CONTAINER_TRANSITION)
           .addTransition(ApplicationState.INITING,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.FINISH_APPLICATION,
               new AppFinishTriggeredTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
               CONTAINER_DONE_TRANSITION)
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
               new AppLogInitDoneTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.INITING,
               ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
               new AppLogInitFailTransition())
           .addTransition(ApplicationState.INITING, ApplicationState.RUNNING,
               ApplicationEventType.APPLICATION_INITED,
               new AppInitDoneTransition())

           // RUNNING状态转移
           .addTransition(ApplicationState.RUNNING,
               ApplicationState.RUNNING,
               ApplicationEventType.INIT_CONTAINER,
               INIT_CONTAINER_TRANSITION)
           .addTransition(ApplicationState.RUNNING,
               ApplicationState.RUNNING,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
               CONTAINER_DONE_TRANSITION)
           .addTransition(
               ApplicationState.RUNNING,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.FINISH_APPLICATION,
               new AppFinishTriggeredTransition())

           // FINISHING_CONTAINERS_WAIT状态转移
           .addTransition(
               ApplicationState.FINISHING_CONTAINERS_WAIT,
               EnumSet.of(ApplicationState.FINISHING_CONTAINERS_WAIT,
                   ApplicationState.APPLICATION_RESOURCES_CLEANINGUP),
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
               new AppFinishTransition())
          .addTransition(ApplicationState.FINISHING_CONTAINERS_WAIT,
              ApplicationState.FINISHING_CONTAINERS_WAIT,
              ApplicationEventType.INIT_CONTAINER,
              INIT_CONTAINER_TRANSITION)
          .addTransition(ApplicationState.FINISHING_CONTAINERS_WAIT,
              ApplicationState.FINISHING_CONTAINERS_WAIT,
              EnumSet.of(
                  ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
                  ApplicationEventType.APPLICATION_INITED,
                  ApplicationEventType.FINISH_APPLICATION))

           // APPLICATION_RESOURCES_CLEANINGUP状态转移
           .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationEventType.APPLICATION_CONTAINER_FINISHED)
           .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
               ApplicationState.FINISHED,
               ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP,
               new AppCompletelyDoneTransition())
          .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
              ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
              ApplicationEventType.INIT_CONTAINER,
              INIT_CONTAINER_TRANSITION)
          .addTransition(ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
              ApplicationState.APPLICATION_RESOURCES_CLEANINGUP,
              EnumSet.of(
                  ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED,
                  ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED,
                  ApplicationEventType.APPLICATION_INITED,
                  ApplicationEventType.FINISH_APPLICATION))

           // FINISHED状态转移
           .addTransition(ApplicationState.FINISHED,
               ApplicationState.FINISHED,
               EnumSet.of(
                   ApplicationEventType.APPLICATION_LOG_HANDLING_FINISHED,
                   ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED),
               new AppLogsAggregatedTransition())
          .addTransition(ApplicationState.FINISHED,
              ApplicationState.FINISHED,
              ApplicationEventType.INIT_CONTAINER,
              INIT_CONTAINER_TRANSITION)
           .addTransition(ApplicationState.FINISHED, ApplicationState.FINISHED,
               EnumSet.of(
                  ApplicationEventType.APPLICATION_LOG_HANDLING_INITED,
                  ApplicationEventType.APPLICATION_CONTAINER_FINISHED,
                  ApplicationEventType.FINISH_APPLICATION))
           // 构建状态转移拓扑表
           .installTopology();

  private final StateMachine<ApplicationState, ApplicationEventType, ApplicationEvent> stateMachine;

  /**
   * 处理应用初始化事件，保存访问控制列表，触发日志聚合服务初始化。
   */
  @SuppressWarnings("unchecked")
  static class AppInitTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      ApplicationInitEvent initEvent = (ApplicationInitEvent)event;
      // 保存应用访问控制列表
      app.applicationACLs = initEvent.getApplicationACLs();
      app.aclsManager.addApplication(app.getAppId(), app.applicationACLs);
      // 保存日志聚合上下文
      app.logAggregationContext = initEvent.getLogAggregationContext();
      // 发布应用启动事件通知日志处理器
      app.dispatcher.getEventHandler().handle(
          new LogHandlerAppStartedEvent(app.appId, app.user,
              app.credentials, app.applicationACLs,
              app.logAggregationContext, app.applicationLogInitedTimestamp));
    }
  }

  /**
   * 处理日志聚合初始化完成事件，触发应用资源本地化，持久化应用状态。
   */
  @SuppressWarnings("unchecked")
  static class AppLogInitDoneTransition implements
      SingleArcTransition<ApplicationImpl, ApplicationEvent> {
    @Override
    public void transition(ApplicationImpl app, ApplicationEvent event) {
      // 触发应用级资源本地化初始化
      app.dispatcher.getEventHandler().handle(
          new ApplicationLocalizationEvent(
              LocalizationEventType.INIT_APPLICATION_RESOURCES, app));
      // 保存日志聚合初始化时间戳
      app.setAppLogInitedTimestamp(event.getTimestamp());
      // 持久化应用状态到状态存储，用于NM恢复
      try {
        app.appStateStore.storeApplication(app.appId, buildAppProto(app));
      } catch (Exception ex) {
        LOG.warn("failed to update application state in state store", ex);
      }
    }
  }

  @VisibleForTesting
  void setAppLogInitedTimestamp(long appLogInitedTimestamp) {
    this.applicationLogInitedTimestamp = appLogInitedTimestamp;
  }

  /**
   * 将应用信息序列化为protobuf格式，用于NM状态持久化。
   */
  static ContainerManagerApplicationProto buildAppProto(ApplicationImpl app)
      throws IOException {
    ContainerManagerApplicationProto.Builder builder =
        ContainerManagerApplicationProto.newBuilder();
    // 写入应用ID
    builder.setId(((ApplicationIdPBImpl) app.appId).getProto());
    // 写入用户名
    builder.setUser(app.getUser());

    // 写入日志聚合上下文
    if (app.logAggregationContext != null) {
      builder.setLogAggregationContext((
          (LogAggregationContextPBImpl)app.logAggregationContext).getProto());
    }

    // 写入凭证信息
    builder.clearCredentials();
    if (app.credentials != null) {
      DataOutputBuffer dob = new DataOutputBuffer();
      app.credentials.writeTokenStorageToStream(dob