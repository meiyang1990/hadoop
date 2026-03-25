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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import java.net.InetAddress;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.BlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.DisabledBlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.SimpleBlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent.RMAppNodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppStartAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.timelineservice.collector.AppLevelTimelineCollector;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN ResourceManager中应用程序的核心实现类，管理应用程序的完整生命周期
 * 采用状态机模式处理各种事件，管理应用的多个尝试运行(Attempt)，处理状态转换和资源清理
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class RMAppImpl implements RMApp, Recoverable {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMAppImpl.class);
  private static final String UNAVAILABLE = "N/A";
  private static final String UNLIMITED = "UNLIMITED";
  private static final long UNKNOWN = -1L;
  // 所有已完成应用程序的状态集合
  private static final EnumSet<RMAppState> COMPLETED_APP_STATES =
      EnumSet.of(RMAppState.FINISHED, RMAppState.FINISHING, RMAppState.FAILED,
          RMAppState.KILLED, RMAppState.FINAL_SAVING, RMAppState.KILLING);
  private static final String STATE_CHANGE_MESSAGE =
      "%s State change from %s to %s on event = %s";
  private static final String RECOVERY_MESSAGE =
      "Recovering app: %s with %d attempts and final state = %s";

  // 不可变字段，应用生命周期内不会改变
  private final ApplicationId applicationId;
  private final RMContext rmContext;
  private final Configuration conf;
  private final String user;
  private final UserGroupInformation userUgi;
  private final String name;
  private final ApplicationSubmissionContext submissionContext;
  private final Dispatcher dispatcher;
  private final YarnScheduler scheduler;
  private final ApplicationMasterService masterService;
  private final StringBuilder diagnostics = new StringBuilder();
  private final int maxAppAttempts;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  // 存储该应用所有尝试运行实例，保持插入顺序
  private final Map<ApplicationAttemptId, RMAppAttempt> attempts
      = new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>();
  private final long submitTime;
  // 存储待处理的节点更新事件
  private final Map<RMNode, NodeUpdateType> updatedNodes = new HashMap<>();
  private final String applicationType;
  private final Set<String> applicationTags;
  private Map<String, String> applicationSchedulingEnvs = new HashMap<>();

  private final long attemptFailuresValidityInterval;
  // 是否启用AM节点黑名单
  private boolean amBlacklistingEnabled = false;
  // 黑名单自动禁用阈值
  private float blacklistDisableThreshold;

  private Clock systemClock;

  private boolean isNumAttemptsBeyondThreshold = false;


  // 可变字段，运行过程中会被修改
  private long startTime;
  private long launchTime = 0;
  private long finishTime = 0;
  private long storedFinishTime = 0;
  private int firstAttemptIdInStateStore = 1;
  private int nextAttemptId = 1;
  private AppCollectorData collectorData;
  private CollectorInfo collectorInfo;
  // 当前正在运行的尝试实例
  private volatile RMAppAttempt currentAttempt;
  private String queue;
  private EventHandler handler;
  private static final AppFinishedTransition FINISHED_TRANSITION =
      new AppFinishedTransition();
  // 存储应用已经运行过的节点ID集合
  private Set<NodeId> ranNodes = new ConcurrentSkipListSet<NodeId>();

  private final RMAppLogAggregation logAggregation;
  // 存储应用各类超时配置（生命周期超时等）
  private Map<ApplicationTimeoutType, Long> applicationTimeouts =
      new HashMap<ApplicationTimeoutType, Long>();

  // 杀死/保存终态前保存原有状态，用于生成应用报告
  private RMAppState stateBeforeKilling;
  private RMAppState stateBeforeFinalSaving;
  private RMAppEvent eventCausingFinalSaving;
  private RMAppState targetedFinalState;
  private RMAppState recoveredFinalState;
  private List<ResourceRequest> amReqs;
  
  private CallerContext callerContext;

  private ApplicationPlacementContext placementContext;

  Object transitionTodo;

  private Priority applicationPriority;

  // 应用状态机工厂，定义所有可能的状态转换规则
  private static final StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent> stateMachineFactory
                               = new StateMachineFactory<RMAppImpl,
                                           RMAppState,
                                           RMAppEventType,
                                           RMAppEvent>(RMAppState.NEW)


     // NEW状态转换
    .addTransition(RMAppState.NEW, RMAppState.NEW,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.NEW, RMAppState.NEW_SAVING,
        RMAppEventType.START, new RMAppNewlySavingTransition())
    .addTransition(RMAppState.NEW, EnumSet.of(RMAppState.SUBMITTED,
            RMAppState.ACCEPTED, RMAppState.FINISHED, RMAppState.FAILED,
            RMAppState.KILLED, RMAppState.FINAL_SAVING),
        RMAppEventType.RECOVER, new RMAppRecoveredTransition())
    .addTransition(RMAppState.NEW, RMAppState.KILLED, RMAppEventType.KILL,
        new AppKilledTransition())
    .addTransition(RMAppState.NEW, RMAppState.FINAL_SAVING,
        RMAppEventType.APP_REJECTED,
        new FinalSavingTransition(new AppRejectedTransition(),
          RMAppState.FAILED))

    // NEW_SAVING状态转换（正在保存新应用信息到状态存储）
    .addTransition(RMAppState.NEW_SAVING, RMAppState.NEW_SAVING,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.NEW_SAVING, RMAppState.SUBMITTED,
        RMAppEventType.APP_NEW_SAVED, new AddApplicationToSchedulerTransition())
    .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING,
        RMAppEventType.KILL,
        new FinalSavingTransition(
          new AppKilledTransition(), RMAppState.KILLED))
    .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING,
        RMAppEventType.APP_REJECTED,
          new FinalSavingTransition(new AppRejectedTransition(),
            RMAppState.FAILED))
      .addTransition(RMAppState.NEW_SAVING, RMAppState.FAILED,
          RMAppEventType.APP_SAVE_FAILED, new AppRejectedTransition())

     // SUBMITTED状态转换（已提交等待调度器接受）
    .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING,
        RMAppEventType.APP_REJECTED,
        new FinalSavingTransition(
          new AppRejectedTransition(), RMAppState.FAILED))
    .addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED,
        RMAppEventType.APP_ACCEPTED, new StartAppAttemptTransition())
    .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING,
        RMAppEventType.KILL,
        new FinalSavingTransition(
          new AppKilledTransition(), RMAppState.KILLED))

     // ACCEPTED状态转换（已被调度器接受，等待AM启动）
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
        RMAppEventType.ATTEMPT_REGISTERED, new RMAppStateUpdateTransition(
            YarnApplicationState.RUNNING))
    .addTransition(RMAppState.ACCEPTED,
        EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.ACCEPTED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_FINISHED,
        new FinalSavingTransition(FINISHED_TRANSITION, RMAppState.FINISHED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.KILLING,
        RMAppEventType.KILL, new KillAttemptTransition())
    .addTransition(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_KILLED,
        new FinalSavingTransition(new AppKilledTransition(), RMAppState.KILLED))
    .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
      .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
        RMAppEventType.ATTEMPT_LAUNCHED,
        new AttemptLaunchedTransition())

     // RUNNING状态转换（AM已注册，正在运行）
    .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
        RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
    .addTransition(RMAppState.RUNNING, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_UNREGISTERED,
        new FinalSavingTransition(
          new AttemptUnregisteredTransition(),
          RMAppState.FINISHING, RMAppState.FINISHED))
    .addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
        RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
    .addTransition(RMAppState.RUNNING, RMAppState.RUNNING, 
        RMAppEventType.APP_RUNNING_ON_NODE,
        new AppRunningOnNodeTransition())
    .addTransition(RMAppState.RUNNING,
        EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
        RMAppEventType.ATTEMPT_FAILED,
        new AttemptFailedTransition(RMAppState.ACCEPTED))
    .addTransition(RMAppState.RUNNING, RMAppState.KILLING,
        RMAppEventType.KILL, new KillAttemptTransition())

     // FINAL_SAVING状态转换（正在保存应用终态到状态存储）
    .addTransition(RMAppState.FINAL_SAVING,
      EnumSet.of(RMAppState.FINISHING, RMAppState.FAILED,
        RMAppState.KILLED, RMAppState.FINISHED), RMAppEventType.APP_UPDATE_SAVED,
        new FinalStateSavedTransition())
    .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
        RMAppEventType.ATTEMPT_FINISHED,
        new AttemptFinishedAtFinalSavingTransition())
    .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING, 
        RMAppEventType.APP_RUNNING_ON_NODE,