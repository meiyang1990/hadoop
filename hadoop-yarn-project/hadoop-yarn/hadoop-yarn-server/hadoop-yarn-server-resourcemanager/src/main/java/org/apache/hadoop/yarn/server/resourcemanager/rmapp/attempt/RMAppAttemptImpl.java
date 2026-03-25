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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import static org.apache.hadoop.yarn.util.StringHelper.pjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEvent;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.BlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.DisabledBlacklistManager;

import org.apache.hadoop.yarn.server.resourcemanager.nodelabels
    .RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppFailedAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeFinishedContainersPulledByAMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.BoundedAppender;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN ResourceManager 中应用尝试（RMAppAttempt）的核心实现类。
 * 代表一个应用程序的一次运行尝试，管理AM容器生命周期、状态转换和容器事件处理
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class RMAppAttemptImpl implements RMAppAttempt, Recoverable {
  private static final String STATE_CHANGE_MESSAGE =
      "%s State change from %s to %s on event = %s";
  private static final String RECOVERY_MESSAGE =
      "Recovering attempt: %s with final state = %s";
  private static final String DIAGNOSTIC_LIMIT_CONFIG_ERROR_MESSAGE =
      "The value of %s should be a positive integer: %s";

  private static final Logger LOG =
      LoggerFactory.getLogger(RMAppAttemptImpl.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public final static Priority AM_CONTAINER_PRIORITY = recordFactory
      .newRecordInstance(Priority.class);

  static {
    AM_CONTAINER_PRIORITY.setPriority(0);
  }

  private final StateMachine<RMAppAttemptState,
                             RMAppAttemptEventType,
                             RMAppAttemptEvent> stateMachine;

  private final RMContext rmContext;
  private final EventHandler eventHandler;
  private final YarnScheduler scheduler;
  private final ApplicationMasterService masterService;
  private final RMApp rmApp;

  private final ReadLock readLock;
  private final WriteLock writeLock;

  private final ApplicationAttemptId applicationAttemptId;
  private final ApplicationSubmissionContext submissionContext;
  private Token<AMRMTokenIdentifier> amrmToken = null;
  private volatile Integer amrmTokenKeyId = null;
  private SecretKey clientTokenMasterKey = null;

  // 刚完成、等待AM拉取的容器状态列表，按Node分组
  private ConcurrentMap<NodeId, List<ContainerStatus>>
      justFinishedContainers = new ConcurrentHashMap<>();
  // Tracks the previous finished containers that are waiting to be
  // verified as received by the AM. If the AM sends the next allocate
  // request it implicitly acks this list.
  // 已经发送给AM、等待AM确认接收的已完成容器列表
  private ConcurrentMap<NodeId, List<ContainerStatus>>
      finishedContainersSentToAM = new ConcurrentHashMap<>();
  private volatile Container masterContainer;

  private float progress = 0;
  private String host = "N/A";
  private int rpcPort = -1;
  private String originalTrackingUrl = "N/A";
  private String proxiedTrackingUrl = "N/A";
  private long startTime = 0;
  private long finishTime = 0;
  private long launchAMStartTime = 0;
  private long launchAMEndTime = 0;
  private long scheduledTime = 0;
  private long containerAllocatedTime = 0;
  // 标记非工作保留重启场景下AM容器已完成
  private boolean nonWorkPreservingAMContainerFinished = false;

  // Set to null initially. Will eventually get set
  // if an RMAppAttemptUnregistrationEvent occurs
  private FinalApplicationStatus finalStatus = null;
  private final BoundedAppender diagnostics;
  private int amContainerExitStatus = ContainerExitStatus.INVALID;

  private Configuration conf;
  private static final ExpiredTransition EXPIRED_TRANSITION =
      new ExpiredTransition();
  private static final AttemptFailedTransition FAILED_TRANSITION =
      new AttemptFailedTransition();
  private static final AMRegisteredTransition REGISTERED_TRANSITION =
      new AMRegisteredTransition();
  private static final AMLaunchedTransition LAUNCHED_TRANSITION =
      new AMLaunchedTransition();
  private RMAppAttemptEvent eventCausingFinalSaving;
  private RMAppAttemptState targetedFinalState;
  private RMAppAttemptState recoveredFinalState;
  private RMAppAttemptState stateBeforeFinalSaving;
  private Object transitionTodo;
  
  private RMAppAttemptMetrics attemptMetrics = null;
  private List<ResourceRequest> amReqs = null;
  private BlacklistManager blacklistedNodesForAM = null;

  private String amLaunchDiagnostics;

  // 状态机工厂，定义应用尝试所有可能的状态和状态转换规则
  private static final StateMachineFactory<RMAppAttemptImpl,
                                           RMAppAttemptState,
                                           RMAppAttemptEventType,
                                           RMAppAttemptEvent>
       stateMachineFactory  = new StateMachineFactory<RMAppAttemptImpl,
                                            RMAppAttemptState,
                                            RMAppAttemptEventType,
                                     RMAppAttemptEvent>(RMAppAttemptState.NEW)

       // Transitions from NEW State
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.SUBMITTED,
          RMAppAttemptEventType.START, new AttemptStartedTransition())
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.NEW, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.REGISTERED,
          new FinalSavingTransition(
            new UnexpectedAMRegisteredTransition(), RMAppAttemptState.FAILED))
      .addTransition( RMAppAttemptState.NEW,
          EnumSet.of(RMAppAttemptState.FINISHED, RMAppAttemptState.KILLED,
            RMAppAttemptState.FAILED, RMAppAttemptState.LAUNCHED),
          RMAppAttemptEventType.RECOVER, new AttemptRecoveredTransition())
          
      // Transitions from SUBMITTED state
      .addTransition(RMAppAttemptState.SUBMITTED, 
          EnumSet.of(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING,
                     RMAppAttemptState.SCHEDULED),
          RMAppAttemptEventType.ATTEMPT_ADDED,
          new ScheduleTransition())
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.SUBMITTED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.REGISTERED,
          new FinalSavingTransition(
            new UnexpectedAMRegisteredTransition(), RMAppAttemptState.FAILED))
          
       // Transitions from SCHEDULED State
      .addTransition(RMAppAttemptState.SCHEDULED,
          EnumSet.of(RMAppAttemptState.ALLOCATED_SAVING,
            RMAppAttemptState.SCHEDULED),
          RMAppAttemptEventType.CONTAINER_ALLOCATED,
          new AMContainerAllocatedTransition())
      .addTransition(RMAppAttemptState.SCHEDULED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.SCHEDULED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.SCHEDULED,
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new FinalSavingTransition(
            new AMContainerCrashedBeforeRunningTransition(),
            RMAppAttemptState.FAILED))

       // Transitions from ALLOCATED_SAVING State
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.ALLOCATED,
          RMAppAttemptEventType.ATTEMPT_NEW_SAVED, new AttemptStoredTransition())
          
       // App could be killed by the client. So need to handle this. 
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING, 
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.CONTAINER_FINISHED,
          new FinalSavingTransition(
            new AMContainerCrashedBeforeRunningTransition(), 
            RMAppAttemptState.FAILED))
      .addTransition(RMAppAttemptState.ALLOCATED_SAVING,
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))

       // Transitions from LAUNCHED_UNMANAGED_SAVING State
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          RMAppAttemptState.LAUNCHED,
          RMAppAttemptEventType.ATTEMPT_NEW_SAVED, 
          new UnmanagedAMAttemptSavedTransition())
      // attempt should not try to register in this state
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.REGISTERED,
          new FinalSavingTransition(
            new UnexpectedAMRegisteredTransition(), RMAppAttemptState.FAILED))
      // App could be killed by the client. So need to handle this. 
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING, 
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.KILL,
          new FinalSavingTransition(new BaseFinalTransition(
            RMAppAttemptState.KILLED), RMAppAttemptState.KILLED))
      .addTransition(RMAppAttemptState.LAUNCHED_UNMANAGED_SAVING,
          RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.FAIL,
          new FinalSavingTransition(FAILED_TRANSITION,
              RMAppAttemptState.FAILED))

       // Transitions from ALLOCATED State
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.LAUNCHED,
          RMAppAttemptEventType.LAUNCHED, LAUNCHED_TRANSITION)
      .addTransition(RMAppAttemptState.ALLOCATED, RMAppAttemptState.FINAL_SAVING,
          RMAppAttemptEventType.LA