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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceChildJVM;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapred.WrappedProgressSplitsBlock;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.MapAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.ReduceAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.Avataar;
import org.apache.hadoop.mapreduce.v2.api.records.Locality;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterTaskAbortEvent;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptFailEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptRecoverEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptTooManyFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptKilledEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestEvent;
import org.apache.hadoop.mapreduce.v2.app.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce任务尝试的核心实现类，维护任务尝试的状态机，处理各类事件，管理容器生命周期。
 * 作为ApplicationMaster中任务执行的核心单元，负责从请求容器到任务完成/失败清理的全流程管理。
 */
@SuppressWarnings({ "rawtypes" })
public abstract class TaskAttemptImpl implements
    org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt,
      EventHandler<TaskAttemptEvent> {

  @VisibleForTesting
  protected final static Map<TaskType, Resource> RESOURCE_REQUEST_CACHE
      = new HashMap<>();
  static final Counters EMPTY_COUNTERS = new Counters();
  private static final Logger LOG =
      LoggerFactory.getLogger(TaskAttemptImpl.class);
  private static final long MEMORY_SPLITS_RESOLUTION = 1024; //TODO Make configurable?
  private final static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  protected final JobConf conf;
  protected final Path jobFile;
  protected final int partition;
  protected EventHandler eventHandler;
  private final TaskAttemptId attemptId;
  private final Clock clock;
  private final org.apache.hadoop.mapred.JobID oldJobId;
  private final TaskAttemptListener taskAttemptListener;
  private Resource resourceCapability;
  protected Set<String> dataLocalHosts;
  protected Set<String> dataLocalRacks;
  private final List<String> diagnostics = new ArrayList<String>();
  private final Lock readLock;
  private final Lock writeLock;
  private final AppContext appContext;
  private Credentials credentials;
  private Token<JobTokenIdentifier> jobToken;
  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  private static String initialClasspath = null;
  private static String initialAppClasspath = null;
  private static Object commonContainerSpecLock = new Object();
  private static ContainerLaunchContext commonContainerSpec = null;
  private static final Object classpathLock = new Object();
  private long launchTime;
  private long finishTime;
  private WrappedProgressSplitsBlock progressSplitBlock;
  private int shufflePort = -1;
  private String trackerName;
  private int httpPort;
  private Locality locality;
  private Avataar avataar;
  private boolean rescheduleNextAttempt = false;
  private boolean failFast = false;

  private static final CleanupContainerTransition
      CLEANUP_CONTAINER_TRANSITION = new CleanupContainerTransition();
  private static final MoveContainerToSucceededFinishingTransition
      SUCCEEDED_FINISHING_TRANSITION =
          new MoveContainerToSucceededFinishingTransition();
  private static final MoveContainerToFailedFinishingTransition
      FAILED_FINISHING_TRANSITION =
          new MoveContainerToFailedFinishingTransition();
  private static final ExitFinishingOnTimeoutTransition
      FINISHING_ON_TIMEOUT_TRANSITION =
          new ExitFinishingOnTimeoutTransition();

  private static final FinalizeFailedTransition FINALIZE_FAILED_TRANSITION =
      new FinalizeFailedTransition();

  private static final DiagnosticInformationUpdater 
    DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION 
      = new DiagnosticInformationUpdater();

  private static final EnumSet<TaskAttemptEventType>
    FAILED_KILLED_STATE_IGNORED_EVENTS = EnumSet.of(
      TaskAttemptEventType.TA_KILL,
      TaskAttemptEventType.TA_ASSIGNED,
      TaskAttemptEventType.TA_CONTAINER_COMPLETED,
      TaskAttemptEventType.TA_UPDATE,
      // Container launch events can arrive late
      TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
      TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
      TaskAttemptEventType.TA_CONTAINER_CLEANED,
      TaskAttemptEventType.TA_COMMIT_PENDING,
      TaskAttemptEventType.TA_DONE,
      TaskAttemptEventType.TA_FAILMSG,
      TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
      TaskAttemptEventType.TA_TIMED_OUT,
      TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE);

  // 状态机工厂：定义任务尝试所有状态和状态转移规则
  private static final StateMachineFactory
        <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
        stateMachineFactory
    = new StateMachineFactory
             <TaskAttemptImpl, TaskAttemptStateInternal, TaskAttemptEventType, TaskAttemptEvent>
           (TaskAttemptStateInternal.NEW)

     // Transitions from the NEW state.
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptEventType.TA_SCHEDULE, new RequestContainerTransition(false))
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptEventType.TA_RESCHEDULE, new RequestContainerTransition(true))
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_KILL, new KilledTransition())
     .addTransition(TaskAttemptStateInternal.NEW, TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT, new FailedTransition())
     .addTransition(TaskAttemptStateInternal.NEW,
         EnumSet.of(TaskAttemptStateInternal.FAILED,
             TaskAttemptStateInternal.KILLED,
             TaskAttemptStateInternal.SUCCEEDED),
         TaskAttemptEventType.TA_RECOVER, new RecoverTransition())
     .addTransition(TaskAttemptStateInternal.NEW,
         TaskAttemptStateInternal.NEW,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)

     // Transitions from the UNASSIGNED state.
     .addTransition(TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptStateInternal.ASSIGNED, TaskAttemptEventType.TA_ASSIGNED,
         new ContainerAssignedTransition())
     .addTransition(TaskAttemptStateInternal.UNASSIGNED, TaskAttemptStateInternal.KILLED,
         TaskAttemptEventType.TA_KILL, new DeallocateContainerTransition(
         TaskAttemptStateInternal.KILLED, true))
     .addTransition(TaskAttemptStateInternal.UNASSIGNED, TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT, new DeallocateContainerTransition(
             TaskAttemptStateInternal.FAILED, true))
     .addTransition(TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptStateInternal.UNASSIGNED,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)

     // Transitions from the ASSIGNED state.
     .addTransition(TaskAttemptStateInternal.ASSIGNED, TaskAttemptStateInternal.RUNNING,
         TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
         new LaunchedContainerTransition())
     .addTransition(TaskAttemptStateInternal.ASSIGNED, TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED, TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
         new DeallocateContainerTransition(TaskAttemptStateInternal.FAILED, false))
     .addTransition(TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         FINALIZE_FAILED_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED, 
         TaskAttemptStateInternal.KILL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_KILL, CLEANUP_CONTAINER_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_FAILMSG, FAILED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.ASSIGNED,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT,
             CLEANUP_CONTAINER_TRANSITION)

     // Transitions from RUNNING state.
     .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING,
         TaskAttemptEventType.TA_UPDATE, new StatusUpdater())
     .addTransition(TaskAttemptStateInternal.RUNNING, TaskAttemptStateInternal.RUNNING,
         TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
         DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
     // If no commit is required, task goes to finishing state
     // This will give a chance for the container to exit by itself
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.SUCCESS_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_DONE, SUCCEEDED_FINISHING_TRANSITION)
     // If commit is required, task goes through commit pending state.
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.COMMIT_PENDING,
         TaskAttemptEventType.TA_COMMIT_PENDING, new CommitPendingTransition())
     // Failure handling while RUNNING
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_FINISHING_CONTAINER,
         TaskAttemptEventType.TA_FAILMSG, FAILED_FINISHING_TRANSITION)
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAIL_CONTAINER_CLEANUP,
         TaskAttemptEventType.TA_FAILMSG_BY_CLIENT, CLEANUP_CONTAINER_TRANSITION)
      //for handling container exit without sending the done or fail msg
     .addTransition(TaskAttemptStateInternal.RUNNING,
         TaskAttemptStateInternal.FAILED,
         TaskAttemptEventType.TA_CONTAINER_COMPLETED,
         FINALIZE_FAILED_TRANSITION)
     //