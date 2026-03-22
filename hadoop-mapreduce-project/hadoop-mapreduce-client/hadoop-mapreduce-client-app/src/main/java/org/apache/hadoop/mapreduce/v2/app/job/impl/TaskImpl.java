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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.jobhistory.TaskFailedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskStartedEvent;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.Avataar;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobMapTaskRescheduledEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptRecoverEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskRecoverEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptKilledEvent;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerFailedEvent;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Task接口的抽象基础实现，管理MapReduce作业中单个Task的生命周期、状态机和任务尝试。
 * 为具体Map/Reduce任务实现提供公共能力，核心职责包括：状态流转管理、任务尝试管理、推测执行支持、故障重试和任务恢复。
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class TaskImpl implements Task, EventHandler<TaskEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(TaskImpl.class);
  private static final String SPECULATION = "Speculation: ";

  protected final JobConf conf;
  protected final Path jobFile;
  protected final int partition;
  protected final TaskAttemptListener taskAttemptListener;
  protected final EventHandler eventHandler;
  private final TaskId taskId;
  private Map<TaskAttemptId, TaskAttempt> attempts;
  private final int maxAttempts;
  protected final Clock clock;
  private final Lock readLock;
  private final Lock writeLock;
  private final MRAppMetrics metrics;
  protected final AppContext appContext;
  private long scheduledTime;
  
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  protected boolean encryptedShuffle;
  protected Credentials credentials;
  protected Token<JobTokenIdentifier> jobToken;
  
  // 当前获得提交权限的尝试，优先选择最先进入提交阶段的尝试
  private TaskAttemptId commitAttempt;

  // 最终成功完成任务的尝试ID
  private TaskAttemptId successfulAttempt;

  private final Set<TaskAttemptId> failedAttempts;
  // 跟踪所有已完成的尝试：成功、失败、被杀死都计入
  private final Set<TaskAttemptId> finishedAttempts;
  // 统计正在运行或等待分配容器的尝试数量
  private final Set<TaskAttemptId> inProgressAttempts;

  private boolean historyTaskStartGenerated = false;
  // 任务启动时间，用于作业历史事件记录
  private long launchTime;
  private boolean speculationEnabled = false;

  private static final SingleArcTransition<TaskImpl, TaskEvent> 
     ATTEMPT_KILLED_TRANSITION = new AttemptKilledTransition();
  private static final SingleArcTransition<TaskImpl, TaskEvent> 
     KILL_TRANSITION = new KillTransition();

  /** 任务状态机定义，描述Task从创建到完成的所有状态和状态转移规则 */
  private static final StateMachineFactory
               <TaskImpl, TaskStateInternal, TaskEventType, TaskEvent> 
            stateMachineFactory 
           = new StateMachineFactory<TaskImpl, TaskStateInternal, TaskEventType, TaskEvent>
               (TaskStateInternal.NEW)

    // define the state machine of Task

    // Transitions from NEW state
    .addTransition(TaskStateInternal.NEW, TaskStateInternal.SCHEDULED, 
        TaskEventType.T_SCHEDULE, new InitialScheduleTransition())
    .addTransition(TaskStateInternal.NEW, TaskStateInternal.KILLED, 
        TaskEventType.T_KILL, new KillNewTransition())
    .addTransition(TaskStateInternal.NEW,
        EnumSet.of(TaskStateInternal.FAILED,
                   TaskStateInternal.KILLED,
                   TaskStateInternal.RUNNING,
                   TaskStateInternal.SUCCEEDED),
        TaskEventType.T_RECOVER, new RecoverTransition())

    // Transitions from SCHEDULED state
      //when the first attempt is launched, the task state is set to RUNNING
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.RUNNING, 
         TaskEventType.T_ATTEMPT_LAUNCHED, new LaunchTransition())
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.KILL_WAIT, 
         TaskEventType.T_KILL, KILL_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED, TaskStateInternal.SCHEDULED, 
         TaskEventType.T_ATTEMPT_KILLED, ATTEMPT_KILLED_TRANSITION)
     .addTransition(TaskStateInternal.SCHEDULED, 
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.FAILED), 
        TaskEventType.T_ATTEMPT_FAILED, 
        new AttemptFailedTransition())
 
    // Transitions from RUNNING state
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING, 
        TaskEventType.T_ATTEMPT_LAUNCHED) //more attempts may start later
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING, 
        TaskEventType.T_ATTEMPT_COMMIT_PENDING,
        new AttemptCommitPendingTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING,
        TaskEventType.T_ADD_SPEC_ATTEMPT, new RedundantScheduleTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.SUCCEEDED, 
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new AttemptSucceededTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.RUNNING, 
        TaskEventType.T_ATTEMPT_KILLED,
        ATTEMPT_KILLED_TRANSITION)
    .addTransition(TaskStateInternal.RUNNING, 
        EnumSet.of(TaskStateInternal.RUNNING, TaskStateInternal.FAILED), 
        TaskEventType.T_ATTEMPT_FAILED,
        new AttemptFailedTransition())
    .addTransition(TaskStateInternal.RUNNING, TaskStateInternal.KILL_WAIT, 
        TaskEventType.T_KILL, KILL_TRANSITION)

    // Transitions from KILL_WAIT state
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_KILLED,
        new KillWaitAttemptKilledTransition())
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new KillWaitAttemptSucceededTransition())
    .addTransition(TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskStateInternal.KILL_WAIT, TaskStateInternal.KILLED),
        TaskEventType.T_ATTEMPT_FAILED,
        new KillWaitAttemptFailedTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.KILL_WAIT,
        TaskStateInternal.KILL_WAIT,
        EnumSet.of(TaskEventType.T_KILL,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_ATTEMPT_COMMIT_PENDING,
            TaskEventType.T_ADD_SPEC_ATTEMPT))

    // Transitions from SUCCEEDED state
    .addTransition(TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED, TaskStateInternal.FAILED),
        TaskEventType.T_ATTEMPT_FAILED, new RetroactiveFailureTransition())
    .addTransition(TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskStateInternal.SCHEDULED, TaskStateInternal.SUCCEEDED),
        TaskEventType.T_ATTEMPT_KILLED, new RetroactiveKilledTransition())
    .addTransition(TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        TaskEventType.T_ATTEMPT_SUCCEEDED,
        new AttemptSucceededAtSucceededTransition())
    // Ignore-able transitions.
    .addTransition(
        TaskStateInternal.SUCCEEDED, TaskStateInternal.SUCCEEDED,
        EnumSet.of(TaskEventType.T_ADD_SPEC_ATTEMPT,
            TaskEventType.T_ATTEMPT_COMMIT_PENDING,
            TaskEventType.T_ATTEMPT_LAUNCHED,
            TaskEventType.T_KILL))

    // Transitions from FAILED state        
    .addTransition(TaskStateInternal.FAILED, TaskStateInternal.FAILED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_ADD_SPEC_ATTEMPT,
                   TaskEventType.T_ATTEMPT_COMMIT_PENDING,
                   TaskEventType.T_ATTEMPT_FAILED,
                   TaskEventType.T_ATTEMPT_KILLED,
                   TaskEventType.T_ATTEMPT_LAUNCHED,
                   TaskEventType.T_ATTEMPT_SUCCEEDED))

    // Transitions from KILLED state
    // There could be a race condition where TaskImpl might receive
    // T_ATTEMPT_SUCCEEDED followed by T_ATTEMPTED_KILLED for the same attempt.
    // a. The task is in KILL_WAIT.
    // b. Before TA transitions to SUCCEEDED state, Task sends TA_KILL event.
    // c. TA transitions to SUCCEEDED state and thus send T_ATTEMPT_SUCCEEDED
    //    to the task. The task transitions to KILLED state.
    // d. TA processes TA_KILL event and sends T_ATTEMPT_KILLED to the task.
    .addTransition(TaskStateInternal.KILLED, TaskStateInternal.KILLED,
        EnumSet.of(TaskEventType.T_KILL,
                   TaskEventType.T_SCHEDULE,
                   TaskEventType.T_ATTEMPT_KILLED,
                   TaskEventType.T_ADD_SPEC_ATTEMPT))

    // create the topology tables
    .installTopology();

  private final StateMachine<TaskStateInternal, TaskEventType, TaskEvent>
    stateMachine;

  // By default, the next TaskAttempt number is zero. Changes during recovery  
  protected int nextAttemptNumber = 0;

  // For sorting task attempts by completion time
  /** 按完成时间对任务尝试信息排序的比较器，用于恢复时保证事件顺序正确 */
  private static final Comparator<TaskAttemptInfo> TA_INFO_COMPARATOR =
      new Comparator<TaskAttemptInfo>() {
        @Override
        public int compare(TaskAttemptInfo a, TaskAttemptInfo b) {
          long diff = a.getFinishTime() - b.getFinishTime();
          return diff == 0 ? 0 : (diff < 0 ? -1 : 1);
        }
      };

  @Override
  public TaskState getState() {
    readLock.lock();
    try {
      return getExternalState(getInternalState());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 构造Task实例，初始化Task的基本属性、锁和状态机
   * @param jobId 所属作业ID
   * @param taskType Task类型（MAP/REDUCE）
   * @param partition 分区编号
   * @param eventHandler 事件处理器，用于向上层Job发送事件
   * @param remoteJobConfFile 远程作业配置文件路径
   * @param conf 作业配置对象
   * @param taskAttemptListener 任务尝试监听器
   * @param jobToken 作业认证令牌
   * @param credentials 凭证信息
   * @param clock 时钟对象，用于获取时间
   * @param appAttemptId 应用尝试ID
   * @param metrics MR应用 metrics 指标
   * @param appContext 应用上下文
   */
  public TaskImpl(JobId jobId, TaskType taskType, int partition,
      EventHandler eventHandler, Path remoteJobConfFile, JobConf conf,
      TaskAttemptListener taskAttemptListener,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      int appAttemptId, MRAppMetrics metrics, AppContext appContext) {
    this.conf = conf;
    this.clock = clock;
    this.jobFile = remoteJobConfFile;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    this.attempts = Collections.emptyMap();
    this.finishedAttempts = new HashSet<TaskAttemptId>(2);
    this.failedAttempts = new HashSet<TaskAttemptId>(2);
    this.inProgressAttempts = new HashSet<TaskAttemptId>(2);
    // 调用子类实现获取最大尝试次数，约定子类不依赖未初始化字段
    maxAttempts = getMaxAttempts();
    taskId = MRBuilderUtils.newTaskId(jobId, partition, taskType);
    this.partition = partition;
    this.taskAttemptListener = taskAttemptListener;
    this.eventHandler = eventHandler;
    this.credentials = credentials;
    this.jobToken = jobToken;
    this.metrics = metrics;
    this.appContext = appContext;
    // 读取Shuffle SSL加密配置
    this.encryptedShuffle = conf.getBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY,
                                            MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT);
    // 根据Task类型读取推测执行配置
    this.speculationEnabled = taskType.equals(TaskType.M