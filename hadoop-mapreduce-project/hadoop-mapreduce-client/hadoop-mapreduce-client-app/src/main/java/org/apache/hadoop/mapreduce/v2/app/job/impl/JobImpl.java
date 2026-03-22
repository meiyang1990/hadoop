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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobInfoChangeEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobInitedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobQueueChangeEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobSubmittedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterJobAbortEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterJobCommitEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterJobSetupEvent;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobAbortCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCommitFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobSetupFailedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobStartEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptCompletedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobTaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptTooManyFetchFailureEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskRecoverEvent;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce作业核心实现类，维护作业状态机，使用读写锁保证并发访问安全。
 * 负责管理作业全生命周期：从初始化、任务调度、状态转换到最终完成/失败的整个流程。
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class JobImpl implements org.apache.hadoop.mapreduce.v2.app.job.Job, 
  EventHandler<JobEvent> {

  private static final TaskAttemptCompletionEvent[]
    EMPTY_TASK_ATTEMPT_COMPLETION_EVENTS = new TaskAttemptCompletionEvent[0];

  private static final TaskCompletionEvent[]
    EMPTY_TASK_COMPLETION_EVENTS = TaskCompletionEvent.EMPTY_ARRAY;

  private static final Logger LOG = LoggerFactory.getLogger(JobImpl.class);

  //Map任务允许的最大抓取失败率阈值
  private float maxAllowedFetchFailuresFraction;
  
  //抓取失败通知超过该次数后，判定Map任务失败
  private int maxFetchFailuresNotifications;

  public static final String JOB_KILLED_DIAG =
      "Job received Kill while in RUNNING state.";
  
  //final fields
  private final ApplicationAttemptId applicationAttemptId;
  private final Clock clock;
  private final JobACLsManager aclsManager;
  private final String reporterUserName;
  private final Map<JobACL, AccessControlList> jobACLs;
  //各阶段进度权重：作业设置阶段
  private float setupWeight = 0.05f;
  //各阶段进度权重：清理阶段
  private float cleanupWeight = 0.05f;
  //各阶段进度权重：Map阶段
  private float mapWeight = 0.0f;
  //各阶段进度权重：Reduce阶段
  private float reduceWeight = 0.0f;
  //上一次运行完成的任务信息（用于作业恢复）
  private final Map<TaskId, TaskInfo> completedTasksFromPreviousRun;
  //应用Master信息列表
  private final List<AMInfo> amInfos;
  //读写锁-读锁
  private final Lock readLock;
  //读写锁-写锁
  private final Lock writeLock;
  //作业ID
  private final JobId jobId;
  //作业名称
  private final String jobName;
  //输出提交器
  private final OutputCommitter committer;
  //是否使用新API的输出提交器
  private final boolean newApiCommitter;
  //向后兼容的旧版JobID
  private final org.apache.hadoop.mapreduce.JobID oldJobId;
  //任务尝试监听器
  private final TaskAttemptListener taskAttemptListener;
  //任务集合同步锁对象
  private final Object tasksSyncHandle = new Object();
  //所有Map任务ID集合
  private final Set<TaskId> mapTasks = new LinkedHashSet<TaskId>();
  //所有Reduce任务ID集合
  private final Set<TaskId> reduceTasks = new LinkedHashSet<TaskId>();
  /**
   * 记录每个节点上运行成功的任务尝试，节点不可用时用于重发失败任务
   */
  private final HashMap<NodeId, List<TaskAttemptId>> 
    nodesToSucceededTaskAttempts = new HashMap<NodeId, List<TaskAttemptId>>();

  //全局事件处理器
  private final EventHandler eventHandler;
  //MR应用指标
  private final MRAppMetrics metrics;
  //作业提交用户名
  private final String userName;
  //作业队列名称
  private String queueName;
  //应用提交时间
  private final long appSubmitTime;
  //应用上下文
  private final AppContext appContext;

  //标记是否需要复制任务集合（支持并发迭代）
  private boolean lazyTasksCopyNeeded = false;
  //所有任务集合，key为任务ID，value为任务实例
  volatile Map<TaskId, Task> tasks = new LinkedHashMap<TaskId, Task>();
  //作业计数器
  private Counters jobCounters = new Counters();
  //完整计数器锁对象
  private Object fullCountersLock = new Object();
  //作业完成后聚合的完整计数器
  private Counters fullCounters = null;
  //聚合后所有Map任务计数器
  private Counters finalMapCounters = null;
  //聚合后所有Reduce任务计数器
  private Counters finalReduceCounters = null;

    // FIXME:  
    //
    // Can then replace task-level uber counters (MR-2424) with job-level ones
    // sent from LocalContainerLauncher, and eventually including a count of
    // of uber-AM attempts (probably sent from MRAppMaster).
  public JobConf conf;

  //fields initialized in init
  private FileSystem fs;
  //远程作业提交目录
  private Path remoteJobSubmitDir;
  public Path remoteJobConfFile;
  //作业上下文
  private JobContext jobContext;
  //允许Map任务失败百分比阈值
  private int allowedMapFailuresPercent = 0;
  //允许Reduce任务失败百分比阈值
  private int allowedReduceFailuresPercent = 0;
  //任务尝试完成事件列表
  private List<TaskAttemptCompletionEvent> taskAttemptCompletionEvents;
  //Map尝试完成事件列表（兼容旧API）
  private List<TaskCompletionEvent> mapAttemptCompletionEvents;
  //任务完成事件索引到Map完成事件索引的映射
  private List<Integer> taskCompletionIdxToMapCompletionIdx;
  //作业诊断信息列表
  private final List<String> diagnostics = new ArrayList<String>();
  
  //任务/尝试相关数据结构
  //任务ID对应成功完成尝试的完成事件编号
  private final Map<TaskId, Integer> successAttemptCompletionEventNoMap = 
    new HashMap<TaskId, Integer>();
  //Map任务尝试抓取失败次数统计
  private final Map<TaskAttemptId, Integer> fetchFailuresMapping = 
    new HashMap<TaskAttemptId, Integer>();

  //状态机转换实例：诊断信息更新
  private static final DiagnosticsUpdateTransition
      DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  //状态机转换实例：内部错误
  private static final InternalErrorTransition
      INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  //状态机转换实例：AM重启
  private static final InternalRebootTransition
      INTERNAL_REBOOT_TRANSITION = new InternalRebootTransition();
  //状态机转换实例：任务尝试完成
  private static final TaskAttemptCompletedEventTransition
      TASK_ATTEMPT_COMPLETED_EVENT_TRANSITION =
          new TaskAttemptCompletedEventTransition();
  //状态机转换实例：计数器更新
  private static final CounterUpdateTransition COUNTER_UPDATE_TRANSITION =
      new CounterUpdateTransition();
  //状态机转换实例：节点更新
  private static final UpdatedNodesTransition UPDATED_NODES_TRANSITION =
      new UpdatedNodesTransition();

  /**
   * 作业状态机定义，定义所有状态和事件的转换规则
   */
  protected static final
    StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent> 
       stateMachineFactory
     = new StateMachineFactory<JobImpl, JobStateInternal, JobEventType, JobEvent>
              (JobStateInternal.NEW)

          // Transitions from NEW state
          .addTransition(JobStateInternal.NEW, JobStateInternal.NEW,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.NEW, JobStateInternal.NEW,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition
              (JobStateInternal.NEW,
              EnumSet.of(JobStateInternal.INITED, JobStateInternal.NEW),
              JobEventType.JOB_INIT,
              new InitTransition())
          .addTransition(JobStateInternal.NEW, JobStateInternal.FAIL_ABORT,
              JobEventType.JOB_INIT_FAILED,
              new InitFailedTransition())
          .addTransition(JobStateInternal.NEW, JobStateInternal.KILLED,
              JobEventType.JOB_KILL,
              new KillNewJobTransition())
          .addTransition(JobStateInternal.NEW, JobStateInternal.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(JobStateInternal.NEW, JobStateInternal.REBOOT,
              JobEventType.JOB_AM_REBOOT,
              INTERNAL_REBOOT_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.NEW, JobStateInternal.NEW,
              JobEventType.JOB_UPDATED_NODES)

          // Transitions from INITED state
          .addTransition(JobStateInternal.INITED, JobStateInternal.INITED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.INITED, JobStateInternal.INITED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.INITED, JobStateInternal.SETUP,
              JobEventType.JOB_START,
              new StartTransition())
          .addTransition(JobStateInternal.INITED, JobStateInternal.KILLED,
              JobEventType.JOB_KILL,
              new KillInitedJobTransition())
          .addTransition(JobStateInternal.INITED, JobStateInternal.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          .addTransition(JobStateInternal.INITED, JobStateInternal.REBOOT,
              JobEventType.JOB_AM_REBOOT,
              INTERNAL_REBOOT_TRANSITION)
          // Ignore-able events
          .addTransition(JobStateInternal.INITED, JobStateInternal.INITED,
              JobEventType.JOB_UPDATED_NODES)

          // Transitions from SETUP state
          .addTransition(JobStateInternal.SETUP, JobStateInternal.SETUP,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.SETUP, JobStateInternal.SETUP,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobStateInternal.SETUP, JobStateInternal.RUNNING,
              JobEventType.JOB_SETUP_COMPLETED,
              new SetupCompletedTransition())
          .addTransition(JobStateInternal.SETUP, JobStateInternal.FAIL_ABORT,
              JobEventType.JOB_SETUP_FAILED,
              new SetupFailedTransition())
          .addTransition(JobStateInternal.SETUP, JobStateInternal.KILL_ABORT,
              JobEventType.JOB_KILL,