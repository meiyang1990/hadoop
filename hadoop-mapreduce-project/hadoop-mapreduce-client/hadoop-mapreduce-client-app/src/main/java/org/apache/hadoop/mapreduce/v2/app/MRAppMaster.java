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

package org.apache.hadoop.mapreduce.v2.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.KeyGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalContainerLauncher;
import org.apache.hadoop.mapred.TaskAttemptListenerImpl;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.AMStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.EventReader;
import org.apache.hadoop.mapreduce.jobhistory.EventType;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryCopyService;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.client.MRClientService;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEvent;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventHandler;
import org.apache.hadoop.mapreduce.v2.app.commit.CommitterEventType;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobFinishEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobStartEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncher;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherEvent;
import org.apache.hadoop.mapreduce.v2.app.launcher.ContainerLauncherImpl;
import org.apache.hadoop.mapreduce.v2.app.local.LocalContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMCommunicator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor;
import org.apache.hadoop.mapreduce.v2.app.rm.RMHeartbeatHandler;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.NoopAMPreemptionPolicy;
import org.apache.hadoop.mapreduce.v2.app.speculate.DefaultSpeculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.Speculator;
import org.apache.hadoop.mapreduce.v2.app.speculate.SpeculatorEvent;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：MapReduce ApplicationMaster核心实现类，负责单个MapReduce作业的整个生命周期管理
 * 
 * MR AppMaster是YARN上MapReduce作业的应用 master，整体采用基于事件驱动的Actor模型设计：
 * 1. 状态机封装在Job实现类中，所有状态变更通过事件驱动完成
 * 2. 各个松耦合服务通过事件总线Dispatcher交互，降低模块耦合，支持高并发
 * 3. 使用AppContext在不同组件间共享全局状态信息
 */
@SuppressWarnings("rawtypes")
public class MRAppMaster extends CompositeService {

  private static final Logger LOG = LoggerFactory.getLogger(MRAppMaster.class);

  /**
   * MRAppMaster关闭钩子优先级，确保在合适的时机执行关闭逻辑
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  public static final String INTERMEDIATE_DATA_ENCRYPTION_ALGO = "HmacSHA1";

  private Clock clock;
  private final long startTime;
  private final long appSubmitTime;
  private String appName;
  private final ApplicationAttemptId appAttemptID;
  private final ContainerId containerID;
  private final String nmHost;
  private final int nmPort;
  private final int nmHttpPort;
  protected final MRAppMetrics metrics;
  private Map<TaskId, TaskInfo> completedTasksFromPreviousRun;
  private List<AMInfo> amInfos;
  private AppContext context;
  private Dispatcher dispatcher;
  private ClientService clientService;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private EventHandler<CommitterEvent> committerEventHandler;
  private Speculator speculator;
  protected TaskAttemptListener taskAttemptListener;
  protected JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private JobId jobId;
  private boolean newApiCommitter;
  private ClassLoader jobClassLoader;
  private OutputCommitter committer;
  private JobEventDispatcher jobEventDispatcher;
  private JobHistoryEventHandler jobHistoryEventHandler;
  private SpeculatorEventDispatcher speculatorEventDispatcher;
  private AMPreemptionPolicy preemptionPolicy;
  private byte[] encryptedSpillKey;

  /**
   * 任务尝试完成后会进入finishing状态，该监控器负责对超时未完成的尝试触发超时事件
   */
  private TaskAttemptFinishingMonitor taskAttemptFinishingMonitor;

  private Job job;
  private Credentials jobCredentials = new Credentials(); // Filled during init
  protected UserGroupInformation currentUser; // Will be setup during init

  @VisibleForTesting
  protected volatile boolean isLastAMRetry = false;
  // 初始化时遇到错误需要立即关闭
  boolean errorHappenedShutDown = false;
  private String shutDownMessage = null;
  JobStateInternal forcedState = null;
  private final ScheduledExecutorService logSyncer;

  private long recoveredJobStartTime = -1L;
  private static boolean mainStarted = false;

  @VisibleForTesting
  protected AtomicBoolean successfullyUnregistered =
      new AtomicBoolean(false);

  /**
   * 构造MRAppMaster，使用系统时钟
   * @param applicationAttemptId YARN应用尝试ID
   * @param containerId 当前AM运行的容器ID
   * @param nmHost 当前AM所在NodeManager主机地址
   * @param nmPort 当前AM所在NodeManager端口
   * @param nmHttpPort 当前AM所在NodeManager HTTP端口
   * @param appSubmitTime 作业提交时间
   */
  public MRAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      long appSubmitTime) {
    this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
        SystemClock.getInstance(), appSubmitTime);
  }

  /**
   * 构造MRAppMaster，允许自定义时钟用于测试
   * @param applicationAttemptId YARN应用尝试ID
   * @param containerId 当前AM运行的容器ID
   * @param nmHost 当前AM所在NodeManager主机地址
   * @param nmPort 当前AM所在NodeManager端口
   * @param nmHttpPort 当前AM所在NodeManager HTTP端口
   * @param clock 时钟对象
   * @param appSubmitTime 作业提交时间
   */
  public MRAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime) {
    super(MRAppMaster.class.getName());
    this.clock = clock;
    this.startTime = clock.getTime();
    this.appSubmitTime = appSubmitTime;
    this.appAttemptID = applicationAttemptId;
    this.containerID = containerId;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;
    this.metrics = MRAppMetrics.create();
    logSyncer = TaskLog.createLogSyncer();
    LOG.info("Created MRAppMaster for application " + applicationAttemptId);
  }

  /**
   * 创建任务尝试finishing状态超时监控器
   * @param eventHandler 事件处理器，用于发送超时事件
   * @return 新建的监控器实例
   */
  protected TaskAttemptFinishingMonitor createTaskAttemptFinishingMonitor(
      EventHandler eventHandler) {
    TaskAttemptFinishingMonitor monitor =
        new TaskAttemptFinishingMonitor(eventHandler);
    return monitor;
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    // 如果启用了作业类加载器，创建用户作业的类加载器
    createJobClassLoader(conf);

    // 初始化作业凭证和用户UGI
    initJobCredentialsAndUGI(conf);

    // 创建中央事件分发器
    dispatcher = createDispatcher();
    addIfService(dispatcher);
    taskAttemptFinishingMonitor = createTaskAttemptFinishingMonitor(dispatcher.getEventHandler());
    addIfService(taskAttemptFinishingMonitor);
    context = new RunningAppContext(conf, taskAttemptFinishingMonitor);

    // 作业名称目前和应用名称一致，未来支持DAG作业后会扩展
    appName = conf.get(MRJobConfig.JOB_NAME, "<missing app name>");

    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, appAttemptID.getAttemptId());
     
    newApiCommitter = false;
    jobId = MRBuilderUtils.newJobId(appAttemptID.getApplicationId(),
        appAttemptID.getApplicationId().getId());
    int numReduceTasks = conf.getInt(MRJobConfig.NUM_REDUCES, 0);
    // 判断是否使用新API的输出提交器
    if ((numReduceTasks > 0 && 
        conf.getBoolean("mapred.reducer.new-api", false)) ||
          (numReduceTasks == 0 && 
           conf.getBoolean("mapred.mapper.new-api", false)))  {
      newApiCommitter = true;
      LOG.info("Using mapred newApiCommitter.");
    }
    
    boolean copyHistory = false;
    committer = createOutputCommitter(conf);
    try {
      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      Path stagingDir = MRApps.getStagingAreaDir(conf, user);
      FileSystem fs = getFileSystem(conf);

      boolean stagingExists = fs.exists(stagingDir);
      Path startCommitFile = MRApps.getStartJobCommitFile(conf, user, jobId);
      boolean commitStarted = fs.exists(startCommitFile);
      Path endCommitSuccessFile = MRApps.getEndJobCommitSuccessFile(conf, user, jobId);
      boolean commitSuccess = fs.exists(endCommitSuccessFile);
      Path endCommitFailureFile = MRApps.getEndJobCommitFailureFile(conf, user, jobId);
      boolean commitFailure = fs.exists(endCommitFailureFile);
      //  staging目录不存在，直接标记为错误并关闭
      if(!stagingExists) {
        isLastAMRetry = true;
        LOG.info("Attempt num: " + appAttemptID.getAttemptId() +
            " is last retry: " + isLastAMRetry +
            " because the staging dir doesn't exist.");
        errorHappenedShutDown = true;
        forcedState = JobStateInternal.ERROR;
        shutDownMessage = "Staging dir does not exist " + stagingDir;
        LOG.error(shutDownMessage);
      } else if (commitStarted) {
        // 之前的尝试已经开始提交，这是最后一次重试，根据提交状态确定最终结果
        errorHappenedShutDown = true;
        isLastAMRetry = true;
        LOG.info("Attempt num: " + appAttemptID.getAttemptId() +
            " is last retry: " + isLastAMRetry +
            " because a commit was started.");
        copyHistory = true;
        if (commitSuccess) {
          shutDownMessage =
              "Job commit succeeded in a prior MRAppMaster attempt " +
              "before it crashed. Recovering.";
          forcedState = JobStateInternal.SUCCEEDED;
        } else if (commitFailure) {
          shutDownMessage =
              "Job commit failed in a prior MRAppMaster attempt " +
              "before it crashed. Not retrying.";
          forcedState = JobStateInternal.FAILED;
        } else {
          if (isCommitJobRepeatable()) {
            // 输出提交器支持可重复提交，清理之前未