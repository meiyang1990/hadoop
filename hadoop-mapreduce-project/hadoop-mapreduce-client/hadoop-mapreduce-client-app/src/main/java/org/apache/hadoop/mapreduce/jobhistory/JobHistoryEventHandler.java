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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.util.JobHistoryEventUtils;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.jobhistory.FileNameIndexUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;

import org.apache.hadoop.classification.VisibleForTesting;
import javax.ws.rs.ProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 作业历史事件处理器，负责接收并持久化MapReduce作业运行过程中的各类事件，
 * 先写入临时 staging 目录，作业完成后移动到最终 done 目录，同时支持将事件发送给YARN Timeline服务。
 */
public class JobHistoryEventHandler extends AbstractService
    implements EventHandler<JobHistoryEvent> {

  /** 应用上下文，获取作业等核心信息 */
  private final AppContext context;
  /** AM启动计数，用于区分同一作业的多次重试 */
  private final int startCount;

  /** 事件计数器，用于定期打印事件队列大小日志 */
  private int eventCounter;

  // 这些文件系统可能与作业配置中的不同，详见JobHistoryUtils的说明
  /** 临时staging目录所在的文件系统 */
  private FileSystem stagingDirFS;
  /** 最终done目录所在的文件系统 */
  private FileSystem doneDirFS;

  /** 临时staging目录路径 */
  private Path stagingDirPath = null;
  /** 已完成作业的done目录前缀路径 */
  private Path doneDirPrefixPath = null;

  /** 触发flush前允许的最大未flush完成事件数量 */
  private int maxUnflushedCompletionEvents;
  /** 作业完成后未flush事件数量倍增系数，减少flush次数提升写入速度 */
  private int postJobCompletionMultiplier;
  /** 自动flush超时时间（毫秒） */
  private long flushTimeout;
  /** 批量flush触发的最小队列大小阈值 */
  private int minQueueSizeForBatchingFlushes;

  /** 当前未flush的完成事件计数 */
  private int numUnflushedCompletionEvents = 0;
  /** flush定时器是否激活 */
  private boolean isTimerActive;
  /** job历史文件写入格式 */
  private EventWriter.WriteMode jhistMode =
      EventWriter.WriteMode.JSON;

  /** 存储待处理作业历史事件的阻塞队列 */
  protected BlockingQueue<JobHistoryEvent> eventQueue =
    new LinkedBlockingQueue<JobHistoryEvent>();

  /** 是否需要将事件发送到Timeline服务 */
  protected boolean handleTimelineEvent = false;
  /** Timeline事件异步分发器 */
  protected AsyncDispatcher atsEventDispatcher = null;
  /** 处理事件的后台线程 */
  protected Thread eventHandlingThread;

  /** 服务停止标记 */
  private volatile boolean stopped;
  /** 同步锁，保护事件处理和停止流程 */
  private final Object lock = new Object();

  private static final Logger LOG = LoggerFactory.getLogger(
      JobHistoryEventHandler.class);

  /** 作业ID到元信息的映射，存储所有当前作业的写入相关信息 */
  protected static final Map<JobId, MetaInfo> fileMap =
    Collections.<JobId,MetaInfo>synchronizedMap(new HashMap<JobId,MetaInfo>());

  /** AM关闭时是否强制完成作业历史写入 */
  protected volatile boolean forceJobCompletion = false;

  @VisibleForTesting
  protected TimelineClient timelineClient;
  @VisibleForTesting
  protected TimelineV2Client timelineV2Client;

  private static String MAPREDUCE_JOB_ENTITY_TYPE = "MAPREDUCE_JOB";
  private static String MAPREDUCE_TASK_ENTITY_TYPE = "MAPREDUCE_TASK";
  private static final String MAPREDUCE_TASK_ATTEMPT_ENTITY_TYPE =
      "MAPREDUCE_TASK_ATTEMPT";

  /**
   * 构造作业历史事件处理器
   * @param context 应用上下文
   * @param startCount AM启动计数
   */
  public JobHistoryEventHandler(AppContext context, int startCount) {
    super("JobHistoryEventHandler");
    this.context = context;
    this.startCount = startCount;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.yarn.service.AbstractService#init(org.
   * apache.hadoop.conf.Configuration)
   * 初始化日志目录和done目录的文件系统与路径对象，若目录不存在则创建它们。
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    String jobId =
      TypeConverter.fromYarn(context.getApplicationID()).toString();
    
    String stagingDirStr = null;
    String doneDirStr = null;
    String userDoneDirStr = null;
    try {
      // 获取配置中的各历史目录路径
      stagingDirStr = JobHistoryUtils.getConfiguredHistoryStagingDirPrefix(conf,
          jobId);
      doneDirStr =
          JobHistoryUtils.getConfiguredHistoryIntermediateDoneDirPrefix(conf);
      userDoneDirStr =
          JobHistoryUtils.getHistoryIntermediateDoneDirForUser(conf);
    } catch (IOException e) {
      LOG.error("Failed while getting the configured log directories", e);
      throw new YarnRuntimeException(e);
    }

    // 检查历史staging目录是否存在，不存在则创建
    try {
      stagingDirPath =
          FileContext.getFileContext(conf).makeQualified(new Path(stagingDirStr));
      stagingDirFS = FileSystem.get(stagingDirPath.toUri(), conf);
      mkdir(stagingDirFS, stagingDirPath, new FsPermission(
          JobHistoryUtils.HISTORY_STAGING_DIR_PERMISSIONS));
    } catch (IOException e) {
      LOG.error("Failed while checking for/creating  history staging path: ["
          + stagingDirPath + "]", e);
      throw new YarnRuntimeException(e);
    }

    // 检查中间done目录是否存在
    Path doneDirPath = null;
    try {
      doneDirPath = FileContext.getFileContext(conf).makeQualified(new Path(doneDirStr));
      doneDirFS = FileSystem.get(doneDirPath.toUri(), conf);
      // 根据配置决定是否创建不存在的基础目录
      if (!doneDirFS.exists(doneDirPath)) {
      if (JobHistoryUtils.shouldCreateNonUserDirectory(conf)) {
        LOG.info("Creating intermediate history logDir: ["
            + doneDirPath
            + "] + based on conf. Should ideally be created by the JobHistoryServer: "
            + MRJobConfig.MR_AM_CREATE_JH_INTERMEDIATE_BASE_DIR);
          mkdir(
              doneDirFS,
              doneDirPath,
              new FsPermission(
            JobHistoryUtils.HISTORY_INTERMEDIATE_DONE_DIR_PERMISSIONS
                .toShort()));
          // TODO Temporary toShort till new FsPermission(FsPermissions) respects sticky
      } else {
          String message = "Not creating intermediate history logDir: ["
                + doneDirPath
                + "] based on conf: "
                + MRJobConfig.MR_AM_CREATE_JH_INTERMEDIATE_BASE_DIR
                + ". Either set to true or pre-create this directory with" +
                " appropriate permissions";
        LOG.error(message);
        throw new YarnRuntimeException(message);
      }
      }
    } catch (IOException e) {
      LOG.error("Failed checking for the existence of history intermediate " +
      		"done directory: [" + doneDirPath + "]");
      throw new YarnRuntimeException(e);
    }

    // 检查/创建用户目录（位于中间done目录下）
    try {
      doneDirPrefixPath =
          FileContext.getFileContext(conf).makeQualified(new Path(userDoneDirStr));
      mkdir(doneDirFS, doneDirPrefixPath, JobHistoryUtils.
          getConfiguredHistoryIntermediateUserDoneDirPermissions(conf));
    } catch (IOException e) {
      LOG.error("Error creating user intermediate history done directory: [ "
          + doneDirPrefixPath + "]", e);
      throw new YarnRuntimeException(e);
    }

    // 从配置加载flush相关参数
    maxUnflushedCompletionEvents =
        conf.getInt(MRJobConfig.MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS,
            MRJobConfig.DEFAULT_MR_AM_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS);
    postJobCompletionMultiplier =
        conf.getInt(
            MRJobConfig.MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER,
            MRJobConfig.DEFAULT_MR_AM_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER);
    flushTimeout =
        conf.getLong(MRJobConfig.MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
            MRJobConfig.DEFAULT_MR_AM_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS);
    minQueueSizeForBatchingFlushes =
        conf.getInt(
            MRJobConfig.MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD,
            MRJobConfig.DEFAULT_MR_AM_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD);

    // 判断是否启用Timeline服务，并初始化对应客户端
    if (conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_EMIT_TIMELINE_DATA,
        MRJobConfig.DEFAULT_MAPREDUCE_JOB_EMIT_TIMELINE_DATA)) {
      LOG.info("Emitting job history data to the timeline service is enabled");
      if (YarnConfiguration.timelineServiceEnabled(conf)) {
        boolean timelineServiceV2Enabled =
            YarnConfiguration.timelineServiceV2Enabled(conf);
        if(timelineServiceV2Enabled) {
          timelineV2Client =
              ((MRAppMaster.RunningAppContext)context).getTimelineV2Client();
          timelineV2Client.init(conf);
        } else {
          timelineClient =
              ((MRAppMaster.RunningAppContext) context).getTimelineClient();
          timelineClient.init(conf);
        }
        handleTimelineEvent = true;
        LOG.info("Timeline service is enabled; version: " +
            YarnConfiguration.getTimelineServiceVersion(conf));
      } else {
        LOG.info("Timeline service is not enabled");
      }
    } else {
      LOG.info("Emitting job history data to the timeline server is not " +
          "enabled");
    }

    // 从配置读取并确定jhist文件格式
    String jhistFormat = conf.get(JHAdminConfig.MR_HS_JHIST_FORMAT,
        JHAdminConfig.DEFAULT_MR_HS_JHIST_FORMAT);
    if (jhistFormat.equals("json")) {
      jhistMode = EventWriter.WriteMode.JSON;
    } else if (jhistFormat.equals("binary")) {
      jhistMode = EventWriter.WriteMode.BINARY;
    } else {
      LOG.warn("Unrecognized value '" + jhistFormat + "' for property " +
          JHAdminConfig.MR_HS_JHIST_FORMAT + ".  Valid values are " +
          "'json' or 'binary'.  Falling back to default value '" +
          JHAdminConfig.DEFAULT_MR_HS_JHIST_FORMAT + "'.");
    }
    // 若启用Timeline服务，初始化异步事件分发器
    if (handleTimelineEvent) {
      atsEventDispatcher = createDispatcher();
      EventHandler<JobHistoryEvent> timelineEventHandler =
          new ForwardingEventHandler();
      atsEventDispatcher.register(EventType.class, timelineEventHandler);
      atsEventDispatcher.setDrainEventsOnStop();
      atsEventDispatcher.init(conf);
    }
    super.serviceInit(conf);
  }

  /**
   * 创建Timeline事件异步分发器
   * @return 新建的AsyncDispatcher实例
   */
  protected AsyncDispatcher createDispatcher() {
    return new AsyncDispatcher("Job ATS Event Dispatcher");
  }

  /**
   * 创建目录并设置正确权限
   * @param fs 文件系统
   * @param path 目录路径
   * @param fsp 期望权限
   * @throws IOException 创建目录或设置权限失败时抛出
   */
  private void mkdir(FileSystem fs, Path path, FsPermission fsp)
      throws IOException {
    if (!fs.exists(path)) {
      try {
        fs.mkdirs(path, fsp);
        FileStatus fsStatus = fs.getFileStatus(path);
        LOG.info("Perms after creating " + fsStatus.getPermission().toShort()
            + ", Expected: " + fsp.toShort());
        if (fsStatus.getPermission().toShort() != fsp.toShort()) {
          LOG.info("Explicitly setting permissions to : " + fsp.toShort()
              + ", " + fsp);
          fs.setPermission(path, fsp);
        }
      } catch (FileAlreadyExistsException e) {
        LOG.info("Directory: [" + path + "] already exists.");
      }
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    // 启动Timeline客户端
    if (timelineClient != null) {
      timelineClient.start();
    } else if (timelineV2Client != null) {
      timelineV2Client.start();
    }
    // 创建并启动事件处理后台线程
    eventHandlingThread = new SubjectInheritingThread(new Runnable() {
      @Override
      public void run() {
        JobHistoryEvent event = null;
        while (!stopped && !Thread.currentThread().isInterrupted()) {

          // 每处理1000个事件打印一次事件队列大小日志
          if (eventCounter != 0 && eventCounter % 1000 == 0) {
            eventCounter = 0;
            LOG.info("Size of the JobHistory event queue is "
                + eventQueue.size());
          } else {
            eventCounter++;
          }

          try {
            // 从队列阻塞获取下一个事件
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.info("EventQueue take interrupted. Returning");
            return;
          }
          // 处理取出的事件，处理完成前不允许中断
          synchronized (lock) {