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

package org.apache.hadoop.mapreduce.v2.hs;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobACLsManager;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.hs.HistoryFileManager.HistoryFileInfo;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 已完成作业的历史数据视图，在作业历史服务器中承载已完成作业的所有数据访问能力。
 * 采用延迟加载策略：仅预先加载作业级别基础数据，任务级别数据仅在请求时才从历史文件加载。
 */
public class CompletedJob implements org.apache.hadoop.mapreduce.v2.app.job.Job {
  // 向后兼容性说明：如果失败或被杀死的map/reduce计数为-1，表示该值未被记录，按0处理
  private static final int UNDEFINED_VALUE = -1;

  private static final Logger LOG = LoggerFactory.getLogger(CompletedJob.class);
  private final Configuration conf;
  private final JobId jobId;
  private final String user;
  private final HistoryFileInfo info;
  private JobInfo jobInfo;
  private JobReport report;
  AtomicBoolean tasksLoaded = new AtomicBoolean(false);
  private Lock tasksLock = new ReentrantLock();
  private Map<TaskId, Task> tasks = new HashMap<TaskId, Task>();
  private Map<TaskId, Task> mapTasks = new HashMap<TaskId, Task>();
  private Map<TaskId, Task> reduceTasks = new HashMap<TaskId, Task>();
  private List<TaskAttemptCompletionEvent> completionEvents = null;
  private List<TaskAttemptCompletionEvent> mapCompletionEvents = null;
  private JobACLsManager aclsMgr;
  
  
  /**
   * 构造已完成作业对象，加载作业基础信息并根据参数决定是否预加载所有任务数据。
   * @param conf Hadoop配置对象
   * @param jobId 作业ID
   * @param historyFile 作业历史文件路径
   * @param loadTasks 是否预加载所有任务数据
   * @param userName 提交作业的用户名
   * @param info 历史文件元信息对象
   * @param aclsMgr 作业访问权限管理器
   * @throws IOException 加载历史文件失败时抛出异常
   */
  public CompletedJob(Configuration conf, JobId jobId, Path historyFile, 
      boolean loadTasks, String userName, HistoryFileInfo info,
      JobACLsManager aclsMgr) 
          throws IOException {
    LOG.info("Loading job: " + jobId + " from file: " + historyFile);
    this.conf = conf;
    this.jobId = jobId;
    this.user = userName;
    this.info = info;
    this.aclsMgr = aclsMgr;
    loadFullHistoryData(loadTasks, historyFile);
  }

  @Override
  public int getCompletedMaps() {
    int killedMaps = (int) jobInfo.getKilledMaps();
    int failedMaps = (int) jobInfo.getFailedMaps();

    if (killedMaps == UNDEFINED_VALUE) {
      killedMaps = 0;
    }

    if (failedMaps == UNDEFINED_VALUE) {
      failedMaps = 0;
    }

    return (int) (jobInfo.getSucceededMaps() +
        killedMaps + failedMaps);
  }

  @Override
  public int getCompletedReduces() {
    int killedReduces = (int) jobInfo.getKilledReduces();
    int failedReduces = (int) jobInfo.getFailedReduces();

    if (killedReduces == UNDEFINED_VALUE) {
      killedReduces = 0;
    }

    if (failedReduces == UNDEFINED_VALUE) {
      failedReduces = 0;
    }

    return (int) (jobInfo.getSucceededReduces() +
        killedReduces + failedReduces);
  }

  @Override
  public Counters getAllCounters() {
    return jobInfo.getTotalCounters();
  }

  @Override
  public JobId getID() {
    return jobId;
  }

  @Override
  public synchronized JobReport getReport() {
    if (report == null) {
      constructJobReport();
    }
    return report;
  }

  /**
   * 从已解析的作业信息构造JobReport对象，用于对外提供作业状态摘要。
   */
  private void constructJobReport() {
    report = Records.newRecord(JobReport.class);
    report.setJobId(jobId);
    // 设置作业状态，从历史文件存储的状态字符串转换
    report.setJobState(JobState.valueOf(jobInfo.getJobStatus()));
    report.setSubmitTime(jobInfo.getSubmitTime());
    report.setStartTime(jobInfo.getLaunchTime());
    report.setFinishTime(jobInfo.getFinishTime());
    report.setJobName(jobInfo.getJobname());
    report.setUser(jobInfo.getUsername());
    report.setDiagnostics(jobInfo.getErrorInfo());

    // 计算Map阶段进度，没有Map任务时进度为100%
    if ( getTotalMaps() == 0 ) {
      report.setMapProgress(1.0f);
    } else {
      report.setMapProgress((float) getCompletedMaps() / getTotalMaps());
    }
    // 计算Reduce阶段进度，没有Reduce任务时进度为100%
    if ( getTotalReduces() == 0 ) {
      report.setReduceProgress(1.0f);
    } else {
      report.setReduceProgress((float) getCompletedReduces() / getTotalReduces());
    }

    report.setJobFile(getConfFile().toString());
    String historyUrl = "N/A";
    try {
      // 生成作业历史服务器上的Web访问URL
      historyUrl =
          MRWebAppUtil.getApplicationWebURLOnJHSWithScheme(conf,
              jobId.getAppId());
    } catch (UnknownHostException e) {
        LOG.error("Problem determining local host: " + e.getMessage());
    }
    report.setTrackingUrl(historyUrl);
    report.setAMInfos(getAMInfos());
    report.setIsUber(isUber());
    report.setHistoryFile(info.getHistoryFile().toString());
  }

  @Override
  public float getProgress() {
    // 已完成作业进度始终为100%
    return 1.0f;
  }

  @Override
  public JobState getState() {
    return JobState.valueOf(jobInfo.getJobStatus());
  }

  @Override
  public Task getTask(TaskId taskId) {
    if (tasksLoaded.get()) {
      // 任务已加载，直接从缓存返回
      return tasks.get(taskId);
    } else {
      // 任务未加载，延迟创建单个任务对象返回
      TaskID oldTaskId = TypeConverter.fromYarn(taskId);
      CompletedTask completedTask =
          new CompletedTask(taskId, jobInfo.getAllTasks().get(oldTaskId));
      return completedTask;
    }
  }

  @Override
  public synchronized TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    if (completionEvents == null) {
      constructTaskAttemptCompletionEvents();
    }
    return getAttemptCompletionEvents(completionEvents,
        fromEventId, maxEvents);
  }

  @Override
  public synchronized TaskCompletionEvent[] getMapAttemptCompletionEvents(
      int startIndex, int maxEvents) {
    if (mapCompletionEvents == null) {
      constructTaskAttemptCompletionEvents();
    }
    return TypeConverter.fromYarn(getAttemptCompletionEvents(
        mapCompletionEvents, startIndex, maxEvents));
  }

  /**
   * 从事件列表中截取指定范围的任务尝试完成事件返回。
   * @param eventList 完整事件列表
   * @param startIndex 起始事件索引
   * @param maxEvents 最大返回事件数
   * @return 截取后的事件数组
   */
  private static TaskAttemptCompletionEvent[] getAttemptCompletionEvents(
      List<TaskAttemptCompletionEvent> eventList,
      int startIndex, int maxEvents) {
    TaskAttemptCompletionEvent[] events = new TaskAttemptCompletionEvent[0];
    if (eventList.size() > startIndex) {
      // 计算实际可返回的最大事件数
      int actualMax = Math.min(maxEvents,
          (eventList.size() - startIndex));
      events = eventList.subList(startIndex, actualMax + startIndex)
          .toArray(events);
    }
    return events;
  }

  /**
   * 从所有已加载任务中构造任务尝试完成事件列表，按完成时间排序。
   */
  private void constructTaskAttemptCompletionEvents() {
    // 确保所有任务已加载
    loadAllTasks();
    completionEvents = new LinkedList<TaskAttemptCompletionEvent>();
    List<TaskAttempt> allTaskAttempts = new LinkedList<TaskAttempt>();
    int numMapAttempts = 0;
    // 遍历所有任务收集所有任务尝试
    for (Map.Entry<TaskId,Task> taskEntry : tasks.entrySet()) {
      Task task = taskEntry.getValue();
      for (Map.Entry<TaskAttemptId,TaskAttempt> taskAttemptEntry : task.getAttempts().entrySet()) {
        TaskAttempt taskAttempt = taskAttemptEntry.getValue();
        allTaskAttempts.add(taskAttempt);
        // 统计Map任务尝试数量，预分配ArrayList容量
        if (task.getType() == TaskType.MAP) {
          ++numMapAttempts;
        }
      }
    }
    // 按完成时间排序，未完成的按启动时间排序，时间晚的排在前面
    Collections.sort(allTaskAttempts, new Comparator<TaskAttempt>() {

      @Override
      public int compare(TaskAttempt o1, TaskAttempt o2) {
        if (o1.getFinishTime() == 0 || o2.getFinishTime() == 0) {
          if (o1.getFinishTime() == 0 && o2.getFinishTime() == 0) {
            if (o1.getLaunchTime() == 0 || o2.getLaunchTime() == 0) {
              if (o1.getLaunchTime() == 0 && o2.getLaunchTime() == 0) {
                return 0;
              } else {
                long res = o1.getLaunchTime() - o2.getLaunchTime();
                return res > 0 ? -1 : 1;
              }
            } else {
              return (int) (o1.getLaunchTime() - o2.getLaunchTime());
            }
          } else {
            long res = o1.getFinishTime() - o2.getFinishTime();
            return res > 0 ? -1 : 1;
          }
        } else {
          return (int) (o1.getFinishTime() - o2.getFinishTime());
        }
      }
    });

    mapCompletionEvents =
        new ArrayList<TaskAttemptCompletionEvent>(numMapAttempts);
    int eventId = 0;
    // 为每个任务尝试构造完成事件对象
    for (TaskAttempt taskAttempt : allTaskAttempts) {

      TaskAttemptCompletionEvent tace =
          Records.newRecord(TaskAttemptCompletionEvent.class);

      int attemptRunTime = -1;
      // 计算任务尝试运行时间
      if (taskAttempt.getLaunchTime() != 0 && taskAttempt.getFinishTime() != 0) {
        attemptRunTime =
            (int) (taskAttempt.getFinishTime() - taskAttempt.getLaunchTime());
      }
      // 默认状态为KILLED，转换失败时使用默认值
      TaskAttemptCompletionEventStatus taceStatus =
          TaskAttemptCompletionEventStatus.KILLED;
      String taStateString = taskAttempt.getState().toString();
      try {
        taceStatus = TaskAttemptCompletionEventStatus.valueOf(taStateString);
      } catch (Exception e) {
        LOG.warn("Cannot constuct TACEStatus from TaskAtemptState: ["
            + taStateString + "] for taskAttemptId: [" + taskAttempt.getID()
            + "]. Defaulting to KILLED");
      }

      tace.setAttemptId(taskAttempt.getID());
      tace.setAttemptRunTime(attemptRunTime);
      tace.setEventId(eventId++);
      tace.setMapOutputServerAddress(taskAttempt
          .getAssignedContainerMgrAddress());
      tace.setStatus(taceStatus);
      completionEvents.add(tace);
      // Map任务尝试单独保存一份，用于兼容旧API
      if (taskAttempt.getID().getTaskId().getTaskType() == TaskType.MAP) {
        mapCompletionEvents.add(tace);
      }
    }
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    loadAllTasks();
    return tasks;
  }

  /**
   * 加载所有任务数据到内存，线程安全的延迟加载实现。
   */
  private void loadAllTasks() {
    // 双重检查锁实现延迟加载
    if (tasksLoaded.get()) {
      return;
    }
    tasksLock.lock();
    try {
      if (tasksLoaded.get()) {
        return;
      }
      // 遍历历史文件解析出的所有任务，转换为CompletedTask对象
      for (Map.Entry<TaskID, TaskInfo> entry : jobInfo.getAllTasks().entrySet()) {
        TaskId yarnTaskID = TypeConverter.toYarn(entry.getKey());
        TaskInfo taskInfo = entry.getValue();
        Task task = new CompletedTask(yarnTaskID, taskInfo);
        tasks.put(yarnTaskID, task);
        // 按任务类型分组保存，方便按类型查询
        if (task.getType() == TaskType.MAP) {
          mapTasks.put(task.getID(), task);
        } else if (task.getType() == TaskType.REDUCE) {
          reduceTasks.put(task.getID(), task);
        }
      }
      tasksLoaded.set(true);
    } finally {
      tasksLock.unlock();
    }
  }

  /**
   * 创建作业历史解析器，子类可覆盖该方法自定义解析逻辑。
   * @param historyFileAbsolute 历史文件绝对路径
   * @return 历史解析器实例
   * @throws IOException 创建失败时抛出异常
   */
  protected JobHistoryParser createJobHistoryParser(Path historyFileAbsolute)
      throws IOException {
    return new JobHistoryParser(historyFileAbsolute.getFileSystem(conf),
                historyFileAbsolute);
  }

  /**
   * 加载完整作业历史数据，解析历史文件获取作业基础信息。
   * @param loadTasks 是否同时加载所有任务数据
   * @param historyFileAbsolute 历史文件绝对路径
   * @throws IOException 解析或加载失败时抛出异常
   */
  // 任务级数据在请求时才延迟加载，此处仅加载作业级别数据
  protected synchronized void loadFullHistoryData(boolean loadTasks,
      Path historyFileAbsolute) throws IOException {
    LOG.info("Loading history file: