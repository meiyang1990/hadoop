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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Clock;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce推测执行的默认实现类，负责周期性检测慢任务并发起推测执行。
 * 核心职责是：根据任务运行时数据识别拖慢整体作业的慢任务，在集群资源允许的情况下启动推测副本，
 * 由先完成的任务提供最终结果，从而提升作业整体运行效率。
 * 继承AbstractService实现服务生命周期管理，实现Speculator接口对接框架推测执行流程。
 */
public class DefaultSpeculator extends AbstractService implements
    Speculator {

  // 推测不执行的标记值：任务运行在预期时间内，无需推测
  private static final long ON_SCHEDULE = Long.MIN_VALUE;
  // 推测不执行的标记值：该任务已经在进行推测执行，无需重复发起
  private static final long ALREADY_SPECULATING = Long.MIN_VALUE + 1;
  // 推测不执行的标记值：任务启动时间太短，尚未积累足够运行数据
  private static final long TOO_NEW = Long.MIN_VALUE + 2;
  // 推测不执行的标记值：任务进度正常，无需推测
  private static final long PROGRESS_IS_GOOD = Long.MIN_VALUE + 3;
  // 推测不执行的标记值：任务当前未处于运行状态
  private static final long NOT_RUNNING = Long.MIN_VALUE + 4;
  // 推测不执行的标记值：新推测任务完成时间预估晚于现有任务，无需推测
  private static final long TOO_LATE_TO_SPECULATE = Long.MIN_VALUE + 5;

  // 未发起推测后，下一次检测的最小间隔时间（毫秒）
  private long soonestRetryAfterNoSpeculate;
  // 发起推测后，下一次检测的最小间隔时间（毫秒）
  private long soonestRetryAfterSpeculate;
  // 允许进行推测执行的运行中任务占总运行任务的比例上限
  private double proportionRunningTasksSpeculatable;
  // 允许进行推测执行的任务占该类型总任务的比例上限
  private double proportionTotalTasksSpeculatable;
  // 允许同时进行推测执行的最小任务数量
  private int  minimumAllowedSpeculativeTasks;

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultSpeculator.class);

  // 存储当前正在运行的任务集合，key为任务ID，value标记是否运行
  private final ConcurrentMap<TaskId, Boolean> runningTasks
      = new ConcurrentHashMap<TaskId, Boolean>();

  // 存储任务尝试的运行历史统计数据，用于检测心跳停止的拖慢任务
  private final ConcurrentMap<TaskAttemptId, TaskAttemptHistoryStatistics>
      runningTaskAttemptStatistics = new ConcurrentHashMap<TaskAttemptId,
          TaskAttemptHistoryStatistics>();
  // 心跳超时阈值，超过该时间未收到心跳则视为任务卡住，主动触发检测（毫秒）
  private static final long MAX_WAITTING_TIME_FOR_HEARTBEAT = 9 * 1000;

  // 存储每个作业当前还需要的Map容器数量，用于判断是否有空闲资源发起推测
  private final ConcurrentMap<JobId, AtomicInteger> mapContainerNeeds
      = new ConcurrentHashMap<JobId, AtomicInteger>();
  // 存储每个作业当前还需要的Reduce容器数量，用于判断是否有空闲资源发起推测
  private final ConcurrentMap<JobId, AtomicInteger> reduceContainerNeeds
      = new ConcurrentHashMap<JobId, AtomicInteger>();

  // 存储已经发起过推测执行的任务集合
  private final Set<TaskId> mayHaveSpeculated = new HashSet<TaskId>();

  private final Configuration conf;
  private AppContext context;
  // 后台检测线程，周期性扫描任务寻找推测机会
  private Thread speculationBackgroundThread = null;
  // 服务停止标记
  private volatile boolean stopped = false;
  // 任务运行时间估算器，用于估算任务剩余运行时间和新推测任务的运行时间
  private TaskRuntimeEstimator estimator;

  // 控制后台扫描的阻塞队列，用于唤醒等待中的后台线程
  private BlockingQueue<Object> scanControl = new LinkedBlockingQueue<Object>();

  private final Clock clock;

  private final EventHandler<Event> eventHandler;

  /**
   * 构造默认推测执行器，使用应用上下文提供的时钟。
   * @param conf 作业配置
   * @param context 应用上下文
   */
  public DefaultSpeculator(Configuration conf, AppContext context) {
    this(conf, context, context.getClock());
  }

  /**
   * 构造默认推测执行器，允许传入自定义时钟（用于测试）。
   * @param conf 作业配置
   * @param context 应用上下文
   * @param clock 时钟实现
   */
  public DefaultSpeculator(Configuration conf, AppContext context, Clock clock) {
    this(conf, context, getEstimator(conf, context), clock);
  }
  
  /**
   * 通过反射从配置中创建任务运行时间估算器实例。
   * @param conf 作业配置
   * @param context 应用上下文
   * @return 配置指定的任务运行时间估算器实例，默认使用LegacyTaskRuntimeEstimator
   */
  static private TaskRuntimeEstimator getEstimator
      (Configuration conf, AppContext context) {
    TaskRuntimeEstimator estimator;
    
    try {
      // 从配置中获取估算器实现类
      Class<? extends TaskRuntimeEstimator> estimatorClass
          = conf.getClass(MRJobConfig.MR_AM_TASK_ESTIMATOR,
                          LegacyTaskRuntimeEstimator.class,
                          TaskRuntimeEstimator.class);

      Constructor<? extends TaskRuntimeEstimator> estimatorConstructor
          = estimatorClass.getConstructor();

      estimator = estimatorConstructor.newInstance();

      estimator.contextualize(conf, context);
    } catch (InstantiationException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    } catch (IllegalAccessException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    } catch (InvocationTargetException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    } catch (NoSuchMethodException ex) {
      LOG.error("Can't make a speculation runtime estimator", ex);
      throw new YarnRuntimeException(ex);
    }
    
  return estimator;
  }

  /**
   * 完整构造方法，允许注入自定义估算器和时钟，主要供测试使用。
   * @param conf 作业配置
   * @param context 应用上下文
   * @param estimator 任务运行时间估算器实例
   * @param clock 时钟实现
   */
  // This constructor is designed to be called by other constructors.
  //  However, it's public because we do use it in the test cases.
  // Normally we figure out our own estimator.
  public DefaultSpeculator
      (Configuration conf, AppContext context,
       TaskRuntimeEstimator estimator, Clock clock) {
    super(DefaultSpeculator.class.getName());

    this.conf = conf;
    this.context = context;
    this.estimator = estimator;
    this.clock = clock;
    this.eventHandler = context.getEventHandler();
    // 从配置加载推测执行相关参数
    this.soonestRetryAfterNoSpeculate =
        conf.getLong(MRJobConfig.SPECULATIVE_RETRY_AFTER_NO_SPECULATE,
                MRJobConfig.DEFAULT_SPECULATIVE_RETRY_AFTER_NO_SPECULATE);
    this.soonestRetryAfterSpeculate =
        conf.getLong(MRJobConfig.SPECULATIVE_RETRY_AFTER_SPECULATE,
                MRJobConfig.DEFAULT_SPECULATIVE_RETRY_AFTER_SPECULATE);
    this.proportionRunningTasksSpeculatable =
        conf.getDouble(MRJobConfig.SPECULATIVECAP_RUNNING_TASKS,
                MRJobConfig.DEFAULT_SPECULATIVECAP_RUNNING_TASKS);
    this.proportionTotalTasksSpeculatable =
        conf.getDouble(MRJobConfig.SPECULATIVECAP_TOTAL_TASKS,
                MRJobConfig.DEFAULT_SPECULATIVECAP_TOTAL_TASKS);
    this.minimumAllowedSpeculativeTasks =
        conf.getInt(MRJobConfig.SPECULATIVE_MINIMUM_ALLOWED_TASKS,
                MRJobConfig.DEFAULT_SPECULATIVE_MINIMUM_ALLOWED_TASKS);
  }

/*   *************************************************************    */

  // This is the task-mongering that creates the two new threads -- one for
  //  processing events from the event queue and one for periodically
  //  looking for speculation opportunities

  /**
   * 服务启动方法，启动后台推测检测线程。
   * @throws Exception 启动异常
   */
  @Override
  protected void serviceStart() throws Exception {
    // 后台线程核心逻辑：周期性扫描任务寻找推测机会
    Runnable speculationBackgroundCore
        = new Runnable() {
            @Override
            public void run() {
              while (!stopped && !Thread.currentThread().isInterrupted()) {
                long backgroundRunStartTime = clock.getTime();
                try {
                  // 执行一次推测检测，返回本次发起的推测任务数量
                  int speculations = computeSpeculations();
                  // 根据本次是否发起推测，计算下次检测的最小等待时间
                  long mininumRecomp
                      = speculations > 0 ? soonestRetryAfterSpeculate
                                         : soonestRetryAfterNoSpeculate;

                  long wait = Math.max(mininumRecomp,
                        clock.getTime() - backgroundRunStartTime);

                  if (speculations > 0) {
                    LOG.info("We launched " + speculations
                        + " speculations.  Sleeping " + wait + " milliseconds.");
                  }

                  // 阻塞等待，等待超时或被唤醒后进行下一轮检测
                  Object pollResult
                      = scanControl.poll(wait, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                  if (!stopped) {
                    LOG.error("Background thread returning, interrupted", e);
                  }
                  return;
                }
              }
            }
          };
    // 创建并启动后台线程
    speculationBackgroundThread = new SubjectInheritingThread
        (speculationBackgroundCore, "DefaultSpeculator background processing");
    speculationBackgroundThread.start();

    super.serviceStart();
  }

  /**
   * 服务停止方法，中断并退出后台检测线程。
   * @throws Exception 停止异常
   */
  @Override
  protected void serviceStop()throws Exception {
      stopped = true;
    // this could be called before background thread is established
    if (speculationBackgroundThread != null) {
      speculationBackgroundThread.interrupt();
    }
    super.serviceStop();
  }

  /**
   * 处理任务尝试状态更新事件。
   * @param status 任务尝试最新状态
   */
  @Override
  public void handleAttempt(TaskAttemptStatus status) {
    long timestamp = clock.getTime();
    statusUpdate(status, timestamp);
  }

  // This is not part of the Speculator interface; it's used only for
  //  testing
  /**
   * 检测事件队列是否为空，仅供测试使用。
   * @return 队列是否为空
   */
  public boolean eventQueueEmpty() {
    return scanControl.isEmpty();
  }

  // This interface is intended to be used only for test cases.
  /**
   * 主动触发一次推测扫描，仅供调试和测试使用。
   */
  public void scanForSpeculations() {
    LOG.info("We got asked to run a debug speculation scan.");
    // debug
    System.out.println("We got asked to run a debug speculation scan.");
    System.out.println("There are " + scanControl.size()
        + " events stacked already.");
    scanControl.add(new Object());
    Thread.yield();
  }


/*   *************************************************************    */

  // This section contains the code that gets run for a SpeculatorEvent

  /**
   * 获取对应作业对应任务类型的容器需求计数器。
   * @param taskID 任务ID
   * @return 容器需求原子计数器
   */
  private AtomicInteger containerNeed(TaskId taskID) {
    JobId jobID = taskID.getJobId();
    TaskType taskType = taskID.getTaskType();

    ConcurrentMap<JobId, AtomicInteger> relevantMap
        = taskType == TaskType.MAP ? mapContainerNeeds : reduceContainerNeeds;

    AtomicInteger result = relevantMap.get(jobID);

    if (result == null) {
      relevantMap.putIfAbsent(jobID, new AtomicInteger(0));
      result = relevantMap.get(jobID);
    }

    return result;
  }

  /**
   * 根据事件类型分发处理推测执行相关事件。
   * @param event 推测执行事件
   */
  private synchronized void processSpeculatorEvent(SpeculatorEvent event) {
    switch (event.getType()) {
      case ATTEMPT_STATUS_UPDATE:
        statusUpdate(event.getReportedStatus(), event.getTimestamp());
        break;

      case TASK_CONTAINER_NEED_UPDATE:
      {
        // 更新作业容器需求数量
        AtomicInteger need = containerNeed(event.getTaskID());
        need.addAndGet(event.containersNeededChange());
        break;
      }

      case ATTEMPT_START:
      {
        // 注册新启动的任务尝试到估算器
        LOG.info("ATTEMPT_START " + event.getTaskID());
        estimator.enrollAttempt
            (event.getReportedStatus(), event.getTimestamp());
        break;
      }
      
      case JOB_CREATE:
      {
        // 作业创建时初始化估算器
        LOG.info("JOB_CREATE " + event.getJobID());
        estimator.contextualize(getConfig(), context);
        break;
      }
    }
  }

  /**
   * 更新任务尝试运行状态，将最新状态合并到推测数据中。
   *
   * @param reportedStatus 任务尝试最新状态报告
   * @param timestamp 状态报告对应的时间戳
   */
  protected void statusUpdate(TaskAttemptStatus reportedStatus, long timestamp) {

    String stateString = reportedStatus.taskState.toString();

    TaskAttemptId attemptID = reportedStatus.id;
    TaskId taskID = attemptID.getTaskId();
    Job job = context.getJob(taskID.getJobId());

    if (job == null) {
      return;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return;
    }

    // 更新估算器中的任务尝试数据
    estimator.updateAttempt(reportedStatus, timestamp);

    // 根据任务状态更新运行集合
    if (stateString.equals(TaskAttemptState.RUNNING.name())) {
      runningTasks.putIfAbsent(taskID, Boolean.TRUE);
    } else {
      runningTasks.remove(taskID, Boolean.TRUE);
      if (!stateString.equals(TaskAttemptState.STARTING.name())) {
        // 任务结束，清理历史统计数据
        runningTaskAttemptStatistics.remove(attemptID);
      }
    }
  }

/*   *************************************************************    */

// This is