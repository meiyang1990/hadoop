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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

/**
 * 推测执行运行时间估算器抽象基类，基于任务开始/结束时间统计实现核心公共逻辑
 * 负责收集已完成任务的运行时长，为慢任务判断和新推测任务运行时间估算提供统计基础
 */
abstract class StartEndTimesBase implements TaskRuntimeEstimator {
  /** 允许开启推测执行的最小已完成任务比例 */
  static final float MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE
      = 0.05F;
  /** 允许开启推测执行的最小已完成任务数量 */
  static final int MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
      = 1;

  protected AppContext context = null;

  /** 存储每个任务尝试的启动时间戳，线程安全 */
  protected final Map<TaskAttemptId, Long> startTimes
      = new ConcurrentHashMap<TaskAttemptId, Long>();

  // XXXX This class design assumes that the contents of AppContext.getAllJobs
  //   never changes.  Is that right?
  //
  // This assumption comes in in several places, mostly in data structure that
  //   can grow without limit if a AppContext gets new Job's when the old ones
  //   run out.  Also, these mapper statistics blocks won't cover the Job's
  //   we don't know about.
  /** 存储每个作业中Map任务的运行时长统计数据 */
  protected final Map<Job, DataStatistics> mapperStatistics
      = new HashMap<Job, DataStatistics>();
  /** 存储每个作业中Reduce任务的运行时长统计数据 */
  protected final Map<Job, DataStatistics> reducerStatistics
      = new HashMap<Job, DataStatistics>();


  /** 存储每个作业配置的慢任务相对阈值，超过该阈值会被判定为慢任务 */
  private final Map<Job, Float> slowTaskRelativeThresholds
      = new HashMap<Job, Float>();

  /** 存储已完成的任务集合，避免重复统计同一个任务的多次成功尝试 */
  protected final Set<Task> doneTasks = new HashSet<Task>();

  /**
   * 注册新启动的任务尝试，记录其启动时间戳
   * @param status 任务尝试状态信息
   * @param timestamp 启动时间戳
   */
  @Override
  public void enrollAttempt(TaskAttemptStatus status, long timestamp) {
    startTimes.put(status.id,timestamp);
  }

  /**
   * 获取任务尝试的注册（启动）时间
   * @param attemptID 任务尝试ID
   * @return 启动时间戳，未找到则返回Long.MAX_VALUE
   */
  @Override
  public long attemptEnrolledTime(TaskAttemptId attemptID) {
    Long result = startTimes.get(attemptID);

    return result == null ? Long.MAX_VALUE : result;
  }


  /**
   * 初始化上下文，加载所有作业的统计结构和配置参数
   * @param conf 作业配置
   * @param context 应用上下文
   */
  @Override
  public void contextualize(Configuration conf, AppContext context) {
    this.context = context;

    Map<JobId, Job> allJobs = context.getAllJobs();

    for (Map.Entry<JobId, Job> entry : allJobs.entrySet()) {
      final Job job = entry.getValue();
      mapperStatistics.put(job, new DataStatistics());
      reducerStatistics.put(job, new DataStatistics());
      slowTaskRelativeThresholds.put
          (job, conf.getFloat(MRJobConfig.SPECULATIVE_SLOWTASK_THRESHOLD,1.0f));
    }
  }

  /**
   * 根据任务ID获取对应类型的运行时长统计数据
   * @param taskID 任务ID
   * @return 对应任务类型的统计数据对象，找不到任务/不支持类型则返回null
   */
  protected DataStatistics dataStatisticsForTask(TaskId taskID) {
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    if (job == null) {
      return null;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return null;
    }

    return task.getType() == TaskType.MAP
            ? mapperStatistics.get(job)
            : task.getType() == TaskType.REDUCE
                ? reducerStatistics.get(job)
                : null;
  }

  /**
   * 计算慢任务判定阈值，运行时间超过该阈值的任务会被判定为慢任务，可启动推测执行
   * @param taskID 目标任务ID
   * @return 慢任务阈值，不满足推测执行条件则返回Long.MAX_VALUE
   */
  @Override
  public long thresholdRuntime(TaskId taskID) {
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    TaskType type = taskID.getTaskType();

    DataStatistics statistics
        = dataStatisticsForTask(taskID);

    int completedTasksOfType
        = type == TaskType.MAP
            ? job.getCompletedMaps() : job.getCompletedReduces();

    int totalTasksOfType
        = type == TaskType.MAP
            ? job.getTotalMaps() : job.getTotalReduces();

    // 判断是否满足开启推测执行的最小完成量要求
    if (completedTasksOfType < MINIMUM_COMPLETE_NUMBER_TO_SPECULATE
        || (((float)completedTasksOfType) / totalTasksOfType)
              < MINIMUM_COMPLETE_PROPORTION_TO_SPECULATE ) {
      return Long.MAX_VALUE;
    }

    long result =  statistics == null
        ? Long.MAX_VALUE
        : (long)statistics.outlier(slowTaskRelativeThresholds.get(job));
    return result;
  }

  /**
   * 估算新任务尝试的预计运行时间
   * @param id 任务ID
   * @return 同类型已完成任务的平均运行时长，无统计数据则返回-1
   */
  @Override
  public long estimatedNewAttemptRuntime(TaskId id) {
    DataStatistics statistics = dataStatisticsForTask(id);

    if (statistics == null) {
      return -1L;
    }
    return (long) statistics.mean();
  }

  /**
   * 更新任务尝试状态，当任务成功完成时统计其运行时长
   * @param status 任务尝试状态信息
   * @param timestamp 更新时间戳（任务完成时间）
   */
  @Override
  public void updateAttempt(TaskAttemptStatus status, long timestamp) {

    TaskAttemptId attemptID = status.id;
    TaskId taskID = attemptID.getTaskId();
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    if (job == null) {
      return;
    }

    Task task = job.getTask(taskID);

    if (task == null) {
      return;
    }

    Long boxedStart = startTimes.get(attemptID);
    long start = boxedStart == null ? Long.MIN_VALUE : boxedStart;
    
    TaskAttempt taskAttempt = task.getAttempt(attemptID);

    // 仅处理成功完成的任务
    if (taskAttempt.getState() == TaskAttemptState.SUCCEEDED) {
      boolean isNew = false;
      // 检查是否为任务首次成功完成，避免重复统计
      synchronized (doneTasks) {
        if (!doneTasks.contains(task)) {
          doneTasks.add(task);
          isNew = true;
        }
      }

      // 仅统计首次成功完成的任务运行时长
      // 说明：如果任务因为推测执行或节点故障多次完成，只统计第一次成功的运行时间
      if (isNew) {
        long finish = timestamp;
        if (start > 1L && finish > 1L && start <= finish) {
          long duration = finish - start;

          DataStatistics statistics
          = dataStatisticsForTask(taskID);

          if (statistics != null) {
            statistics.add(duration);
          }
        }
      }
    }
  }
}