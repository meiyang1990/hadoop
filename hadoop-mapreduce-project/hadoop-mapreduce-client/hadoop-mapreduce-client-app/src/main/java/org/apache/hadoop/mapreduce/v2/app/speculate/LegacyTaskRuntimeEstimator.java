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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;


/**
 * 传统MapReduce任务运行时间预估实现类，为推测执行提供运行时间预测
 * 基于当前运行任务的进度推算剩余运行时间，是经典的推测执行预估算法
 */
public class LegacyTaskRuntimeEstimator extends StartEndTimesBase {

  // 存储每个任务尝试的运行时间预估结果
  private final Map<TaskAttempt, AtomicLong> attemptRuntimeEstimates
      = new ConcurrentHashMap<TaskAttempt, AtomicLong>();
  // 存储每个任务尝试运行时间预估的方差
  private final ConcurrentHashMap<TaskAttempt, AtomicLong> attemptRuntimeEstimateVariances
      = new ConcurrentHashMap<TaskAttempt, AtomicLong>();

  /**
   * 更新任务尝试状态，重新计算运行时间预估
   * @param status 任务尝试最新状态
   * @param timestamp 当前时间戳
   */
  @Override
  public void updateAttempt(TaskAttemptStatus status, long timestamp) {
    super.updateAttempt(status, timestamp);
    
    // 获取任务尝试相关ID
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

    TaskAttempt taskAttempt = task.getAttempt(attemptID);

    if (taskAttempt == null) {
      return;
    }

    // 获取任务尝试启动时间
    Long boxedStart = startTimes.get(attemptID);
    long start = boxedStart == null ? Long.MIN_VALUE : boxedStart;

    // We need to do two things.
    //  1: If this is a completion, we accumulate statistics in the superclass
    //  2: If this is not a completion, we learn more about it.

    // 仅对正在运行中的任务更新预估
    if (taskAttempt.getState() == TaskAttemptState.RUNNING) {
      // 从缓存获取已有预估容器
      AtomicLong estimateContainer = attemptRuntimeEstimates.get(taskAttempt);
      AtomicLong estimateVarianceContainer
          = attemptRuntimeEstimateVariances.get(taskAttempt);

      // 初始化预估结果容器
      if (estimateContainer == null) {
        if (attemptRuntimeEstimates.get(taskAttempt) == null) {
          attemptRuntimeEstimates.put(taskAttempt, new AtomicLong());

          estimateContainer = attemptRuntimeEstimates.get(taskAttempt);
        }
      }

      // 初始化方差容器
      if (estimateVarianceContainer == null) {
        attemptRuntimeEstimateVariances.putIfAbsent(taskAttempt, new AtomicLong());
        estimateVarianceContainer = attemptRuntimeEstimateVariances.get(taskAttempt);
      }


      long estimate = -1;
      long varianceEstimate = -1;

      // 算法假设：一个任务最多同时运行两个尝试，不会启动第三个推测任务
      // 基于已运行时间和当前进度推算总运行时间
      if (start > 0 && timestamp > start) {
        estimate = (long) ((timestamp - start) / Math.max(0.0001, status.progress));
        varianceEstimate = (long) (estimate * status.progress / 10);
      }
      // 更新预估结果
      if (estimateContainer != null) {
        estimateContainer.set(estimate);
      }
      // 更新方差结果
      if (estimateVarianceContainer != null) {
        estimateVarianceContainer.set(varianceEstimate);
      }
    }
  }

  /**
   * 从缓存中根据任务尝试ID获取对应存储的预估数据
   * @param data 存储预估数据的缓存
   * @param attemptID 任务尝试ID
   * @return 存储的预估数值，找不到则返回-1
   */
  private long storedPerAttemptValue
       (Map<TaskAttempt, AtomicLong> data, TaskAttemptId attemptID) {
    TaskId taskID = attemptID.getTaskId();
    JobId jobID = taskID.getJobId();
    Job job = context.getJob(jobID);

    Task task = job.getTask(taskID);

    if (task == null) {
      return -1L;
    }

    TaskAttempt taskAttempt = task.getAttempt(attemptID);

    if (taskAttempt == null) {
      return -1L;
    }

    AtomicLong estimate = data.get(taskAttempt);

    return estimate == null ? -1L : estimate.get();

  }

  /**
   * 获取指定任务尝试的预估总运行时间
   * @param attemptID 任务尝试ID
   * @return 预估总运行时间，单位毫秒
   */
  @Override
  public long estimatedRuntime(TaskAttemptId attemptID) {
    return storedPerAttemptValue(attemptRuntimeEstimates, attemptID);
  }

  /**
   * 获取指定任务尝试运行时间预估的方差
   * @param attemptID 任务尝试ID
   * @return 预估方差
   */
  @Override
  public long runtimeEstimateVariance(TaskAttemptId attemptID) {
    return storedPerAttemptValue(attemptRuntimeEstimateVariances, attemptID);
  }
}