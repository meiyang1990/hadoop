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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.yarn.util.Records;

/**
 * 历史任务完成后存储在历史服务器中的Task实现类
 * 从历史日志中解析出任务信息，提供只读访问能力，供历史查询使用
 */
public class CompletedTask implements Task {

  private static final Counters EMPTY_COUNTERS = new Counters();

  private final TaskId taskId;
  private final TaskInfo taskInfo;
  private TaskReport report;
  private TaskAttemptId successfulAttempt;
  private List<String> reportDiagnostics = new ArrayList<String>(2);
  private Lock taskAttemptsLock = new ReentrantLock();
  private AtomicBoolean taskAttemptsLoaded = new AtomicBoolean(false);
  private final Map<TaskAttemptId, TaskAttempt> attempts =
      new LinkedHashMap<TaskAttemptId, TaskAttempt>(2);

  /**
   * 构造已完成任务对象，从历史解析信息中初始化
   * @param taskId 任务ID
   * @param taskInfo 从历史日志解析得到的任务信息
   */
  CompletedTask(TaskId taskId, TaskInfo taskInfo) {
    //TODO JobHistoryParser.handleTaskFailedAttempt should use state from the event.
    this.taskInfo = taskInfo;
    this.taskId = taskId;
  }

  @Override
  /**
   * 已完成任务无需提交，固定返回false
   * @param taskAttemptID 尝试ID
   * @return 始终返回false
   */
  public boolean canCommit(TaskAttemptId taskAttemptID) {
    return false;
  }

  @Override
  /**
   * 根据尝试ID获取对应任务尝试
   * 触发懒加载加载所有尝试信息
   * @param attemptID 任务尝试ID
   * @return 对应任务尝试对象
   */
  public TaskAttempt getAttempt(TaskAttemptId attemptID) {
    loadAllTaskAttempts();
    return attempts.get(attemptID);
  }

  @Override
  /**
   * 获取该任务所有尝试的集合
   * 触发懒加载加载所有尝试信息
   * @return 任务尝试ID到尝试对象的映射
   */
  public Map<TaskAttemptId, TaskAttempt> getAttempts() {
    loadAllTaskAttempts();
    return attempts;
  }

  @Override
  /**
   * 获取该任务的所有计数器
   * @return 任务计数器对象
   */
  public Counters getCounters() {
    return taskInfo.getCounters();
  }

  @Override
  /**
   * 获取该任务的ID
   * @return 任务ID
   */
  public TaskId getID() {
    return taskId;
  }

  @Override
  /**
   * 获取任务进度，已完成固定返回100%
   * @return 始终返回1.0f
   */
  public float getProgress() {
    return 1.0f;
  }

  @Override
  /**
   * 获取任务报告，懒加载构造任务报告
   * @return 任务报告对象
   */
  public synchronized TaskReport getReport() {
    if (report == null) {
      constructTaskReport();
    }
    return report;
  }
  

  
  @Override
  /**
   * 获取任务类型（MAP/REDUCE）
   * @return 任务类型枚举
   */
  public TaskType getType() {
    return TypeConverter.toYarn(taskInfo.getTaskType());
  }

  @Override
  /**
   * 判断任务是否已完成，已完成任务固定返回true
   * @return 始终返回true
   */
  public boolean isFinished() {
    return true;
  }

  @Override
  /**
   * 获取任务最终状态
   * @return 任务状态枚举
   */
  public TaskState getState() {
    return taskInfo.getTaskStatus() == null ? TaskState.KILLED : TaskState
        .valueOf(taskInfo.getTaskStatus());
  }

  /**
   * 构造完整的任务报告对象，填充所有字段
   */
  private void constructTaskReport() {
    loadAllTaskAttempts();
    this.report = Records.newRecord(TaskReport.class);
    report.setTaskId(taskId);
    long minLaunchTime = Long.MAX_VALUE;
    // 找到最早的尝试启动时间作为任务启动时间
    for(TaskAttempt attempt: attempts.values()) {
      minLaunchTime = Math.min(minLaunchTime, attempt.getLaunchTime());
    }
    // 无尝试时设置启动时间为-1
    minLaunchTime = minLaunchTime == Long.MAX_VALUE ? -1 : minLaunchTime;
    report.setStartTime(minLaunchTime);
    report.setFinishTime(taskInfo.getFinishTime());
    report.setTaskState(getState());
    report.setProgress(getProgress());
    Counters counters = getCounters();
    // 计数器为空时使用空计数器
    if (counters == null) {
      counters = EMPTY_COUNTERS;
    }
    report.setRawCounters(counters);
    // 设置成功的尝试
    if (successfulAttempt != null) {
      report.setSuccessfulAttempt(successfulAttempt);
    }
    // 添加所有诊断信息
    report.addAllDiagnostics(reportDiagnostics);
    // 添加所有尝试作为运行过的尝试
    report
        .addAllRunningAttempts(new ArrayList<TaskAttemptId>(attempts.keySet()));
  }

  /**
   * 懒加载所有任务尝试，从历史解析信息转换为CompletedTaskAttempt
   * 使用双重检查锁定保证线程安全，只加载一次
   */
  private void loadAllTaskAttempts() {
    // 已经加载过直接返回
    if (taskAttemptsLoaded.get()) {
      return;
    }
    taskAttemptsLock.lock();
    try {
      // 双重检查，避免并发重复加载
      if (taskAttemptsLoaded.get()) {
        return;
      }

      // 遍历所有历史尝试信息，转换为CompletedTaskAttempt对象
      for (TaskAttemptInfo attemptHistory : taskInfo.getAllTaskAttempts()
          .values()) {
        CompletedTaskAttempt attempt =
            new CompletedTaskAttempt(taskId, attemptHistory);
        // 聚合所有尝试的诊断信息
        reportDiagnostics.addAll(attempt.getDiagnostics());
        attempts.put(attempt.getID(), attempt);
        // 记录第一个成功的尝试
        if (successfulAttempt == null
            && attemptHistory.getTaskStatus() != null
            && attemptHistory.getTaskStatus().equals(
                TaskState.SUCCEEDED.toString())) {
          successfulAttempt =
              TypeConverter.toYarn(attemptHistory.getAttemptId());
        }
      }
      // 标记加载完成
      taskAttemptsLoaded.set(true);
    } finally {
      taskAttemptsLock.unlock();
    }
  }
}