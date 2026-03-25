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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认的作业历史文件解析器，用于从历史文件中解析MapReduce作业运行信息，聚合到内存对象中供查询使用
 * 
 * 典型使用方式:
 * JobHistoryParser parser = new JobHistoryParser(fs, historyFile);
 * job = parser.parse();
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobHistoryParser implements HistoryEventHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryParser.class);
  
  private final FSDataInputStream in;
  private JobInfo info = null;

  private IOException parseException = null;
  
  /**
   * 基于指定文件系统和历史文件路径构造作业历史解析器
   * @param fs 文件系统对象
   * @param file 历史文件路径字符串
   * @throws IOException 打开文件失败时抛出
   */
  public JobHistoryParser(FileSystem fs, String file) throws IOException {
    this(fs, new Path(file));
  }
  
  /**
   * 基于指定文件系统和历史文件路径构造作业历史解析器
   * @param fs 文件系统对象
   * @param historyFile 历史文件路径对象
   * @throws IOException 打开文件失败时抛出
   */
  public JobHistoryParser(FileSystem fs, Path historyFile) 
  throws IOException {
    this(fs.open(historyFile));
  }
  
  /**
   * 基于输入流构造作业历史解析器
   * @param in 历史文件输入流
   */
  public JobHistoryParser(FSDataInputStream in) {
    this.in = in;
  }
  
  /**
   * 解析历史文件，将事件交给指定处理器处理
   * @param handler 历史事件处理器
   * @throws IOException 解析过程中IO错误时抛出
   */
  public synchronized void parse(HistoryEventHandler handler) 
    throws IOException {
    parse(new EventReader(in), handler);
  }
  
  /**
   * 仅用于单元测试，基于指定事件读取器解析，将事件交给指定处理器处理
   * @param reader 事件读取器
   * @param handler 历史事件处理器
   * @throws IOException 解析过程中IO错误时抛出
   */
  @Private
  public synchronized void parse(EventReader reader, HistoryEventHandler handler)
    throws IOException {
    int eventCtr = 0;
    HistoryEvent event;
    try {
      // 循环读取事件直到文件结束
      while ((event = reader.getNextEvent()) != null) {
        handler.handleEvent(event);
        ++eventCtr;
      } 
    } catch (IOException ioe) {
      // 解析出错后保存异常，供后续查询
      LOG.info("Caught exception parsing history file after " + eventCtr + 
          " events", ioe);
      parseException = ioe;
    } finally {
      // 无论解析成功失败，都关闭输入流
      in.close();
    }
  }
  
  
  /**
   * 解析整个历史文件，将结果聚合填充到JobInfo对象中
   * 第一次调用会执行解析并填充对象，后续调用直接返回已解析好的对象
   * 方法返回时会关闭输入流
   * 
   * 解析遇到不完整记录会停止解析，可通过{@link #getParseException()}获取解析过程中产生的异常
   * 
   * @return 填充完成的作业信息对象
   * @throws IOException 解析过程中IO错误时抛出
   * @see #getParseException()
   */
  public synchronized JobInfo parse() throws IOException {
    return parse(new EventReader(in)); 
  }

  /**
   * 仅用于单元测试，基于指定事件读取器解析作业历史
   * @param reader 事件读取器
   * @return 填充完成的作业信息对象
   * @throws IOException 解析过程中IO错误时抛出
   */
  @Private
  public synchronized JobInfo parse(EventReader reader) throws IOException {

    if (info != null) {
      return info;
    }

    info = new JobInfo();
    parse(reader, this);
    return info;
  }
  
  /**
   * 获取解析过程中产生的异常，如果没有异常则返回null
   * 
   * @return 解析异常对象
   * @see #parse()
   */
  public synchronized IOException getParseException() {
    return parseException;
  }
  
  @Override
  public void handleEvent(HistoryEvent event)  { 
    // 根据事件类型分发到对应处理方法
    EventType type = event.getEventType();

    switch (type) {
    case JOB_SUBMITTED:
      handleJobSubmittedEvent((JobSubmittedEvent)event);
      break;
    case JOB_STATUS_CHANGED:
      break;
    case JOB_INFO_CHANGED:
      handleJobInfoChangeEvent((JobInfoChangeEvent) event);
      break;
    case JOB_INITED:
      handleJobInitedEvent((JobInitedEvent) event);
      break;
    case JOB_PRIORITY_CHANGED:
      handleJobPriorityChangeEvent((JobPriorityChangeEvent) event);
      break;
    case JOB_QUEUE_CHANGED:
      handleJobQueueChangeEvent((JobQueueChangeEvent) event);
      break;
    case JOB_FAILED:
    case JOB_KILLED:
    case JOB_ERROR:
      handleJobFailedEvent((JobUnsuccessfulCompletionEvent) event);
      break;
    case JOB_FINISHED:
      handleJobFinishedEvent((JobFinishedEvent)event);
      break;
    case TASK_STARTED:
      handleTaskStartedEvent((TaskStartedEvent) event);
      break;
    case TASK_FAILED:
      handleTaskFailedEvent((TaskFailedEvent) event);
      break;
    case TASK_UPDATED:
      handleTaskUpdatedEvent((TaskUpdatedEvent) event);
      break;
    case TASK_FINISHED:
      handleTaskFinishedEvent((TaskFinishedEvent) event);
      break;
    case MAP_ATTEMPT_STARTED:
    case CLEANUP_ATTEMPT_STARTED:
    case REDUCE_ATTEMPT_STARTED:
    case SETUP_ATTEMPT_STARTED:
      handleTaskAttemptStartedEvent((TaskAttemptStartedEvent) event);
      break;
    case MAP_ATTEMPT_FAILED:
    case CLEANUP_ATTEMPT_FAILED:
    case REDUCE_ATTEMPT_FAILED:
    case SETUP_ATTEMPT_FAILED:
    case MAP_ATTEMPT_KILLED:
    case CLEANUP_ATTEMPT_KILLED:
    case REDUCE_ATTEMPT_KILLED:
    case SETUP_ATTEMPT_KILLED:
      handleTaskAttemptFailedEvent(
          (TaskAttemptUnsuccessfulCompletionEvent) event);
      break;
    case MAP_ATTEMPT_FINISHED:
      handleMapAttemptFinishedEvent((MapAttemptFinishedEvent) event);
      break;
    case REDUCE_ATTEMPT_FINISHED:
      handleReduceAttemptFinishedEvent((ReduceAttemptFinishedEvent) event);
      break;
    case SETUP_ATTEMPT_FINISHED:
    case CLEANUP_ATTEMPT_FINISHED:
      handleTaskAttemptFinishedEvent((TaskAttemptFinishedEvent) event);
      break;
    case AM_STARTED:
      handleAMStartedEvent((AMStartedEvent) event);
      break;
    default:
      break;
    }
  }
  
  private void handleTaskAttemptFinishedEvent(TaskAttemptFinishedEvent event) {
    // 获取任务和尝试信息对象
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getAttemptId());
    // 填充完成事件信息
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.state = StringInterner.weakIntern(event.getState());
    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    // 添加到已完成尝试集合
    info.completedTaskAttemptsMap.put(event.getAttemptId(), attemptInfo);
  }

  private void handleReduceAttemptFinishedEvent
  (ReduceAttemptFinishedEvent event) {
    // 获取任务和尝试信息对象
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getAttemptId());
    // 填充Reduce尝试完成信息，包含shuffle和sort阶段时间
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.state = StringInterner.weakIntern(event.getState());
    attemptInfo.shuffleFinishTime = event.getShuffleFinishTime();
    attemptInfo.sortFinishTime = event.getSortFinishTime();
    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    attemptInfo.port = event.getPort();
    attemptInfo.rackname = StringInterner.weakIntern(event.getRackName());
    // 添加到已完成尝试集合
    info.completedTaskAttemptsMap.put(event.getAttemptId(), attemptInfo);
  }

  private void handleMapAttemptFinishedEvent(MapAttemptFinishedEvent event) {
    // 获取任务和尝试信息对象
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getAttemptId());
    // 填充Map尝试完成信息，包含map阶段完成时间
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.state = StringInterner.weakIntern(event.getState());
    attemptInfo.mapFinishTime = event.getMapFinishTime();
    attemptInfo.counters = event.getCounters();
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    attemptInfo.port = event.getPort();
    attemptInfo.rackname = StringInterner.weakIntern(event.getRackName());
    // 添加到已完成尝试集合
    info.completedTaskAttemptsMap.put(event.getAttemptId(), attemptInfo);
  }

  private void handleTaskAttemptFailedEvent(
      TaskAttemptUnsuccessfulCompletionEvent event) {
    // 获取任务信息对象
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    if(taskInfo == null) {
      LOG.warn("TaskInfo is null for TaskAttemptUnsuccessfulCompletionEvent"
          + " taskId:  " + event.getTaskId().toString());
      return;
    }
    // 获取尝试信息对象
    TaskAttemptInfo attemptInfo = 
      taskInfo.attemptsMap.get(event.getTaskAttemptId());
    if(attemptInfo == null) {
      LOG.warn("AttemptInfo is null for TaskAttemptUnsuccessfulCompletionEvent"
          + " taskAttemptId:  " + event.getTaskAttemptId().toString());
      return;
    }
    // 填充失败事件信息
    attemptInfo.finishTime = event.getFinishTime();
    attemptInfo.error = StringInterner.weakIntern(event.getError());
    attemptInfo.status = StringInterner.weakIntern(event.getTaskStatus());
    attemptInfo.hostname = StringInterner.weakIntern(event.getHostname());
    attemptInfo.port = event.getPort();
    attemptInfo.rackname = StringInterner.weakIntern(event.getRackName());
    attemptInfo.shuffleFinishTime = event.getFinishTime();
    attemptInfo.sortFinishTime = event.getFinishTime();
    attemptInfo.mapFinishTime = event.getFinishTime();
    attemptInfo.counters = event.getCounters();
    // 如果任务之前标记为成功，且当前失败的正是成功尝试，则重置任务成功状态
    if(TaskStatus.State.SUCCEEDED.toString().equals(taskInfo.status))
    {
      if(attemptInfo.getAttemptId().equals(taskInfo.getSuccessfulAttemptId()))
      {
        taskInfo.counters = null;
        taskInfo.finishTime = -1;
        taskInfo.status = null;
        taskInfo.successfulAttemptId = null;
      }
    }
    // 添加到已完成尝试集合
    info.completedTaskAttemptsMap.put(event.getTaskAttemptId(), attemptInfo);
  }

  private void handleTaskAttemptStartedEvent(TaskAttemptStartedEvent event) {
    TaskAttemptID attemptId = event.getTaskAttemptId();
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    
    // 创建新的尝试信息对象，填充启动信息
    TaskAttemptInfo attemptInfo = new TaskAttemptInfo();
    attemptInfo.startTime = event.getStartTime();
    attemptInfo.attemptId = event.getTaskAttemptId();
    attemptInfo.httpPort = event.getHttpPort();
    attemptInfo.trackerName = StringInterner.weakIntern(event.getTrackerName());
    attemptInfo.taskType = event.getTaskType();
    attemptInfo.shufflePort = event.getShufflePort();
    attemptInfo.containerId = event.getContainerId();
    
    // 添加到任务的尝试映射表
    taskInfo.attemptsMap.put(attemptId, attemptInfo);
  }

  private void handleTaskFinishedEvent(TaskFinishedEvent event) {
    // 获取任务信息对象，填充完成信息
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    taskInfo.counters = event.getCounters();
    taskInfo.finishTime = event.getFinishTime();
    taskInfo.status = TaskStatus.State.SUCCEEDED.toString();
    taskInfo.successfulAttemptId = event.getSuccessfulTaskAttemptId();
  }

  private void handleTaskUpdatedEvent(TaskUpdatedEvent event) {
    // 更新任务完成时间
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    taskInfo.finishTime = event.getFinishTime();
  }

  private void handleTaskFailedEvent(TaskFailedEvent event) {
    // 获取任务信息对象，填充失败信息
    TaskInfo taskInfo = info.tasksMap.get(event.getTaskId());
    taskInfo.status = TaskStatus.State.FAILED.toString();
    taskInfo.finishTime = event.getFinishTime();
    taskInfo.error = StringInterner.weakIntern(event.getError());
    taskInfo.failedDueToAttemptId = event.getFailedAttemptID();
    taskInfo.counters = event.getCounters();
  }

  private void handleTaskStartedEvent(TaskStartedEvent event) {
    // 创建新的任务信息对象，填充启动信息
    TaskInfo taskInfo = new TaskInfo();
    taskInfo.taskId = event.getTaskId();
    taskInfo.startTime = event.getStartTime();
    taskInfo.taskType = event.getTaskType();
    taskInfo.splitLocations = event.getSplitLocations();
    // 添加到作业的任务映射表
    info.tasksMap.put(event.getTaskId(), taskInfo);
  }

  private void handleJobFailedEvent(JobUnsuccessfulCompletionEvent event) {
    // 填充作业失败/杀死/错误信息，统计任务完成情况
    info.finishTime = event.getFinishTime();
    info.succeededMaps = event.getSucceededMaps();
    info.succeededReduces = event.getSucceededReduces();
    info.failedMaps = event.getFailedMaps();
    info.failedReduces = event.getFailedReduces();
    info.killedMaps = event.getKilledMaps();
    info.killedReduces = event.getKilledReduces();
    info.jobStatus = StringInterner.weakIntern(event.getStatus());
    info.errorInfo = StringInterner.weakIntern(event.getDiagnostics());
  }

  private void handleJobFinishedEvent(JobFinishedEvent event) {
    // 填充作业成功完成信息，统计任务完成情况和计数器
    info.finishTime = event.getFinishTime();
    info.succeededMaps = event.getSucceededMaps();
    info.succeededReduces = event.getSucceededReduces();
    info.failedMaps = event.get