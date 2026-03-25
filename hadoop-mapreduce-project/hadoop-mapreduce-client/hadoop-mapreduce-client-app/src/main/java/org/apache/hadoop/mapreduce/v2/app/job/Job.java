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

package org.apache.hadoop.mapreduce.v2.app.job;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;


/**
 * MapReduce作业的核心接口，定义了与作业交互的所有抽象方法，供ApplicationMaster管理作业生命周期使用
 */
public interface Job {

  /**
   * 获取当前作业的唯一ID
   * @return 作业ID对象
   */
  JobId getID();

  /**
   * 获取当前作业的名称
   * @return 作业名称字符串
   */
  String getName();

  /**
   * 获取当前作业的状态
   * @return 作业状态枚举
   */
  JobState getState();

  /**
   * 获取当前作业的完整报告
   * @return 作业报告对象，包含状态、进度等信息
   */
  JobReport getReport();

  /**
   * Get all the counters of this job. This includes job-counters aggregated
   * together with the counters of each task. This creates a clone of the
   * Counters, so use this judiciously.  
   * @return job-counters and aggregate task-counters
   */
  Counters getAllCounters();

  /**
   * 获取作业中所有任务的映射表
   * @return 任务ID到任务对象的映射
   */
  Map<TaskId,Task> getTasks();

  /**
   * 获取指定类型任务的映射表
   * @param taskType 任务类型（MAP/REDUCE）
   * @return 符合类型的任务ID到任务对象的映射
   */
  Map<TaskId,Task> getTasks(TaskType taskType);

  /**
   * 根据任务ID获取对应任务对象
   * @param taskID 任务ID
   * @return 对应任务对象
   */
  Task getTask(TaskId taskID);

  /**
   * 获取作业的诊断信息列表
   * @return 诊断信息字符串列表
   */
  List<String> getDiagnostics();

  /**
   * 获取作业总的Map任务数
   * @return 总Map任务数
   */
  int getTotalMaps();

  /**
   * 获取作业总的Reduce任务数
   * @return 总Reduce任务数
   */
  int getTotalReduces();

  /**
   * 获取已完成的Map任务数
   * @return 已完成Map任务数
   */
  int getCompletedMaps();

  /**
   * 获取已完成的Reduce任务数
   * @return 已完成Reduce任务数
   */
  int getCompletedReduces();

  /**
   * 获取失败的Map任务数
   * @return 失败Map任务数
   */
  int getFailedMaps();

  /**
   * 获取失败的Reduce任务数
   * @return 失败Reduce任务数
   */
  int getFailedReduces();

  /**
   * 获取被杀死的Map任务数
   * @return 被杀死Map任务数
   */
  int getKilledMaps();

  /**
   * 获取被杀死的Reduce任务数
   * @return 被杀死Reduce任务数
   */
  int getKilledReduces();

  /**
   * 获取作业整体完成进度
   * @return 进度值0-1
   */
  float getProgress();

  /**
   * 判断是否是Uber模式（小作业所有任务在同一个JVM中执行）
   * @return true为Uber模式，false为普通模式
   */
  boolean isUber();

  /**
   * 获取提交作业的用户名
   * @return 提交用户名
   */
  String getUserName();

  /**
   * 获取作业所在队列名称
   * @return 队列名称
   */
  String getQueueName();
  
  /**
   * @return a path to where the config file for this job is located.
   */
  Path getConfFile();
  
  /**
   * @return a parsed version of the config files pointed to by 
   * {@link #getConfFile()}.
   * @throws IOException on any error trying to load the conf file. 
   */
  Configuration loadConfFile() throws IOException;
  
  /**
   * @return the ACLs for this job for each type of JobACL given. 
   */
  Map<JobACL, AccessControlList> getJobACLs();

  /**
   * 获取指定范围的任务尝试完成事件
   * @param fromEventId 起始事件ID
   * @param maxEvents 最大返回事件数
   * @return 任务尝试完成事件数组
   */
  TaskAttemptCompletionEvent[]
      getTaskAttemptCompletionEvents(int fromEventId, int maxEvents);

  /**
   * 获取指定范围的Map尝试完成事件（兼容旧MapReduce API）
   * @param startIndex 起始索引
   * @param maxEvents 最大返回事件数
   * @return 任务完成事件数组
   */
  TaskCompletionEvent[]
      getMapAttemptCompletionEvents(int startIndex, int maxEvents);

  /**
   * @return information for MR AppMasters (previously failed and current)
   */
  List<AMInfo> getAMInfos();
  
  /**
   * 检查指定用户是否有权限执行作业操作
   * @param callerUGI 调用者用户信息
   * @param jobOperation 请求的操作类型
   * @return true有权限，false无权限
   */
  boolean checkAccess(UserGroupInformation callerUGI, JobACL jobOperation);
  
  /**
   * 设置作业所在队列名称
   * @param queueName 目标队列名称
   */
  public void setQueueName(String queueName);

  /**
   * 设置作业优先级
   * @param priority 目标优先级
   */
  public void setJobPriority(Priority priority);
}