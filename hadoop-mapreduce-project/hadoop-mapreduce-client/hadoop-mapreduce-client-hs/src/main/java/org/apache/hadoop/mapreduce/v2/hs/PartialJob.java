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
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobIndexInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 历史服务器中基于作业索引信息的轻量级不完整Job实现
 * 仅包含作业基本元信息，不加载完整任务详情，用于作业列表展示等场景
 * 减少历史作业浏览时的内存占用和加载时间
 */
public class PartialJob implements Job {
  private static final Logger LOG = LoggerFactory.getLogger(PartialJob.class);

  private JobIndexInfo jobIndexInfo = null;
  private JobId jobId = null;
  private JobReport jobReport = null;
  
  /**
   * 构造不完整Job对象，仅使用作业索引信息初始化基本信息
   * @param jobIndexInfo 作业索引信息，从历史作业索引文件读取
   * @param jobId 作业ID
   */
  public PartialJob(JobIndexInfo jobIndexInfo, JobId jobId) {
    this.jobIndexInfo = jobIndexInfo;
    this.jobId = jobId;
    // 创建并初始化JobReport对象
    jobReport = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobReport.class);
    jobReport.setSubmitTime(jobIndexInfo.getSubmitTime());
    jobReport.setStartTime(jobIndexInfo.getJobStartTime());
    jobReport.setFinishTime(jobIndexInfo.getFinishTime());
    jobReport.setJobState(getState());
  }
  
  @Override
  public JobId getID() {
//    return jobIndexInfo.getJobId();
    return this.jobId;
  }

  @Override
  public String getName() {
    return jobIndexInfo.getJobName();
  }

  @Override
  public String getQueueName() {
    return jobIndexInfo.getQueueName();
  }

  @Override
  /**
   * 从作业索引信息解析获取作业最终状态
   * @return 解析后的作业状态，解析失败默认返回KILLED
   */
  public JobState getState() {
    JobState js = null;
    try {
      js = JobState.valueOf(jobIndexInfo.getJobStatus());
    } catch (Exception e) {
      // 解析异常不阻塞UI渲染，默认设置为KILLED
      LOG.warn("Exception while parsing job state. Defaulting to KILLED", e);
      js = JobState.KILLED;
    }
    return js;
  }

  @Override
  public JobReport getReport() {
    return jobReport;
  }

  @Override
  public float getProgress() {
    // 历史作业已完成，进度固定为100%
    return 1.0f;
  }

  @Override
  public Counters getAllCounters() {
    // 不完整Job不加载计数器信息
    return null;
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    // 不完整Job不加载任务列表
    return null;
  }

  @Override
  public Map<TaskId, Task> getTasks(TaskType taskType) {
    // 不完整Job不加载任务列表
    return null;
  }

  @Override
  public Task getTask(TaskId taskID) {
    // 不完整Job不提供任务详情
    return null;
  }

  @Override
  public List<String> getDiagnostics() {
    // 不完整Job不加载诊断信息
    return null;
  }

  @Override
  public int getTotalMaps() {
    return jobIndexInfo.getNumMaps();
  }

  @Override
  public int getTotalReduces() {
    return jobIndexInfo.getNumReduces();
  }

  @Override
  public int getCompletedMaps() {
    // 历史作业已完成，所有任务均已完成
    return jobIndexInfo.getNumMaps();
  }

  @Override
  public int getCompletedReduces() {
    // 历史作业已完成，所有任务均已完成
    return jobIndexInfo.getNumReduces();
  }

  @Override
  public boolean isUber() {
    // 不完整Job不支持uber模式信息，默认返回false
    return false;
  }

  @Override
  public TaskAttemptCompletionEvent[] getTaskAttemptCompletionEvents(
      int fromEventId, int maxEvents) {
    // 不完整Job不加载完成事件
    return null;
  }

  @Override
  public TaskCompletionEvent[] getMapAttemptCompletionEvents(
      int startIndex, int maxEvents) {
    // 不完整Job不加载完成事件
    return null;
  }

  @Override
  public boolean checkAccess(UserGroupInformation callerUGI, JobACL jobOperation) {
    // 不完整Job默认允许访问，完整权限检查在加载完整Job后执行
    return true;
  }
  
  @Override
  public String getUserName() {
    return jobIndexInfo.getUser();
  }

  @Override
  public Path getConfFile() {
    throw new IllegalStateException("Not implemented yet");
  }
  
  @Override
  public Configuration loadConfFile() {
    throw new IllegalStateException("Not implemented yet");
  }

  @Override
  public Map<JobACL, AccessControlList> getJobACLs() {
    throw new IllegalStateException("Not implemented yet");
  }

  @Override
  public List<AMInfo> getAMInfos() {
    // 不完整Job不加载AM信息
    return null;
  }
  
  @Override
  public void setQueueName(String queueName) {
    throw new UnsupportedOperationException("Can't set job's queue name in history");
  }

  @Override
  public void setJobPriority(Priority priority) {
    throw new UnsupportedOperationException(
        "Can't set job's priority in history");
  }

  @Override
  public int getFailedMaps() {
    // 不完整Job不提供失败任务统计，返回-1表示无数据
    return -1;
  }

  @Override
  public int getFailedReduces() {
    // 不完整Job不提供失败任务统计，返回-1表示无数据
    return -1;
  }

  @Override
  public int getKilledMaps() {
    // 不完整Job不提供被杀死任务统计，返回-1表示无数据
    return -1;
  }

  @Override
  public int getKilledReduces() {
    // 不完整Job不提供被杀死任务统计，返回-1表示无数据
    return -1;
  }
}