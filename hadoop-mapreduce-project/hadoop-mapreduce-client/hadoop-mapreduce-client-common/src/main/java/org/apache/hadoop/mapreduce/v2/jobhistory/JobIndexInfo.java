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

package org.apache.hadoop.mapreduce.v2.jobhistory;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * 保存作业历史索引系统所需的作业基础索引信息，用于作业历史的检索与索引构建
 */
public class JobIndexInfo {
  private long submitTime;
  private long finishTime;
  private String user;
  private String queueName;
  private String jobName;
  private JobId jobId;
  private int numMaps;
  private int numReduces;
  private String jobStatus;
  private long jobStartTime;
  
  /**
   * 构造空的作业索引信息对象
   */
  public JobIndexInfo() {
  }
  
  /**
   * 构造作业索引信息对象，使用默认队列名
   * @param submitTime 作业提交时间戳
   * @param finishTime 作业完成时间戳
   * @param user 提交作业的用户名
   * @param jobName 作业名称
   * @param jobId 作业唯一标识
   * @param numMaps Map任务数量
   * @param numReduces Reduce任务数量
   * @param jobStatus 作业最终状态
   */
  public JobIndexInfo(long submitTime, long finishTime, String user,
      String jobName, JobId jobId, int numMaps, int numReduces, String jobStatus) {
    this(submitTime, finishTime, user, jobName, jobId, numMaps, numReduces,
         jobStatus, JobConf.DEFAULT_QUEUE_NAME);
  }

  /**
   * 构造完整的作业索引信息对象
   * @param submitTime 作业提交时间戳
   * @param finishTime 作业完成时间戳
   * @param user 提交作业的用户名
   * @param jobName 作业名称
   * @param jobId 作业唯一标识
   * @param numMaps Map任务数量
   * @param numReduces Reduce任务数量
   * @param jobStatus 作业最终状态
   * @param queueName 作业所属队列名称
   */
  public JobIndexInfo(long submitTime, long finishTime, String user,
                      String jobName, JobId jobId, int numMaps, int numReduces,
                      String jobStatus, String queueName) {
    this.submitTime = submitTime;
    this.finishTime = finishTime;
    this.user = user;
    this.jobName = jobName;
    this.jobId = jobId;
    this.numMaps = numMaps;
    this.numReduces = numReduces;
    this.jobStatus = jobStatus;
    this.jobStartTime = -1;
    this.queueName = queueName;
  }

  /**
   * 获取作业提交时间戳
   * @return 作业提交时间戳
   */
  public long getSubmitTime() {
    return submitTime;
  }

  /**
   * 设置作业提交时间戳
   * @param submitTime 作业提交时间戳
   */
  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  /**
   * 获取作业完成时间戳
   * @return 作业完成时间戳
   */
  public long getFinishTime() {
    return finishTime;
  }

  /**
   * 设置作业完成时间戳
   * @param finishTime 作业完成时间戳
   */
  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  /**
   * 获取提交作业的用户名
   * @return 用户名
   */
  public String getUser() {
    return user;
  }

  /**
   * 设置提交作业的用户名
   * @param user 用户名
   */
  public void setUser(String user) {
    this.user = user;
  }

  /**
   * 获取作业所属队列名称
   * @return 队列名称
   */
  public String getQueueName() {
    return queueName;
  }

  /**
   * 设置作业所属队列名称
   * @param queueName 队列名称
   */
  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  /**
   * 获取作业名称
   * @return 作业名称
   */
  public String getJobName() {
    return jobName;
  }

  /**
   * 设置作业名称
   * @param jobName 作业名称
   */
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  /**
   * 获取作业唯一标识
   * @return 作业ID对象
   */
  public JobId getJobId() {
    return jobId;
  }

  /**
   * 设置作业唯一标识
   * @param jobId 作业ID对象
   */
  public void setJobId(JobId jobId) {
    this.jobId = jobId;
  }

  /**
   * 获取Map任务数量
   * @return Map任务数量
   */
  public int getNumMaps() {
    return numMaps;
  }

  /**
   * 设置Map任务数量
   * @param numMaps Map任务数量
   */
  public void setNumMaps(int numMaps) {
    this.numMaps = numMaps;
  }

  /**
   * 获取Reduce任务数量
   * @return Reduce任务数量
   */
  public int getNumReduces() {
    return numReduces;
  }

  /**
   * 设置Reduce任务数量
   * @param numReduces Reduce任务数量
   */
  public void setNumReduces(int numReduces) {
    this.numReduces = numReduces;
  }

  /**
   * 获取作业最终状态
   * @return 作业状态字符串
   */
  public String getJobStatus() {
    return jobStatus;
  }

  /**
   * 设置作业最终状态
   * @param jobStatus 作业状态字符串
   */
  public void setJobStatus(String jobStatus) {
    this.jobStatus = jobStatus;
  }

  /**
   * 获取作业开始运行时间戳
   * @return 作业开始运行时间戳
   */
  public long getJobStartTime() {
      return jobStartTime;
  }

  /**
   * 设置作业开始运行时间戳
   * @param lTime 作业开始运行时间戳
   */
  public void setJobStartTime(long lTime) {
      this.jobStartTime = lTime;
  }

  @Override
  public String toString() {
    return "JobIndexInfo [submitTime=" + submitTime + ", finishTime="
        + finishTime + ", user=" + user + ", jobName=" + jobName + ", jobId="
        + jobId + ", numMaps=" + numMaps + ", numReduces=" + numReduces
        + ", jobStatus=" + jobStatus + "]";
  }
  
  
}