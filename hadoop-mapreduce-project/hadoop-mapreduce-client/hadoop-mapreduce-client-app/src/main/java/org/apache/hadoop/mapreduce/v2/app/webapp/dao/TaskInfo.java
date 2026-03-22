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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.util.Times;

/**
 * MapReduce任务信息数据访问对象，用于Web UI序列化展示任务基本信息
 * 封装任务的运行状态、时间、进度等核心指标，供YARN Web UI展示
 */
@XmlRootElement(name = "task")
@XmlAccessorType(XmlAccessType.FIELD)
public class TaskInfo {

  protected long startTime;
  protected long finishTime;
  protected long elapsedTime;
  protected float progress;
  protected String id;
  protected TaskState state;
  protected String type;
  protected String successfulAttempt;
  protected String status;

  @XmlTransient
  int taskNum;

  @XmlTransient
  TaskAttempt successful;

  /**
   * 无参构造函数，供JAXB序列化使用
   */
  public TaskInfo() {
  }

  /**
   * 根据任务对象构造TaskInfo，提取任务核心信息
   * @param task MapReduce任务对象
   */
  public TaskInfo(Task task) {
    TaskType ttype = task.getType();
    this.type = ttype.toString();
    TaskReport report = task.getReport();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.state = report.getTaskState();
    // 计算任务已运行时间，运行中任务使用当前时间计算
    this.elapsedTime = Times.elapsed(this.startTime, this.finishTime,
      this.state == TaskState.RUNNING);
    // 处理异常时间，置为0
    if (this.elapsedTime == -1) {
      this.elapsedTime = 0;
    }
    // 转换进度为百分比
    this.progress = report.getProgress() * 100;
    this.status =  report.getStatus();
    this.id = MRApps.toString(task.getID());
    this.taskNum = task.getID().getId();
    this.successful = getSuccessfulAttempt(task);
    if (successful != null) {
      this.successfulAttempt = MRApps.toString(successful.getID());
    } else {
      this.successfulAttempt = "";
    }
  }

  /**
   * 获取任务进度百分比
   * @return 进度百分比(0-100)
   */
  public float getProgress() {
    return this.progress;
  }

  /**
   * 获取任务状态字符串
   * @return 任务状态字符串
   */
  public String getState() {
    return this.state.toString();
  }

  /**
   * 获取任务ID字符串
   * @return 任务ID字符串
   */
  public String getId() {
    return this.id;
  }

  /**
   * 获取任务编号
   * @return 任务编号
   */
  public int getTaskNum() {
    return this.taskNum;
  }

  /**
   * 获取任务开始时间戳
   * @return 开始时间戳
   */
  public long getStartTime() {
    return this.startTime;
  }

  /**
   * 获取任务结束时间戳
   * @return 结束时间戳
   */
  public long getFinishTime() {
    return this.finishTime;
  }

  /**
   * 获取任务已运行时长
   * @return 运行时长(毫秒)
   */
  public long getElapsedTime() {
    return this.elapsedTime;
  }

  /**
   * 获取成功尝试的ID字符串
   * @return 成功尝试ID
   */
  public String getSuccessfulAttempt() {
    return this.successfulAttempt;
  }

  /**
   * 获取成功的任务尝试对象
   * @return 成功的任务尝试
   */
  public TaskAttempt getSuccessful() {
    return this.successful;
  }

  /**
   * 从任务中查找成功完成的尝试
   * @param task 目标任务
   * @return 成功的尝试对象，无成功尝试返回null
   */
  private TaskAttempt getSuccessfulAttempt(Task task) {
    for (TaskAttempt attempt : task.getAttempts().values()) {
      if (attempt.getState() == TaskAttemptState.SUCCEEDED) {
        return attempt;
      }
    }
    return null;
  }

  /**
   * 获取任务状态描述信息
   * @return 状态描述
   */
  public String getStatus() {
    return status;
  }
}