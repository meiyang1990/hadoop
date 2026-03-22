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
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Times;

/**
 * MapReduce任务尝试信息的抽象数据访问对象，为Web UI提供任务尝试的基础信息封装
 * 作为MapTaskAttemptInfo和ReduceTaskAttemptInfo的基类，支持XML序列化
 */
@XmlRootElement(name = "taskAttempt")
@XmlSeeAlso({MapTaskAttemptInfo.class, ReduceTaskAttemptInfo.class})
@XmlAccessorType(XmlAccessType.FIELD)
public abstract class TaskAttemptInfo {

  protected long startTime;
  protected long finishTime;
  protected long elapsedTime;
  protected float progress;
  protected String id;
  protected String rack;
  protected TaskAttemptState state;
  protected String status;
  protected String nodeHttpAddress;
  protected String diagnostics;
  protected String type;
  protected String assignedContainerId;

  @XmlTransient
  protected ContainerId assignedContainer;

  /**
   * 无参构造函数，供JAXB序列化使用
   */
  public TaskAttemptInfo() {
  }

  /**
   * 从任务尝试实体构造任务尝试信息对象，提取并封装所需展示信息
   * @param ta 任务尝试实体对象
   * @param type 任务类型（Map/Reduce）
   * @param isRunning 任务尝试是否仍在运行
   */
  public TaskAttemptInfo(TaskAttempt ta, TaskType type, Boolean isRunning) {
    final TaskAttemptReport report = ta.getReport();
    this.type = type.toString();
    this.id = MRApps.toString(ta.getID());
    this.nodeHttpAddress = ta.getNodeHttpAddress();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.assignedContainer = report.getContainerId();
    // 如果分配了容器，保存容器ID字符串用于展示
    if (assignedContainer != null) {
      this.assignedContainerId = assignedContainer.toString();
    }
    this.progress = report.getProgress() * 100;
    this.status = report.getStateString();
    this.state = report.getTaskAttemptState();
    // 计算任务尝试已运行时间
    this.elapsedTime = Times
        .elapsed(this.startTime, this.finishTime, isRunning);
    // 处理异常时间结果，默认置为0
    if (this.elapsedTime == -1) {
      this.elapsedTime = 0;
    }
    this.diagnostics = report.getDiagnosticInfo();
    this.rack = ta.getNodeRackName();
  }

  /**
   * 获取分配容器ID的字符串形式
   * @return 分配容器ID字符串
   */
  public String getAssignedContainerIdStr() {
    return this.assignedContainerId;
  }

  /**
   * 获取分配容器ID对象
   * @return 容器ID对象
   */
  public ContainerId getAssignedContainerId() {
    return this.assignedContainer;
  }

  /**
   * 获取任务尝试状态字符串
   * @return 任务尝试状态字符串
   */
  public String getState() {
    return this.state.toString();
  }

  /**
   * 获取任务尝试状态描述
   * @return 状态描述字符串
   */
  public String getStatus() {
    return status;
  }

  /**
   * 获取任务尝试ID字符串
   * @return 任务尝试ID字符串
   */
  public String getId() {
    return this.id;
  }

  /**
   * 获取任务尝试开始时间戳
   * @return 开始时间戳
   */
  public long getStartTime() {
    return this.startTime;
  }

  /**
   * 获取任务尝试结束时间戳
   * @return 结束时间戳
   */
  public long getFinishTime() {
    return this.finishTime;
  }

  /**
   * 获取任务尝试进度百分比
   * @return 进度百分比（0-100）
   */
  public float getProgress() {
    return this.progress;
  }

  /**
   * 获取任务尝试已运行时间（毫秒）
   * @return 已运行时间（毫秒）
   */
  public long getElapsedTime() {
    return this.elapsedTime;
  }

  /**
   * 获取运行节点的HTTP地址
   * @return 节点HTTP地址
   */
  public String getNode() {
    return this.nodeHttpAddress;
  }

  /**
   * 获取运行节点所在机架名称
   * @return 机架名称
   */
  public String getRack() {
    return this.rack;
  }

  /**
   * 获取任务尝试诊断信息
   * @return 诊断信息字符串
   */
  public String getNote() {
    return this.diagnostics;
  }
}