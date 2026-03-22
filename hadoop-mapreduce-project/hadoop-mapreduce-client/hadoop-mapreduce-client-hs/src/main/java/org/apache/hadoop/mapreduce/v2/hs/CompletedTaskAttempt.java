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
import java.util.List;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;

/**
 * 已完成任务尝试的历史实体，存储从作业历史解析出的已完成任务尝试信息
 * 用于历史服务器中展示已完成任务尝试的状态、指标和诊断信息
 * 实现了TaskAttempt接口，提供对已完成任务尝试信息的访问
 */
public class CompletedTaskAttempt implements TaskAttempt {

  private final TaskAttemptInfo attemptInfo;
  private final TaskAttemptId attemptId;
  private final TaskAttemptState state;
  private final List<String> diagnostics = new ArrayList<String>(2);
  private TaskAttemptReport report;

  private String localDiagMessage;

  /**
   * 构造已完成任务尝试对象，从历史解析信息中初始化任务尝试数据
   * @param taskId 所属任务ID
   * @param attemptInfo 作业历史解析得到的任务尝试信息
   */
  CompletedTaskAttempt(TaskId taskId, TaskAttemptInfo attemptInfo) {
    this.attemptInfo = attemptInfo;
    this.attemptId = TypeConverter.toYarn(attemptInfo.getAttemptId());
    if (attemptInfo.getTaskStatus() != null) {
      this.state = TaskAttemptState.valueOf(attemptInfo.getTaskStatus());
    } else {
      this.state = TaskAttemptState.KILLED;
      localDiagMessage = "Attmpt state missing from History : marked as KILLED";
      diagnostics.add(localDiagMessage);
    }
    if (attemptInfo.getError() != null) {
      diagnostics.add(attemptInfo.getError());
    }
  }

  @Override
  /**
   * 获取节点ID，历史查询场景不支持此操作，直接抛出异常
   */
  public NodeId getNodeId() throws UnsupportedOperationException{
    throw new UnsupportedOperationException();
  }
  
  @Override
  /**
   * 获取任务尝试分配的容器ID
   * @return 容器ID，从历史信息中获取
   */
  public ContainerId getAssignedContainerID() {
    return attemptInfo.getContainerId();
  }

  @Override
  /**
   * 获取分配容器的NodeManager地址
   * @return NodeManager地址(host:port格式)，从历史信息构建
   */
  public String getAssignedContainerMgrAddress() {
    return attemptInfo.getHostname() + ":" + attemptInfo.getPort();
  }

  @Override
  /**
   * 获取节点的HTTP服务地址
   * @return 节点HTTP地址，从历史信息构建
   */
  public String getNodeHttpAddress() {
    return attemptInfo.getTrackerName() + ":" + attemptInfo.getHttpPort();
  }
  
  @Override
  /**
   * 获取节点所在机架名称
   * @return 机架名称，从历史信息获取
   */
  public String getNodeRackName() {
    return attemptInfo.getRackname();
  }

  @Override
  /**
   * 获取任务尝试的计数器
   * @return 计数器对象，从历史信息获取
   */
  public Counters getCounters() {
    return attemptInfo.getCounters();
  }

  @Override
  /**
   * 获取当前任务尝试ID
   * @return 任务尝试ID
   */
  public TaskAttemptId getID() {
    return attemptId;
  }

  @Override
  /**
   * 获取任务尝试进度，已完成任务进度固定为100%
   * @return 进度值，固定为1.0
   */
  public float getProgress() {
    return 1.0f;
  }

  @Override
  /**
   * 获取任务尝试报告，延迟构造报告对象保证线程安全
   * @return 任务尝试报告，包含所有历史信息
   */
  public synchronized TaskAttemptReport getReport() {
    if (report == null) {
      constructTaskAttemptReport();
    }
    return report;
  }

  @Override
  /**
   * 获取任务尝试所处阶段，已完成任务固定为清理阶段
   * @return 阶段，固定为CLEANUP
   */
  public Phase getPhase() {
    return Phase.CLEANUP;
  }

  @Override
  /**
   * 获取任务尝试最终状态
   * @return 任务尝试状态
   */
  public TaskAttemptState getState() {
    return state;
  }

  @Override
  /**
   * 判断任务尝试是否已完成，已完成任务固定返回true
   * @return 固定为true
   */
  public boolean isFinished() {
    return true;
  }

  @Override
  /**
   * 获取任务尝试的诊断信息列表
   * @return 诊断信息列表，包含错误和异常信息
   */
  public List<String> getDiagnostics() {
    return diagnostics;
  }

  @Override
  /**
   * 获取任务尝试启动时间
   * @return 启动时间戳，从历史信息获取
   */
  public long getLaunchTime() {
    return attemptInfo.getStartTime();
  }

  @Override
  /**
   * 获取任务尝试完成时间
   * @return 完成时间戳，从历史信息获取
   */
  public long getFinishTime() {
    return attemptInfo.getFinishTime();
  }
  
  @Override
  /**
   * 获取shuffle阶段完成时间
   * @return shuffle完成时间戳，从历史信息获取
   */
  public long getShuffleFinishTime() {
    return attemptInfo.getShuffleFinishTime();
  }

  @Override
  /**
   * 获取排序阶段完成时间
   * @return 排序完成时间戳，从历史信息获取
   */
  public long getSortFinishTime() {
    return attemptInfo.getSortFinishTime();
  }

  @Override
  /**
   * 获取shuffle服务端口号
   * @return shuffle端口号，从历史信息获取
   */
  public int getShufflePort() {
    return attemptInfo.getShufflePort();
  }

  /**
   * 构造TaskAttemptReport对象，填充所有历史信息到报告中
   */
  private void constructTaskAttemptReport() {
    report = Records.newRecord(TaskAttemptReport.class);

    report.setTaskAttemptId(attemptId);
    report.setTaskAttemptState(state);
    report.setProgress(getProgress());
    report.setStartTime(attemptInfo.getStartTime());
    report.setFinishTime(attemptInfo.getFinishTime());
    report.setShuffleFinishTime(attemptInfo.getShuffleFinishTime());
    report.setSortFinishTime(attemptInfo.getSortFinishTime());
    if (localDiagMessage != null) {
      report
          .setDiagnosticInfo(attemptInfo.getError() + ", " + localDiagMessage);
    } else {
      report.setDiagnosticInfo(attemptInfo.getError());
    }
    // report.setPhase(attemptInfo.get); //TODO
    report.setStateString(attemptInfo.getState());
    report.setRawCounters(getCounters());
    report.setContainerId(attemptInfo.getContainerId());
    if (attemptInfo.getHostname() == null) {
      report.setNodeManagerHost("UNKNOWN");
    } else {
      report.setNodeManagerHost(attemptInfo.getHostname());
      report.setNodeManagerPort(attemptInfo.getPort());
    }
    report.setNodeManagerHttpPort(attemptInfo.getHttpPort());
  }
}