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

package org.apache.hadoop.mapreduce.v2.api.records;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * TaskAttempt运行报告接口，定义了获取和设置任务尝试运行状态信息的标准方法
 * 用于在MapReduce框架中传递任务尝试的运行进度、状态、时间等监控信息
 */
public interface TaskAttemptReport {
  /**
   * 获取任务尝试ID
   * @return 任务尝试唯一标识
   */
  public abstract TaskAttemptId getTaskAttemptId();
  /**
   * 获取任务尝试当前状态
   * @return 任务尝试状态枚举
   */
  public abstract TaskAttemptState getTaskAttemptState();
  /**
   * 获取任务尝试执行进度
   * @return 进度值，范围0-1
   */
  public abstract float getProgress();
  /**
   * 获取任务尝试启动时间戳
   * @return 启动时间（毫秒）
   */
  public abstract long getStartTime();
  /**
   * 获取任务尝试完成时间戳
   * @return 完成时间（毫秒）
   */
  public abstract long getFinishTime();
  /** @return the shuffle finish time. Applicable only for reduce attempts */
  public abstract long getShuffleFinishTime();
  /** @return the sort/merge finish time. Applicable only for reduce attempts */
  public abstract long getSortFinishTime();
  /**
   * 获取任务尝试计数器
   * @return 任务运行指标计数器集合
   */
  public abstract Counters getCounters();
  /**
   * 获取MapReduce旧API格式的原始计数器
   * @return 旧API格式计数器对象
   */
  public abstract org.apache.hadoop.mapreduce.Counters getRawCounters();
  /**
   * 获取诊断信息
   * @return 任务尝试失败/异常诊断文本
   */
  public abstract String getDiagnosticInfo();
  /**
   * 获取自定义状态描述字符串
   * @return 自定义状态文本
   */
  public abstract String getStateString();
  /**
   * 获取任务尝试当前执行阶段
   * @return 执行阶段枚举
   */
  public abstract Phase getPhase();
  /**
   * 获取运行该任务尝试的NodeManager主机地址
   * @return NodeManager主机名/IP
   */
  public abstract String getNodeManagerHost();
  /**
   * 获取运行该任务尝试的NodeManager服务端口
   * @return NodeManager RPC端口
   */
  public abstract int getNodeManagerPort();
  /**
   * 获取运行该任务尝试的NodeManager HTTP服务端口
   * @return NodeManager HTTP端口
   */
  public abstract int getNodeManagerHttpPort();
  /**
   * 获取运行该任务尝试的YARN容器ID
   * @return YARN容器唯一标识
   */
  public abstract ContainerId getContainerId();

  /**
   * 设置任务尝试ID
   * @param taskAttemptId 任务尝试唯一标识
   */
  public abstract void setTaskAttemptId(TaskAttemptId taskAttemptId);
  /**
   * 设置任务尝试状态
   * @param taskAttemptState 任务尝试状态枚举
   */
  public abstract void setTaskAttemptState(TaskAttemptState taskAttemptState);
  /**
   * 设置任务尝试执行进度
   * @param progress 进度值，范围0-1
   */
  public abstract void setProgress(float progress);
  /**
   * 设置任务尝试启动时间戳
   * @param startTime 启动时间（毫秒）
   */
  public abstract void setStartTime(long startTime);
  /**
   * 设置任务尝试完成时间戳
   * @param finishTime 完成时间（毫秒）
   */
  public abstract void setFinishTime(long finishTime);
  /**
   * 设置任务尝试计数器
   * @param counters 任务运行指标计数器集合
   */
  public abstract void setCounters(Counters counters);
  /**
   * 设置MapReduce旧API格式的原始计数器
   * @param rCounters 旧API格式计数器对象
   */
  public abstract void
      setRawCounters(org.apache.hadoop.mapreduce.Counters rCounters);
  /**
   * 设置诊断信息
   * @param diagnosticInfo 任务尝试失败/异常诊断文本
   */
  public abstract void setDiagnosticInfo(String diagnosticInfo);
  /**
   * 设置自定义状态描述字符串
   * @param stateString 自定义状态文本
   */
  public abstract void setStateString(String stateString);
  /**
   * 设置任务尝试当前执行阶段
   * @param phase 执行阶段枚举
   */
  public abstract void setPhase(Phase phase);
  /**
   * 设置运行该任务尝试的NodeManager主机地址
   * @param nmHost NodeManager主机名/IP
   */
  public abstract void setNodeManagerHost(String nmHost);
  /**
   * 设置运行该任务尝试的NodeManager服务端口
   * @param nmPort NodeManager RPC端口
   */
  public abstract void setNodeManagerPort(int nmPort);
  /**
   * 设置运行该任务尝试的NodeManager HTTP服务端口
   * @param nmHttpPort NodeManager HTTP端口
   */
  public abstract void setNodeManagerHttpPort(int nmHttpPort);
  /**
   * 设置运行该任务尝试的YARN容器ID
   * @param containerId YARN容器唯一标识
   */
  public abstract void setContainerId(ContainerId containerId);
  
  /** 
   * Set the shuffle finish time. Applicable only for reduce attempts
   * @param time the time the shuffle finished.
   */
  public abstract void setShuffleFinishTime(long time);
  /** 
   * Set the sort/merge finish time. Applicable only for reduce attempts
   * @param time the time the shuffle finished.
   */
  public abstract void setSortFinishTime(long time);
}