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

import java.util.List;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * MapReduce任务尝试的只读视图接口，定义了获取任务尝试运行状态、运行位置和执行信息的标准方法
 * 供ApplicationMaster查询任务尝试执行情况使用
 */
public interface TaskAttempt {
  /**
   * 获取当前任务尝试的唯一标识ID
   * @return 任务尝试ID
   */
  TaskAttemptId getID();
  
  /**
   * 获取当前任务尝试的完整报告对象
   * @return 任务尝试报告，包含状态、进度等汇总信息
   */
  TaskAttemptReport getReport();
  
  /**
   * 获取当前任务尝试的诊断信息列表，用于错误排查
   * @return 诊断信息字符串列表
   */
  List<String> getDiagnostics();
  
  /**
   * 获取当前任务尝试的所有计数器指标
   * @return 任务执行计数器集合
   */
  Counters getCounters();
  
  /**
   * 获取当前任务尝试的执行进度
   * @return 进度值，范围[0.0, 1.0]
   */
  float getProgress();
  
  /**
   * 获取当前任务尝试所处的执行阶段
   * @return 执行阶段枚举（MAP/SHUFFLE/SORT/REDUCE等）
   */
  Phase getPhase();
  
  /**
   * 获取当前任务尝试的状态
   * @return 任务尝试状态枚举
   */
  TaskAttemptState getState();

  /** 
   * 判断当前任务尝试是否已进入最终状态（无论成功失败）
   * @return true 已完成，false 仍在运行
   */
  boolean isFinished();

  /**
   * 获取当前任务尝试分配所在容器的ID
   * @return 若已分配容器返回容器ID，否则返回null
   */
  ContainerId getAssignedContainerID();

  /**
   * 获取当前任务尝试容器所在NodeManager的地址
   * @return 若已分配容器返回NodeManager地址，否则返回null
   */
  String getAssignedContainerMgrAddress();
  
  /**
   * 获取当前任务尝试容器所在节点的ID
   * @return 若已分配容器返回节点ID，否则返回null
   */
  NodeId getNodeId();
  
  /**
   * 获取当前任务尝试容器所在节点的HTTP服务地址
   * @return 若已分配容器返回节点HTTP地址，否则返回null
   */
  String getNodeHttpAddress();
  
  /**
   * 获取当前任务尝试容器所在节点的机架名称
   * @return 若已分配容器返回机架名称，否则返回null
   */
  String getNodeRackName();

  /** 
   * 获取当前任务尝试容器的启动时间
   * @return 容器启动时间戳，若容器未启动则返回0
   */
  long getLaunchTime();

  /** 
   * 获取当前任务尝试的完成时间
   * @return 尝试完成时间戳，若尝试未完成则返回0
   */
  long getFinishTime();
  
  /**
   * 获取Reduce任务尝试的shuffle阶段完成时间
   * @return shuffle完成时间戳，若未完成或不是Reduce任务则返回0
   */
  long getShuffleFinishTime();

  /**
   * 获取Reduce任务尝试的排序/合并阶段完成时间
   * @return 排序/合并完成时间戳，若未完成或不是Reduce任务则返回0
   */
  long getSortFinishTime();

  /**
   * 获取shuffle服务监听的端口号
   * @return shuffle端口号
   */
  public int getShufflePort();
}