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

package org.apache.hadoop.mapreduce.v2.app;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;


/**
 * MapReduce ApplicationMaster 上下文接口，负责在 ApplicationMaster 各组件之间共享应用级状态信息。
 * 提供应用基本信息、作业管理、事件总线、系统时钟等核心能力的访问入口。
 */
@InterfaceAudience.Private
public interface AppContext {

  /**
   * 获取当前 YARN 应用的全局 ID。
   * @return YARN 应用 ID
   */
  ApplicationId getApplicationID();

  /**
   * 获取当前应用尝试的 ID。
   * @return 当前应用尝试 ID
   */
  ApplicationAttemptId getApplicationAttemptId();

  /**
   * 获取当前应用的名称。
   * @return 应用名称字符串
   */
  String getApplicationName();

  /**
   * 获取应用启动时间戳。
   * @return 应用启动时间（毫秒）
   */
  long getStartTime();

  /**
   * 获取提交当前应用的用户。
   * @return 提交用户标识
   */
  CharSequence getUser();

  /**
   * 根据作业 ID 获取对应的作业实例。
   * @param jobID 作业 ID
   * @return 对应作业实例
   */
  Job getJob(JobId jobID);

  /**
   * 获取当前应用中所有作业的映射表。
   * @return 作业ID到作业实例的全量映射
   */
  Map<JobId, Job> getAllJobs();

  /**
   * 获取应用全局事件处理器，用于向事件总线发送事件。
   * @return 全局事件处理器实例
   */
  EventHandler<Event> getEventHandler();

  /**
   * 获取应用使用的系统时钟实例，用于统一时间计算。
   * @return 时钟实例
   */
  Clock getClock();
  
  /**
   * 获取集群信息，包含集群拓扑等相关信息。
   * @return 集群信息实例
   */
  ClusterInfo getClusterInfo();
  
  /**
   * 获取当前被拉黑的节点列表，这些节点不会被分配任务。
   * @return 拉黑节点地址集合
   */
  Set<String> getBlacklistedNodes();
  
  /**
   * 获取客户端到 ApplicationMaster 令牌的密钥管理器，用于身份认证。
   * @return 客户端到AM令牌密钥管理器
   */
  ClientToAMTokenSecretManager getClientToAMTokenSecretManager();

  /**
   * 判断当前是否为 ApplicationMaster 的最后一次重试。
   * @return true 表示是最后一次重试，false 表示还有重试机会
   */
  boolean isLastAMRetry();

  /**
   * 判断应用是否已经成功向 YARN ResourceManager 注销。
   * @return true 表示已成功注销，false 表示未注销
   */
  boolean hasSuccessfullyUnregistered();

  /**
   * 获取 NodeManager 的主机名，当前 ApplicationMaster 运行在该 NodeManager 上。
   * @return NodeManager 主机名
   */
  String getNMHostname();

  /**
   * 获取任务尝试结束监控器，用于监控正在完成阶段的任务尝试。
   * @return 任务尝试结束监控器实例
   */
  TaskAttemptFinishingMonitor getTaskAttemptFinishingMonitor();

  /**
   * 获取作业历史服务器的访问地址。
   * @return 历史服务 URL
   */
  String getHistoryUrl();

  /**
   * 设置作业历史服务器的访问地址。
   * @param historyUrl 历史服务 URL
   */
  void setHistoryUrl(String historyUrl);
}