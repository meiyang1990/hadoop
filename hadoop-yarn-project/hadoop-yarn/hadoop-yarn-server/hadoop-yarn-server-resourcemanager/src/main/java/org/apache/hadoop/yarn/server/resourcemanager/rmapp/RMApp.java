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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * ResourceManager中应用程序的核心接口，定义了应用程序需要提供的所有能力。
 * 具体实现参考{@link RMAppImpl}，该接口对外暴露应用状态查询、更新等核心方法。
 */
public interface RMApp extends EventHandler<RMAppEvent> {

  /**
   * 获取当前应用程序的ApplicationId。
   * @return 当前应用程序的ApplicationId
   */
  ApplicationId getApplicationId();
  
  /**
   * 获取当前应用程序的提交上下文信息。
   * @return 当前应用程序的提交上下文
   */
  ApplicationSubmissionContext getApplicationSubmissionContext();

  /**
   * 获取当前应用程序的内部状态（RMAppState）。
   * @return 当前应用的状态枚举值
   */
  RMAppState getState();

  /**
   * 获取提交该应用程序的用户名。
   * @return 提交应用的用户名
   */
  String getUser();

  /**
   * 获取应用程序的执行进度（0-1）。
   * @return 应用程序进度值
   */
  float getProgress();

  /**
   * 根据尝试ID获取对应的应用尝试实例。
   * @param appAttemptId 应用尝试ID
   * @return 对应ApplicationAttemptId的RMAppAttempt实例
   */
  RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId);

  /**
   * 获取应用程序提交到的队列名称。
   * @return 应用程序所属队列名称
   */
  String getQueue();
  
  /**
   * 更新应用程序所属队列，用于动态修改应用队列。
   * @param name 新队列名称
   */
  void setQueue(String name);

  /**
   * 获取应用程序名称，来自提交上下文。
   * @return 应用程序名称
   */
  String getName();

  /**
   * 获取当前正在运行（或最新）的应用尝试实例。
   * @return 当前活跃的应用尝试实例
   */
  RMAppAttempt getCurrentAppAttempt();

  /**
   * 获取该应用所有的尝试实例，按尝试ID索引。
   * @return 该应用所有尝试实例的映射表
   */
  Map<ApplicationAttemptId, RMAppAttempt> getAppAttempts();

  /**
   * 生成应用程序的状态报告，根据访问权限决定是否返回完整信息。
   * 如果不允许全访问，部分敏感字段会被占位符替换：
   * <ul>
   *   <li>host - 设为"N/A"</li>
   *   <li>RPC port - 设为-1</li>
   *   <li>client token - 设为"N/A"</li>
   *   <li>diagnostics - 设为"N/A"</li>
   *   <li>tracking URL - 设为"N/A"</li>
   *   <li>original tracking URL - 设为"N/A"</li>
   *   <li>resource usage report - 所有值设为-1</li>
   * </ul>
   *
   * @param clientUserName 请求报告的客户端用户名
   * @param allowAccess 是否允许访问完整信息
   * @return 填充完成的应用状态报告
   */
  ApplicationReport createAndGetApplicationReport(String clientUserName,
      boolean allowAccess);
  
  /**
   * 拉取应用需要处理的节点更新增量，拉取后会清空内部缓存。
   * 节点更新包括节点失联、节点恢复健康等变化。
   * @param updatedNodes 用于存放增量更新的Map，key为更新节点，value为更新类型
   * @return 本次拉取到的更新节点数量
   */
  int pullRMNodeUpdates(Map<RMNode, NodeUpdateType> updatedNodes);

  /**
   * 获取应用程序的结束时间戳。
   * @return 应用程序结束时间
   */
  long getFinishTime();

  /**
   * 获取应用程序的启动时间戳。
   * @return 应用程序启动时间
   */
  long getStartTime();

  /**
   * 获取应用程序的提交时间戳。
   * @return 应用程序提交时间
   */
  long getSubmitTime();

  /**
   * 获取应用程序的真正启动时间戳。
   * 由于原getStartTime()实际返回的是提交时间，新增该字段保证向后兼容。
   * @return 应用程序启动时间
   */
  long getLaunchTime();

  /**
   * 获取ApplicationMaster的追踪页面URL。
   * @return ApplicationMaster追踪URL
   */
  String getTrackingUrl();

  /**
   * 获取应用的时间线收集器完整数据，仅在启用时间线服务v2时有效。
   *
   * @return 应用收集器数据，包含地址、RM ID、版本和令牌；未启用v2时返回null
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  AppCollectorData getCollectorData();

  /**
   * 获取要发送给AM的时间线收集器信息，仅在启用时间线服务v2时有效。
   *
   * @return 收集器信息，包含地址和令牌；未启用v2时返回null
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  CollectorInfo getCollectorInfo();

  /**
   * 获取原始的ApplicationMaster追踪页面URL。
   * @return 原始追踪URL
   */
  String getOriginalTrackingUrl();

  /**
   * 获取应用程序的诊断信息字符串构建器。
   * @return 保存诊断信息的StringBuilder
   */
  StringBuilder getDiagnostics();

  /**
   * 获取应用程序的最终状态，由AM取消注册时设置。
   * @return AM设置的最终应用状态
   */
  FinalApplicationStatus getFinalApplicationStatus();

  /**
   * 获取应用程序允许的最大尝试次数。
   * @return 最大尝试次数
   */
  int getMaxAppAttempts();

  /**
   * 获取应用程序类型。
   * @return 应用程序类型字符串
   */
  String getApplicationType();

  /**
   * 获取应用程序标签集合。
   * @return 应用程序对应的标签集合
   */
  Set<String> getApplicationTags();

  /**
   * 检查应用最终状态是否已经保存到状态存储。
   * @return true表示状态已保存，false表示未保存
   */
  boolean isAppFinalStateStored();
  
  
  /**
   * 获取该应用曾经运行过容器的所有节点集合。
   * @return 运行过该应用容器的节点ID集合
   */
  Set<NodeId> getRanNodes();

  /**
   * 根据应用内部状态转换为对外暴露的YarnApplicationState。
   * @return 对外可见的应用状态
   */
  YarnApplicationState createApplicationState();
  
  /**
   * 获取应用程序的指标统计对象。
   * 
   * @return 应用指标对象
   */
  RMAppMetrics getRMAppMetrics();

  /**
   * 获取应用关联的预订ID。
   * @return 预订ID
   */
  ReservationId getReservationId();
  
  /**
   * 获取ApplicationMaster的资源请求列表。
   * @return AM资源请求列表
   */
  List<ResourceRequest> getAMResourceRequests();

  /**
   * 获取应用各节点的日志聚合报告。
   * @return 按节点索引的日志聚合报告映射
   */
  Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp();

  /**
   * 获取用于应用报告的日志聚合整体状态。
   * @return 日志聚合状态
   */
  LogAggregationStatus getLogAggregationStatusForAppReport();

  /**
   * 获取AM容器的节点标签表达式。
   * @return AM容器节点标签表达式
   */
  String getAmNodeLabelExpression();

  /**
   * 获取应用容器的节点标签表达式。
   * @return 应用节点标签表达式
   */
  String getAppNodeLabelExpression();

  /**
   * 获取调用者上下文信息。
   * @return 调用者上下文
   */
  CallerContext getCallerContext();

  /**
   * 获取应用各类超时时间的映射。
   * @return 超时类型到超时时间戳的映射
   */
  Map<ApplicationTimeoutType, Long> getApplicationTimeouts();

  /**
   * 获取应用程序的调度优先级。
   * @return 应用优先级
   */
  Priority getApplicationPriority();

  /**
   * 检查应用是否已经处于完成相关状态（completing/completed）。
   *
   * @return true表示应用已进入最终状态，false表示未完成
   */
  boolean isAppInCompletedStates();

  /**
   * 获取应用到队列的放置上下文信息。
   * @return 应用放置上下文
   */
  ApplicationPlacementContext getApplicationPlacementContext();

  /**
   * 获取应用调度相关的环境变量配置。
   * @return 应用调度偏好环境变量映射
   */
  Map<String, String> getApplicationSchedulingEnvs();

  /**
   * 获取应用运行的真实用户名。
   * @return 真实用户名
   */
  String getRealUser();
}