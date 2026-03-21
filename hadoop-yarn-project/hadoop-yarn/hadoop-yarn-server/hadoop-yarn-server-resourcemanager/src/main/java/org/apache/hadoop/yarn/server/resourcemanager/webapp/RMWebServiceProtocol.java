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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import java.io.IOException;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppPriority;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppQueue;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppTimeoutsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationStatisticsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ApplicationSubmissionContextInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterUserInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.DelegationToken;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.RMQueueAclInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationDeleteRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationSubmissionRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ReservationUpdateRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceOptionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.BulkActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * <p>
 * ResourceManager REST Web服务接口协议，定义了客户端通过REST调用与RM交互的所有API端点，
 * 包括应用提交管理、集群信息查询、节点标签管理、队列管理、资源预留等功能。
 * </p>
 *
 * 该Web服务可通过{@link RMWSConsts#RM_WEB_SERVICE_PATH}访问
 */
@Private
@Evolving
public interface RMWebServiceProtocol {

  /**
   * 获取集群基本信息，可通过{@link RMWSConsts#INFO}访问
   *
   * @return 集群基本信息对象
   */
  ClusterInfo get();

  /**
   * 获取集群基本信息，可通过{@link RMWSConsts#INFO}访问
   *
   * @return 集群基本信息对象
   */
  ClusterInfo getClusterInfo();


  /**
   * 获取当前请求用户的集群用户信息，可通过{@link RMWSConsts#CLUSTER_USER_INFO}访问
   *
   * @param hsr HTTP Servlet请求对象
   * @return 当前用户的集群信息对象
   */
  ClusterUserInfo getClusterUserInfo(HttpServletRequest hsr);

  /**
   * 获取集群 metrics 指标信息，可通过{@link RMWSConsts#METRICS}访问
   *
   * @see ApplicationClientProtocol#getClusterMetrics
   * @return 集群 metrics 指标对象
   */
  ClusterMetricsInfo getClusterMetricsInfo();

  /**
   * 获取当前调度器类型信息，可通过{@link RMWSConsts#SCHEDULER}访问
   *
   * @return 当前调度器类型信息对象
   */
  SchedulerTypeInfo getSchedulerInfo();

  /**
   * 导出指定时间段内的调度器日志，可通过{@link RMWSConsts#SCHEDULER_LOGS}访问
   *
   * @param time 导出日志的时间范围（FormParam参数）
   * @param hsr HTTP Servlet请求对象
   * @return 操作结果字符串
   * @throws IOException 无法创建日志转储文件时抛出
   */
  String dumpSchedulerLogs(String time, HttpServletRequest hsr)
      throws IOException;

  /**
   * 获取集群所有节点信息，可按节点状态过滤，可通过{@link RMWSConsts#NODES}访问
   *
   * @see ApplicationClientProtocol#getClusterNodes
   * @param states 过滤节点状态，逗号分隔的状态列表（QueryParam参数）
   * @return 节点信息集合，若传入states参数则只返回对应状态的节点
   */
  NodesInfo getNodes(String states);

  /**
   * 获取指定节点的详细信息，可通过{@link RMWSConsts#NODES_NODEID}访问
   *
   * @param nodeId 目标节点ID（PathParam参数）
   * @return 指定节点的详细信息对象
   */
  NodeInfo getNode(String nodeId);

  /**
   * 更新指定节点可分配资源量，可通过{@link RMWSConsts#NODE_RESOURCE}访问
   *
   * @param hsr HTTP Servlet请求对象
   * @param nodeId 目标节点ID（PathParam参数）
   * @param resourceOption 资源变更信息
   * @return 更新后的节点资源信息
   * @throws AuthorizationException 用户未授权时抛出
   */
  ResourceInfo updateNodeResource(HttpServletRequest hsr, String nodeId,
      ResourceOptionInfo resourceOption) throws AuthorizationException;

  /**
   * 获取集群中符合条件的应用列表，支持多维度过滤，可通过{@link RMWSConsts#APPS}访问
   *
   * @see ApplicationClientProtocol#getApplications
   * @param hsr HTTP Servlet请求对象
   * @param stateQuery 已废弃的状态过滤参数（QueryParam参数）
   * @param statesQuery 按应用状态过滤（QueryParam参数）
   * @param finalStatusQuery 按应用最终状态过滤（QueryParam参数）
   * @param userQuery 按提交用户过滤（QueryParam参数）
   * @param queueQuery 按队列过滤（QueryParam参数）
   * @param count 限制返回结果最大数量（QueryParam参数）
   * @param startedBegin 按开始时间过滤，起始时间（QueryParam参数）
   * @param startedEnd 按开始时间过滤，结束时间（QueryParam参数）
   * @param finishBegin 按结束时间过滤，起始时间（QueryParam参数）
   * @param finishEnd 按结束时间过滤，结束时间（QueryParam参数）
   * @param applicationTypes 按应用类型过滤（QueryParam参数）
   * @param applicationTags 按应用标签过滤（QueryParam参数）
   * @param name 按应用名称过滤（QueryParam参数）
   * @param unselectedFields 需要排除返回结果的字段（QueryParam参数）
   * @return 符合过滤条件的应用列表
   */
  @SuppressWarnings("checkstyle:parameternumber")
  AppsInfo getApps(HttpServletRequest hsr, String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery,
      String queueQuery, String count, String startedBegin, String startedEnd,
      String finishBegin, String finishEnd, Set<String> applicationTypes,
      Set<String> applicationTags, String name, Set<String> unselectedFields);

  /**
   * 获取指定节点上的调度活动列表，可通过{@link RMWSConsts#SCHEDULER_ACTIVITIES}访问
   *
   * @param hsr HTTP Servlet请求对象
   * @param nodeId 目标节点ID（QueryParam参数）
   * @param groupBy 活动聚合分组方式（QueryParam参数）
   * @return 指定节点的调度活动信息
   */
  ActivitiesInfo getActivities(HttpServletRequest hsr, String nodeId,
      String groupBy);

  /**
   * 获取最近N条调度器全局活动，可通过{@link RMWSConsts#SCHEDULER_BULK_ACTIVITIES}访问
   *
   * @param hsr HTTP Servlet请求对象
   * @param groupBy 活动聚合分组方式（QueryParam参数）
   * @param activitiesCount 需要返回的活动数量
   * @return 最近N条调度活动信息
   * @throws InterruptedException 线程被中断时抛出
   */
  BulkActivitiesInfo getBulkActivities(HttpServletRequest hsr,
      String groupBy, int activitiesCount) throws InterruptedException;

  /**
   * 获取指定应用在指定时间段内的所有调度活动，可通过{@link RMWSConsts#SCHEDULER_APP_ACTIVITIES}访问
   *
   * @param hsr HTTP Servlet请求对象
   * @param appId 目标应用ID（QueryParam参数）
   * @param time 查询的时间范围（QueryParam参数）
   * @param requestPriorities 按请求优先级过滤（QueryParam参数）
   * @param allocationRequestIds 按分配请求ID过滤（QueryParam参数）
   * @param groupBy 活动聚合分组方式（QueryParam参数）
   * @param limit 限制返回结果数量（QueryParam参数）
   * @param actions 按活动动作过滤（QueryParam参数）
   * @param summarize 是否聚合多个调度周期的活动（QueryParam参数）
   * @return 指定应用指定时间范围的调度活动信息
   */
  AppActivitiesInfo getAppActivities(HttpServletRequest hsr, String appId,
      String time, Set<String> requestPriorities,
      Set<String> allocationRequestIds, String groupBy, String limit,
      Set<String> actions, boolean summarize);

  /**
   * 获取应用统计信息，可按状态和类型过滤，可通过{@link RMWSConsts#APP_STATISTICS}访问
   *
   * @param hsr HTTP Servlet请求对象
   * @param stateQueries 按应用状态过滤（QueryParam参数）
   * @param typeQueries 按应用类型过滤（QueryParam参数）
   * @return 指定条件的应用统计信息
   */
  ApplicationStatisticsInfo getAppStatistics(HttpServletRequest hsr,
      Set<String> stateQueries, Set<String> typeQueries);

  /**
   * 获取指定应用的详细报告信息，可通过{@link RMWSConsts#APPS_APPID}访问
   *
   * @see ApplicationClientProtocol#getApplicationReport
   * @param hsr HTTP Servlet请求对象
   * @param appId 目标应用ID（PathParam参数）
   * @param unselectedFields 需要排除返回结果的字段（QueryParam参数）
   * @return 指定应用详细报告信息
   */
  AppInfo getApp(HttpServletRequest hsr, String appId,
      Set<String> unselectedFields);

  /**
   * 获取指定应用当前状态，可通过{@link RMWSConsts#APPS_APPID_STATE}访问
   *
   * @param hsr HTTP Servlet请求对象
   * @param appId 目标应用ID（PathParam参数）
   * @return 指定应用当前状态
   * @throws AuthorizationException 用户未授权时抛出
   */
  AppState getAppState(HttpServletRequest hsr, String appId)
      throws AuthorizationException;

  /**
   * 更新指定应用状态（如终止运行的应用），可通过{@link RMWSConsts#APPS_APPID_STATE}访问
   *
   * @param targetState 目标状态（请求体参数）
   * @param hsr HTTP Servlet请求对象
   * @param appId 目标应用ID（PathParam参数）
   * @return HTTP响应对象，包含状态码
   * @throws AuthorizationException 用户未授权调用时抛出
   * @throws YarnException 应用不存在时抛出
   * @throws InterruptedException 线程被中断时抛出
   * @throws IOException doAs操作抛出IO异常时抛出
   */
  Response updateAppState(AppState targetState, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException;

  /**
   * 获取集群节点标签与节点映射关系，可通过{@link RMWSConsts#GET_NODE_TO_LABELS}访问
   *
   * @see ApplicationClientProtocol#getNodeToLabels
   * @param hsr HTTP Servlet请求对象
   * @return 节点到标签的映射信息
   * @throws IOException IO异常时抛出
   */
  NodeToLabelsInfo getNodeToLabels(HttpServletRequest hsr) throws IOException;

  /**
   * 获取RM注册的所有节点标签信息
   *
   * @param hsr HTTP Servlet请求对象
   * @return 节点标签信息列表
   * @throws IOException IO异常时抛出
   */
  NodeLabelsInfo getRMNodeLabels(HttpServletRequest hsr) throws IOException;

  /**
   * 获取标签到节点的映射关系，可按标签过滤，可通过{@link RMWSConsts#LABEL_MAPPINGS}访问
   *
   * @see ApplicationClientProtocol#getLabelsToNodes
   * @param labels 按标签过滤（QueryParam参数）
   * @return 标签到节点的映射信息
   * @throws IOException IO异常时抛出
   */
  LabelsToNodesInfo getLabelsToNodes(Set<String> labels) throws IOException;

  /**
   * 批量替换多个节点的标签，可通过{@link RMWSConsts#REPLACE_NODE_TO_LABELS}访问
   *
   * @see ResourceManagerAdministrationProtocol#replaceLabelsOnNode
   * @param newNodeToLabels 新节点标签映射列表（请求体参数）
   * @param hsr HTTP Servlet请求对象
   * @return HTTP响应对象，包含状态码
   * @throws Exception 处理过程发生异常时抛出
   */
  Response replaceLabelsOnNodes(NodeToLabelsEntryList newNodeToLabels,
      HttpServletRequest hsr) throws Exception;

  /**
   * 替换指定节点的标签，可通过{@link RMWSConsts#NODES_NODEID_REPLACE_LABELS}访问
   *
   * @see ResourceManagerAdministrationProtocol#replaceLabelsOnNode
   * @param newNodeLabelsName 新标签列表（QueryParam参数）
   * @param hsr HTTP Servlet请求对象
   * @param nodeId 目标节点ID（PathParam参数）
   * @return HTTP响应对象，包含状态码
   * @throws Exception 处理过程发生异常时抛出
   */
  Response replaceLabelsOnNode(Set<String> newNodeLabelsName,
      HttpServletRequest hsr, String nodeId) throws Exception;

  /**
   * 获取集群中所有节点标签，可通过{@link RMWSConsts#GET_NODE_LABELS}访问
   *
   * @see ApplicationClientProtocol#getClusterNodeLabels
   * @param hsr HTTP Servlet请求对象
   * @return 集群所有节点标签信息
   * @throws IOException IO异常时抛出
   */
  NodeLabelsInfo getClusterNodeLabels(HttpServletRequest hsr)
      throws IOException;

  /**
   * 向集群添加新节点标签，可通过{@link RMWSConsts#ADD_NODE_LABELS}访问
   *
   * @see ResourceManagerAdministrationProtocol#addToClusterNodeLabels
   * @param newNodeLabels 要添加的节点标签（请求体参数）
   * @param hsr HTTP Servlet请求对象
   * @return HTTP响应对象，包含状态码
   * @throws Exception 请求非法时抛出
   */
  Response addToClusterNodeLabels(NodeLabelsInfo newNodeLabels,
      HttpServletRequest hsr) throws Exception;

  /**
   * 从集群删除节点标签，可通过{@link RMWSConsts#REMOVE_NODE_LABELS}访问
   *
   * @see ResourceManagerAdministrationProtocol#removeFromClusterNodeLabels
   * @param oldNodeLabels 要删除的标签列表（QueryParam参数）
   * @param hsr HTTP Servlet请求对象
   * @return HTTP响应对象，包含状态码
   * @throws Exception 请求非法时抛出
   */
  Response removeFromClusterNodeLabels(Set<String> oldNodeLabels,
      HttpServletRequest hsr) throws Exception;

  /**
   * 获取指定节点上的所有标签，可通过{@link RMWSConsts#