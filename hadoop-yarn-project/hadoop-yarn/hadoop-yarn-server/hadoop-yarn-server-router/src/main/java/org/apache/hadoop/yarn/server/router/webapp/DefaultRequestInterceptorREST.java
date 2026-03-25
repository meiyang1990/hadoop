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

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
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
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * YARN Router联邦场景下的默认REST请求拦截器，直接将客户端REST请求转发到目标ResourceManager处理。
 * 继承AbstractRESTRequestInterceptor，实现了所有REST接口的转发逻辑，是拦截器链的末端节点。
 */
public class DefaultRequestInterceptorREST
    extends AbstractRESTRequestInterceptor {

  private String webAppAddress;
  private SubClusterId subClusterId = null;

  // 创建Jersey客户端开销很大，每个请求会生成一个新线程，此处复用单例客户端
  private Client client = null;

  /**
   * 设置目标ResourceManager的Web服务地址。
   * @param webAppAddress Web服务地址
   */
  public void setWebAppAddress(String webAppAddress) {
    this.webAppAddress = webAppAddress;
  }

  protected String getWebAppAddress() {
    return this.webAppAddress;
  }

  protected void setSubClusterId(SubClusterId scId) {
    this.subClusterId = scId;
  }

  protected SubClusterId getSubClusterId() {
    return this.subClusterId;
  }

  @Override
  public void init(String user) {
    super.init(user);
    // 从配置获取ResourceManager Web服务地址
    webAppAddress = WebAppUtils.getRMWebAppURLWithScheme(getConf());
    // 创建Jersey客户端实例
    client = RouterWebServiceUtil.createJerseyClient(getConf());
  }

  @Override
  public ClusterInfo get() {
    return getClusterInfo();
  }

  @Override
  public ClusterInfo getClusterInfo() {
    // 转发获取集群信息请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        ClusterInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.INFO, null, null,
        getConf(), client);
  }

  @Override
  public ClusterUserInfo getClusterUserInfo(HttpServletRequest hsr) {
    // 转发获取当前用户集群信息请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        ClusterUserInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.CLUSTER_USER_INFO, null,
        null, getConf(), client);
  }

  @Override
  public ClusterMetricsInfo getClusterMetricsInfo() {
    // 转发获取集群指标请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        ClusterMetricsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
        getConf(), client);
  }

  @Override
  public SchedulerTypeInfo getSchedulerInfo() {
    // 转发获取调度器类型请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        SchedulerTypeInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER, null, null,
        getConf(), client);
  }

  @Override
  public String dumpSchedulerLogs(String time, HttpServletRequest hsr)
      throws IOException {
    // 转发导出调度器日志请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        String.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_LOGS, null, null,
        getConf(), client);
  }

  @Override
  public NodesInfo getNodes(String states) {
    // 状态过滤参数添加到请求参数
    Map<String, String[]> additionalParam = new HashMap<String, String[]>();
    if (states != null && !states.isEmpty()) {
      additionalParam.put(RMWSConsts.STATES, new String[] {states});
    }
    // 转发获取节点列表请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        NodesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES, null,
        additionalParam, getConf(), client);
  }

  @Override
  public NodeInfo getNode(String nodeId) {
    // 转发获取单个节点信息请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        NodeInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES + "/" + nodeId, null,
        null, getConf(), client);
  }

  @Override
  public ResourceInfo updateNodeResource(HttpServletRequest hsr,
      String nodeId, ResourceOptionInfo resourceOption) {
    final String nodePath =
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES + "/" + nodeId;
    // 转发更新节点资源请求到目标RM
    return RouterWebServiceUtil
        .genericForward(webAppAddress, hsr, ResourceInfo.class,
            HTTPMethods.POST, nodePath + "/resource", resourceOption, null,
            getConf(), client);
  }

  @Override
  public AppsInfo getApps(HttpServletRequest hsr, String stateQuery,
      Set<String> statesQuery, String finalStatusQuery, String userQuery,
      String queueQuery, String count, String startedBegin, String startedEnd,
      String finishBegin, String finishEnd, Set<String> applicationTypes,
      Set<String> applicationTags, String name, Set<String> unselectedFields) {
    // 转发获取应用列表请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS, null, null,
        getConf(), client);
  }

  @Override
  public ActivitiesInfo getActivities(HttpServletRequest hsr, String nodeId,
      String groupBy) {
    // 转发获取调度活动列表请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        ActivitiesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_ACTIVITIES, null,
        null, getConf(), client);
  }

  @Override
  public BulkActivitiesInfo getBulkActivities(HttpServletRequest hsr,
      String groupBy, int activitiesCount) {
    // 转发获取批量调度活动请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        BulkActivitiesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_BULK_ACTIVITIES,
        null, null, getConf(), client);
  }

  @Override
  public AppActivitiesInfo getAppActivities(HttpServletRequest hsr,
      String appId, String time, Set<String> requestPriorities,
      Set<String> allocationRequestIds, String groupBy, String limit,
      Set<String> actions, boolean summarize) {
    // 转发获取应用调度活动请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppActivitiesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_APP_ACTIVITIES,
        null, null, getConf(), client);
  }

  @Override
  public ApplicationStatisticsInfo getAppStatistics(HttpServletRequest hsr,
      Set<String> stateQueries, Set<String> typeQueries) {
    // 转发获取应用统计信息请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        ApplicationStatisticsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APP_STATISTICS, null, null,
        getConf(), client);
  }

  @Override
  public AppInfo getApp(HttpServletRequest hsr, String appId,
      Set<String> unselectedFields) {
    // 转发获取单个应用信息请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.APPS + "/" + appId, null,
        null, getConf(), client);
  }

  @Override
  public AppState getAppState(HttpServletRequest hsr, String appId)
      throws AuthorizationException {
    // 转发获取应用状态请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        AppState.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.STATE,
        null, null, getConf(), client);
  }

  @Override
  public Response updateAppState(AppState targetState, HttpServletRequest hsr,
      String appId) throws AuthorizationException, YarnException,
      InterruptedException, IOException {
    // 转发更新应用状态请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.PUT, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.APPS + "/" + appId + "/" + RMWSConsts.STATE,
        targetState, null, getConf(), client);
  }

  @Override
  public NodeToLabelsInfo getNodeToLabels(HttpServletRequest hsr)
      throws IOException {
    // 转发获取节点标签映射请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeToLabelsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_NODE_TO_LABELS, null,
        null, getConf(), client);
  }

  @Override
  public LabelsToNodesInfo getLabelsToNodes(Set<String> labels)
      throws IOException {
    // 标签参数添加到请求参数
    Map<String, String[]> additionalParam = new HashMap<>();
    if (labels != null && !labels.isEmpty()) {
      additionalParam.put(RMWSConsts.LABELS,
          labels.toArray(new String[labels.size()]));
    }
    // 转发获取标签到节点映射请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, null,
        LabelsToNodesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.LABEL_MAPPINGS, null,
        additionalParam, getConf(), client);
  }

  @Override
  public Response replaceLabelsOnNodes(NodeToLabelsEntryList newNodeToLabels,
      HttpServletRequest hsr) throws IOException {
    // 转发替换节点标签映射请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REPLACE_NODE_TO_LABELS,
        newNodeToLabels, null, getConf(), client);
  }

  @Override
  public Response replaceLabelsOnNode(Set<String> newNodeLabelsName,
      HttpServletRequest hsr, String nodeId) throws Exception {
    // 转发替换单个节点标签请求到目标RM
    return RouterWebServiceUtil
        .genericForward(webAppAddress, hsr,
            Response.class, HTTPMethods.POST, RMWSConsts.RM_WEB_SERVICE_PATH
                + RMWSConsts.NODES + "/" + nodeId + "/replace-labels",
            null, null, getConf(), client);
  }

  @Override
  public NodeLabelsInfo getClusterNodeLabels(HttpServletRequest hsr)
      throws IOException {
    // 转发获取集群节点标签请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeLabelsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_NODE_LABELS, null,
        null, getConf(), client);
  }

  @Override
  public Response addToClusterNodeLabels(NodeLabelsInfo newNodeLabels,
      HttpServletRequest hsr) throws Exception {
    // 转发添加集群节点标签请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.ADD_NODE_LABELS,
        newNodeLabels, null, getConf(), client);
  }

  @Override
  public Response removeFromClusterNodeLabels(Set<String> oldNodeLabels,
      HttpServletRequest hsr) throws Exception {
    // 转发删除集群节点标签请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        Response.class, HTTPMethods.POST,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.REMOVE_NODE_LABELS, null,
        null, getConf(), client);
  }

  @Override
  public NodeLabelsInfo getLabelsOnNode(HttpServletRequest hsr, String nodeId)
      throws IOException {
    // 转发获取节点标签请求到目标RM
    return RouterWebServiceUtil.genericForward(webAppAddress, hsr,
        NodeLabelsInfo.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH
            + RMWSConsts.NODES + "/" + nodeId + "/get-labels",
        null, null, getConf(), client);