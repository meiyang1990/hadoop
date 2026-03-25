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
package org.apache.hadoop.yarn.server.router.clientrm;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN Router联邦场景下Yarn客户端API调用结果合并工具类。
 * 提供将多个子集群返回的API结果合并为统一全局结果的各类方法。
 */
public final class RouterYarnClientUtils {

  private final static String PARTIAL_REPORT = "Partial Report ";

  private RouterYarnClientUtils() {

  }

  /**
   * 合并多个子集群返回的集群指标响应。
   * @param responses 多个子集群的集群指标响应集合
   * @return 合并后的全局集群指标响应
   */
  public static GetClusterMetricsResponse merge(
      Collection<GetClusterMetricsResponse> responses) {
    // 初始化临时指标容器，从0开始累加
    YarnClusterMetrics tmp = YarnClusterMetrics.newInstance(0);
    // 遍历所有子集群响应，累加各状态节点数量
    for (GetClusterMetricsResponse response : responses) {
      YarnClusterMetrics metrics = response.getClusterMetrics();
      tmp.setNumNodeManagers(
          tmp.getNumNodeManagers() + metrics.getNumNodeManagers());
      tmp.setNumActiveNodeManagers(
          tmp.getNumActiveNodeManagers() + metrics.getNumActiveNodeManagers());
      tmp.setNumDecommissioningNodeManagers(
          tmp.getNumDecommissioningNodeManagers() + metrics
              .getNumDecommissioningNodeManagers());
      tmp.setNumDecommissionedNodeManagers(
          tmp.getNumDecommissionedNodeManagers() + metrics
              .getNumDecommissionedNodeManagers());
      tmp.setNumLostNodeManagers(
          tmp.getNumLostNodeManagers() + metrics.getNumLostNodeManagers());
      tmp.setNumRebootedNodeManagers(tmp.getNumRebootedNodeManagers() + metrics
          .getNumRebootedNodeManagers());
      tmp.setNumUnhealthyNodeManagers(
          tmp.getNumUnhealthyNodeManagers() + metrics
              .getNumUnhealthyNodeManagers());
      tmp.setNumShutdownNodeManagers(
          tmp.getNumShutdownNodeManagers() + metrics
              .getNumShutdownNodeManagers());
    }
    return GetClusterMetricsResponse.newInstance(tmp);
  }

  /**
   * 按ApplicationId分组合并多个子集群返回的应用列表，合并托管AM和非托管UAM的资源使用信息。
   * 策略：仅合并可访问子集群的应用报告；支持返回不完整的结果。
   * @param responses 多个子集群的应用列表响应集合
   * @param returnPartialResult 是否允许返回部分结果（仅收集到部分UAM时）
   * @return 合并后的全局应用列表响应
   */
  public static GetApplicationsResponse mergeApplications(
      Collection<GetApplicationsResponse> responses,
      boolean returnPartialResult){
    // 存储托管AM应用报告
    Map<ApplicationId, ApplicationReport> federationAM = new HashMap<>();
    // 存储未找到对应AM的非托管UAM聚合报告
    Map<ApplicationId, ApplicationReport> federationUAMSum = new HashMap<>();

    // 遍历所有子集群的应用响应
    for (GetApplicationsResponse appResponse : responses){
      for (ApplicationReport appReport : appResponse.getApplicationList()){
        ApplicationId appId = appReport.getApplicationId();
        // 当前报告是托管AM
        if (!appReport.isUnmanagedApp()) {
          // 保存托管AM报告
          federationAM.put(appId, appReport);
          // 如果之前已收集该应用的UAM，合并资源信息
          if (federationUAMSum.containsKey(appId)) {
            mergeAMWithUAM(appReport, federationUAMSum.get(appId));
            federationUAMSum.remove(appId);
          }
        // 当前报告是非托管UAM
        } else if (federationAM.containsKey(appId)) {
          // 已存在对应AM，直接合并资源信息到AM
          mergeAMWithUAM(federationAM.get(appId), appReport);
        } else if (federationUAMSum.containsKey(appId)) {
          // 已有该应用的UAM聚合结果，合并当前UAM到聚合结果
          ApplicationReport mergedUAMReport =
              mergeUAMWithUAM(federationUAMSum.get(appId), appReport);
          federationUAMSum.put(appId, mergedUAMReport);
        } else {
          // 首次遇到该UAM且未找到对应AM，存入待聚合列表
          federationUAMSum.put(appId, appReport);
        }
      }
    }
    // 处理剩余未匹配AM的UAM聚合结果，根据配置决定是否加入最终结果
    for (ApplicationReport appReport : federationUAMSum.values()) {
      if (mergeUamToReport(appReport.getName(), returnPartialResult)) {
        federationAM.put(appReport.getApplicationId(), appReport);
      }
    }

    return GetApplicationsResponse.newInstance(federationAM.values());
  }

  /**
   * 合并两个非托管UAM应用报告的资源使用信息，标记为部分结果。
   * @param uam1 已聚合的UAM报告
   * @param uam2 当前待合并UAM报告
   * @return 合并后的UAM聚合报告
   */
  private static ApplicationReport mergeUAMWithUAM(ApplicationReport uam1,
      ApplicationReport uam2){
    uam1.setName(PARTIAL_REPORT + uam1.getApplicationId());
    mergeAMWithUAM(uam1, uam2);
    return uam1;
  }

  /**
   * 将非托管UAM的资源使用信息合并到托管AM的资源报告中。
   * 累加容器数、资源量、各种指标百分比和资源秒数统计。
   * @param am 托管AM应用报告
   * @param uam 非托管UAM应用报告
   */
  private static void mergeAMWithUAM(ApplicationReport am,
      ApplicationReport uam){
    // 获取AM和UAM各自的资源使用报告
    ApplicationResourceUsageReport amResourceReport =
        am.getApplicationResourceUsageReport();

    ApplicationResourceUsageReport uamResourceReport =
        uam.getApplicationResourceUsageReport();

    // AM没有资源报告直接使用UAM的
    if (amResourceReport == null) {
      am.setApplicationResourceUsageReport(uamResourceReport);
    } else if (uamResourceReport != null) {
      // 累加已使用容器数
      amResourceReport.setNumUsedContainers(
          amResourceReport.getNumUsedContainers() +
              uamResourceReport.getNumUsedContainers());

      // 累加预留容器数
      amResourceReport.setNumReservedContainers(
          amResourceReport.getNumReservedContainers() +
              uamResourceReport.getNumReservedContainers());

      // 累加已使用资源量
      amResourceReport.setUsedResources(Resources.add(
          amResourceReport.getUsedResources(),
          uamResourceReport.getUsedResources()));

      // 累加预留资源量
      amResourceReport.setReservedResources(Resources.add(
          amResourceReport.getReservedResources(),
          uamResourceReport.getReservedResources()));

      // 累加所需资源量
      amResourceReport.setNeededResources(Resources.add(
          amResourceReport.getNeededResources(),
          uamResourceReport.getNeededResources()));

      // 累加内存秒数
      amResourceReport.setMemorySeconds(
          amResourceReport.getMemorySeconds() +
              uamResourceReport.getMemorySeconds());

      // 累加vcore秒数
      amResourceReport.setVcoreSeconds(
          amResourceReport.getVcoreSeconds() +
              uamResourceReport.getVcoreSeconds());

      // 累加队列使用率百分比
      amResourceReport.setQueueUsagePercentage(
          amResourceReport.getQueueUsagePercentage() +
              uamResourceReport.getQueueUsagePercentage());

      // 累加集群使用率百分比
      amResourceReport.setClusterUsagePercentage(
          amResourceReport.getClusterUsagePercentage() +
              uamResourceReport.getClusterUsagePercentage());

      // 更新合并后的资源报告到AM
      am.setApplicationResourceUsageReport(amResourceReport);
    }
  }

  /**
   * 判断是否应将未匹配到AM的UAM加入最终结果。
   * @param appName 应用名称
   * @param returnPartialResult 是否允许返回部分结果
   * @return true表示加入结果，false表示不加入
   */
  private static boolean mergeUamToReport(String appName,
      boolean returnPartialResult){
    // 允许返回部分结果直接返回true
    if (returnPartialResult) {
      return true;
    }
    // 应用名称为空不加入
    if (appName == null) {
      return false;
    }
    // 不允许返回部分结果时，仅返回非UAM且非部分报告的应用
    return !(appName.startsWith(UnmanagedApplicationManager.APP_NAME) ||
        appName.startsWith(PARTIAL_REPORT));
  }

  /**
   * 合并多个子集群返回的集群节点列表响应。
   * @param responses 多个子集群的节点列表响应集合
   * @return 合并后的全局节点列表响应
   */
  public static GetClusterNodesResponse mergeClusterNodesResponse(
      Collection<GetClusterNodesResponse> responses) {
    GetClusterNodesResponse clusterNodesResponse = Records.newRecord(GetClusterNodesResponse.class);
    List<NodeReport> nodeReports = new ArrayList<>();
    for (GetClusterNodesResponse response : responses) {
      if (response != null && response.getNodeReports() != null) {
        nodeReports.addAll(response.getNodeReports());
      }
    }
    clusterNodesResponse.setNodeReports(nodeReports);
    return clusterNodesResponse;
  }

  /**
   * 合并多个子集群返回的节点到标签映射响应。
   * @param responses 多个子集群的节点标签映射响应集合
   * @return 合并后的全局节点到标签映射响应
   */
  public static GetNodesToLabelsResponse mergeNodesToLabelsResponse(
      Collection<GetNodesToLabelsResponse> responses) {
    GetNodesToLabelsResponse nodesToLabelsResponse = Records.newRecord(
         GetNodesToLabelsResponse.class);
    Map<NodeId, Set<String>> nodesToLabelMap = new HashMap<>();
    for (GetNodesToLabelsResponse response : responses) {
      if (response != null && response.getNodeToLabels() != null) {
        nodesToLabelMap.putAll(response.getNodeToLabels());
      }
    }
    nodesToLabelsResponse.setNodeToLabels(nodesToLabelMap);
    return nodesToLabelsResponse;
  }

  /**
   * 合并多个子集群返回的标签到节点映射响应。
   * @param responses 多个子集群的标签节点映射响应集合
   * @return 合并后的全局标签到节点映射响应
   */
  public static GetLabelsToNodesResponse mergeLabelsToNodes(
      Collection<GetLabelsToNodesResponse> responses){
    GetLabelsToNodesResponse labelsToNodesResponse = Records.newRecord(
        GetLabelsToNodesResponse.class);
    Map<String, Set<NodeId>> labelsToNodesMap = new HashMap<>();
    // 遍历所有子集群响应，按标签合并节点集合
    for (GetLabelsToNodesResponse response : responses) {
      if (response != null && response.getLabelsToNodes() != null) {
        Map<String, Set<NodeId>> clusterLabelsToNodesMap = response.getLabelsToNodes();
        for (Map.Entry<String, Set<NodeId>> entry : clusterLabelsToNodesMap.entrySet()) {
          String label = entry.getKey();
          Set<NodeId> clusterNodes = entry.getValue();
          if (labelsToNodesMap.containsKey(label)) {
            // 已有该标签，追加节点集合
            Set<NodeId> allNodes = labelsToNodesMap.get(label);
            allNodes.addAll(clusterNodes);
          } else {
            // 新标签，直接存入
            labelsToNodesMap.put(label, clusterNodes);
          }
        }
      }
    }
    labelsToNodesResponse.setLabelsToNodes(labelsToNodesMap);
    return labelsToNodesResponse;
  }

  /**
   * 合并多个子集群返回的集群节点标签列表响应。
   * @param responses 多个子集群的节点标签列表响应集合
   * @return 合并后的全局节点标签列表响应
   */
  public static GetClusterNodeLabelsResponse mergeClusterNodeLabelsResponse(
      Collection<GetClusterNodeLabelsResponse> responses) {
    GetClusterNodeLabelsResponse nodeLabelsResponse = Records.newRecord(
        GetClusterNodeLabelsResponse.class);
    Set<NodeLabel> nodeLabelsList = new HashSet<>();
    for (GetClusterNodeLabelsResponse response : responses) {
      if (response != null && response.getNodeLabelList() != null) {
        nodeLabelsList.addAll(response.getNodeLabelList());
      }
    }
    nodeLabelsResponse.setNodeLabelList(new ArrayList<>(nodeLabelsList));
    return nodeLabelsResponse;
  }

  /**
   * 合并多个子集群返回的队列用户ACL信息响应。
   * @param responses 多个子集群的队列ACL响应集合
   * @return 合并后的全局队列用户ACL响应
   */
  public static GetQueueUserAclsInfoResponse mergeQueueUserAcls(
      Collection<GetQueueUserAclsInfoResponse> responses) {
    GetQueueUserAclsInfoResponse aclsInfoResponse = Records.newRecord(
        GetQueueUserAclsInfoResponse.class);
    Set<QueueUserACLInfo> queueUserACLInfos = new HashSet<>();
    for (GetQueueUserAclsInfoResponse response : responses) {
      if (response != null && response.getUserAclsInfoList() != null) {
        queueUserACLInfos.addAll(response.getUserAclsInfoList());
      }
    }
    aclsInfoResponse.setUserAclsInfoList(new ArrayList<>(queueUserACLInfos));
    return aclsInfoResponse;
  }

  /**
   * 合并多个子集群返回的预约列表响应。
   * @param responses 多个子集群的预约列表响应集合
   * @return 合并后的全局预约列表响应
   */
  public static ReservationListResponse mergeReservationsList(
      Collection<ReservationListResponse> responses) {
    ReservationListResponse reservationListResponse =
        Records.newRecord(ReservationListResponse.class);
    List<ReservationAllocationState> reservationAllocationStates =
        new ArrayList<>();
    for (ReservationListResponse response : responses) {
      if (response != null && response.getReservationAllocationState() != null) {
        reservationAllocationStates.addAll(
            response.getReservationAllocationState());
      }
    }
    reservationListResponse.setReservationAllocationState(