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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;

/**
 * YARN ResourceManager 节点状态更新事件，封装NodeManager上报的节点最新状态信息
 */
public class RMNodeStatusEvent extends RMNodeEvent {

  // NodeManager上报的完整节点状态对象
  private final NodeStatus nodeStatus;
  // 应用日志聚合状态报告列表
  private List<LogAggregationReport> logAggregationReportsForApps;

  /**
   * 构造节点状态更新事件（不包含日志聚合报告）
   * @param nodeId 目标节点ID
   * @param nodeStatus NodeManager上报的节点状态
   */
  public RMNodeStatusEvent(NodeId nodeId, NodeStatus nodeStatus) {
    this(nodeId, nodeStatus, null);
  }

  /**
   * 构造节点状态更新事件（包含日志聚合报告）
   * @param nodeId 目标节点ID
   * @param nodeStatus NodeManager上报的节点状态
   * @param logAggregationReportsForApps 应用日志聚合状态报告列表
   */
  public RMNodeStatusEvent(NodeId nodeId, NodeStatus nodeStatus,
      List<LogAggregationReport> logAggregationReportsForApps) {
    super(nodeId, RMNodeEventType.STATUS_UPDATE);
    this.nodeStatus = nodeStatus;
    this.logAggregationReportsForApps = logAggregationReportsForApps;
  }

  /**
   * 获取节点健康状态信息
   * @return 节点健康状态
   */
  public NodeHealthStatus getNodeHealthStatus() {
    return this.nodeStatus.getNodeHealthStatus();
  }

  /**
   * 获取节点上所有容器的状态列表
   * @return 容器状态列表
   */
  public List<ContainerStatus> getContainers() {
    return this.nodeStatus.getContainersStatuses();
  }

  * 获取需要保持活跃的应用ID列表
   * @return 保活应用ID列表
   */
  public List<ApplicationId> getKeepAliveAppIds() {
    return this.nodeStatus.getKeepAliveApplications();
  }

  /**
   * 获取节点上所有容器的聚合资源利用率
   * @return 容器聚合资源利用率
   */
  public ResourceUtilization getAggregatedContainersUtilization() {
    return this.nodeStatus.getContainersUtilization();
  }

  /**
   * 获取整个节点的资源利用率
   * @return 节点资源利用率
   */
  public ResourceUtilization getNodeUtilization() {
    return this.nodeStatus.getNodeUtilization();
  }

  /**
   * 获取所有应用的日志聚合状态报告
   * @return 日志聚合报告列表
   */
  public List<LogAggregationReport> getLogAggregationReportsForApps() {
    return this.logAggregationReportsForApps;
  }

  /**
   * 获取机会型容器的状态信息
   * @return 机会型容器状态
   */
  public OpportunisticContainersStatus getOpportunisticContainersStatus() {
    return this.nodeStatus.getOpportunisticContainersStatus();
  }

  /**
   * 设置应用日志聚合状态报告列表
   * @param logAggregationReportsForApps 日志聚合报告列表
   */
  public void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationReportsForApps) {
    this.logAggregationReportsForApps = logAggregationReportsForApps;
  }
  
  /**
   * 获取NodeManager上报的已扩容容器列表
   * @return 已扩容容器列表，无扩容容器则返回空列表
   */
  public List<Container> getNMReportedIncreasedContainers() {
    return this.nodeStatus.getIncreasedContainers() == null ?
        Collections.emptyList() : this.nodeStatus.getIncreasedContainers();
  }


}