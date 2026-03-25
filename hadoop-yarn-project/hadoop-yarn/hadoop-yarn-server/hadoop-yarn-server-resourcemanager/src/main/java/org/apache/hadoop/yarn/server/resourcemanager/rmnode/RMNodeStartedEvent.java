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

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;

/**
 * YARN ResourceManager 节点启动事件，封装NodeManager上线时携带的各类状态信息
 */
public class RMNodeStartedEvent extends RMNodeEvent {

  // NodeManager节点状态信息
  private final NodeStatus nodeStatus;
  // NodeManager上当前所有容器的状态列表
  private List<NMContainerStatus> containerStatuses;
  // NodeManager上当前运行的应用ID列表
  private List<ApplicationId> runningApplications;
  // 各应用的日志聚合状态报告列表
  private List<LogAggregationReport> logAggregationReportsForApps;

  /**
   * 构造RM节点启动事件，存储NodeManager启动上报的各类状态信息
   * @param nodeId 启动的NodeManager节点ID
   * @param containerReports NodeManager上所有容器的状态列表
   * @param runningApplications NodeManager上当前运行的应用ID列表
   * @param nodeStatus NodeManager节点整体状态信息
   */
  public RMNodeStartedEvent(NodeId nodeId,
      List<NMContainerStatus> containerReports,
      List<ApplicationId> runningApplications,
      NodeStatus nodeStatus) {
    super(nodeId, RMNodeEventType.STARTED);
    this.containerStatuses = containerReports;
    this.runningApplications = runningApplications;
    this.nodeStatus = nodeStatus;
  }

  /**
   * 获取NodeManager上报的所有容器状态列表
   * @return 容器状态列表
   */
  public List<NMContainerStatus> getNMContainerStatuses() {
    return this.containerStatuses;
  }
  
  /**
   * 获取NodeManager上当前运行的应用ID列表
   * @return 运行应用ID列表
   */
  public List<ApplicationId> getRunningApplications() {
    return runningApplications;
  }

  /**
   * 获取NodeManager节点整体状态信息
   * @return 节点状态对象
   */
  public NodeStatus getNodeStatus() {
    return nodeStatus;
  }

  /**
   * 获取各应用的日志聚合状态报告列表
   * @return 日志聚合报告列表
   */
  public List<LogAggregationReport> getLogAggregationReportsForApps() {
    return this.logAggregationReportsForApps;
  }

  /**
   * 设置各应用的日志聚合状态报告列表
   * @param logAggregationReportsForApps 日志聚合报告列表
   */
  public void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationReportsForApps) {
    this.logAggregationReportsForApps = logAggregationReportsForApps;
  }
}