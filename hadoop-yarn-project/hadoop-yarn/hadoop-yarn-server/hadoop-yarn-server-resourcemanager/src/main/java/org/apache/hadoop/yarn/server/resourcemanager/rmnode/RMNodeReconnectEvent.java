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
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;

/**
 * 节点重连事件，当NodeManager重新连接ResourceManager时触发
 * 携带重连后的节点信息、正在运行的应用列表和容器状态信息
 */
public class RMNodeReconnectEvent extends RMNodeEvent {
  // 重连后的RMNode对象
  private RMNode reconnectedNode;
  // 该节点上正在运行的应用ID列表
  private List<ApplicationId> runningApplications;
  // 该节点上所有容器的状态列表
  private List<NMContainerStatus> containerStatuses;

  /**
   * 构造节点重连事件
   * @param nodeId 重连节点ID
   * @param newNode 重连后的RMNode对象
   * @param runningApps 节点上正在运行的应用ID列表
   * @param containerReports 节点上容器状态列表
   */
  public RMNodeReconnectEvent(NodeId nodeId, RMNode newNode,
      List<ApplicationId> runningApps, List<NMContainerStatus> containerReports) {
    super(nodeId, RMNodeEventType.RECONNECTED);
    reconnectedNode = newNode;
    runningApplications = runningApps;
    containerStatuses = containerReports;
  }

  /**
   * 获取重连后的节点对象
   * @return 重连后的RMNode
   */
  public RMNode getReconnectedNode() {
    return reconnectedNode;
  }

  /**
   * 获取节点重连时正在运行的应用列表
   * @return 正在运行的应用ID列表
   */
  public List<ApplicationId> getRunningApplications() {
    return runningApplications;
  }

  /**
   * 获取节点重连时所有容器的状态列表
   * @return 容器状态列表
   */
  public List<NMContainerStatus> getNMContainerStatuses() {
    return containerStatuses;
  }
}