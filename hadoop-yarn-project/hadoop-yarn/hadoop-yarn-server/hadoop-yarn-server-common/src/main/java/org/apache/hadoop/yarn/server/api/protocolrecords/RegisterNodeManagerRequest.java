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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.util.Records;

/**
 * NodeManager向ResourceManager注册时的请求记录
 * 包含节点基本信息、当前运行状态、容器信息等注册所需的全部内容
 */
public abstract class RegisterNodeManagerRequest {

  /**
   * 创建RegisterNodeManagerRequest实例（最简参数）
   * @param nodeId 节点ID
   * @param httpPort HTTP服务端口
   * @param resource 节点可分配资源
   * @param nodeManagerVersionId NodeManager版本号
   * @param containerStatuses 容器状态列表
   * @param runningApplications 运行中应用ID列表
   * @return 注册请求实例
   */
  public static RegisterNodeManagerRequest newInstance(NodeId nodeId,
      int httpPort, Resource resource, String nodeManagerVersionId,
      List<NMContainerStatus> containerStatuses,
      List<ApplicationId> runningApplications) {
    return newInstance(nodeId, httpPort, resource, nodeManagerVersionId,
        containerStatuses, runningApplications, null);
  }

  /**
   * 创建RegisterNodeManagerRequest实例（添加节点标签）
   * @param nodeId 节点ID
   * @param httpPort HTTP服务端口
   * @param resource 节点可分配资源
   * @param nodeManagerVersionId NodeManager版本号
   * @param containerStatuses 容器状态列表
   * @param runningApplications 运行中应用ID列表
   * @param nodeLabels 节点标签集合
   * @return 注册请求实例
   */
  public static RegisterNodeManagerRequest newInstance(NodeId nodeId,
      int httpPort, Resource resource, String nodeManagerVersionId,
      List<NMContainerStatus> containerStatuses,
      List<ApplicationId> runningApplications, Set<NodeLabel> nodeLabels) {
    return newInstance(nodeId, httpPort, resource, nodeManagerVersionId,
        containerStatuses, runningApplications, nodeLabels, null);
  }

  /**
   * 创建RegisterNodeManagerRequest实例（添加物理资源信息）
   * @param nodeId 节点ID
   * @param httpPort HTTP服务端口
   * @param resource 节点可分配资源
   * @param nodeManagerVersionId NodeManager版本号
   * @param containerStatuses 容器状态列表
   * @param runningApplications 运行中应用ID列表
   * @param nodeLabels 节点标签集合
   * @param physicalResource 节点物理总资源
   * @return 注册请求实例
   */
  public static RegisterNodeManagerRequest newInstance(NodeId nodeId,
      int httpPort, Resource resource, String nodeManagerVersionId,
      List<NMContainerStatus> containerStatuses,
      List<ApplicationId> runningApplications, Set<NodeLabel> nodeLabels,
      Resource physicalResource) {
    return newInstance(nodeId, httpPort, resource, nodeManagerVersionId,
        containerStatuses, runningApplications, nodeLabels, physicalResource,
        null, null);
  }

  /**
   * 创建RegisterNodeManagerRequest实例（全参数）
   * @param nodeId 节点ID
   * @param httpPort HTTP服务端口
   * @param resource 节点可分配资源
   * @param nodeManagerVersionId NodeManager版本号
   * @param containerStatuses 容器状态列表
   * @param runningApplications 运行中应用ID列表
   * @param nodeLabels 节点标签集合
   * @param physicalResource 节点物理总资源
   * @param nodeAttributes 节点属性集合
   * @param nodeStatus 节点状态
   * @return 注册请求实例
   */
  public static RegisterNodeManagerRequest newInstance(NodeId nodeId,
      int httpPort, Resource resource, String nodeManagerVersionId,
      List<NMContainerStatus> containerStatuses,
      List<ApplicationId> runningApplications, Set<NodeLabel> nodeLabels,
      Resource physicalResource, Set<NodeAttribute> nodeAttributes,
      NodeStatus nodeStatus) {
    // 通过Records工具创建实例
    RegisterNodeManagerRequest request =
        Records.newRecord(RegisterNodeManagerRequest.class);
    request.setHttpPort(httpPort);
    request.setResource(resource);
    request.setNodeId(nodeId);
    request.setNMVersion(nodeManagerVersionId);
    request.setContainerStatuses(containerStatuses);
    request.setRunningApplications(runningApplications);
    request.setNodeLabels(nodeLabels);
    request.setPhysicalResource(physicalResource);
    request.setNodeAttributes(nodeAttributes);
    request.setNodeStatus(nodeStatus);
    return request;
  }
  
  public abstract NodeId getNodeId();
  public abstract int getHttpPort();
  public abstract Resource getResource();
  public abstract String getNMVersion();
  public abstract List<NMContainerStatus> getNMContainerStatuses();
  public abstract Set<NodeLabel> getNodeLabels();
  public abstract void setNodeLabels(Set<NodeLabel> nodeLabels);
  
  /**
   * We introduce this here because currently YARN RM doesn't persist nodes info
   * for application running. When RM restart happened, we cannot determinate if
   * a node should do application cleanup (like log-aggregation, status update,
   * etc.) or not.
   * <p>
   * When we have this running application list in node manager register
   * request, we can recover nodes info for running applications. And then we
   * can take actions accordingly
   * 
   * @return running application list in this node
   */
  public abstract List<ApplicationId> getRunningApplications();
  
  public abstract void setNodeId(NodeId nodeId);
  public abstract void setHttpPort(int port);
  public abstract void setResource(Resource resource);
  public abstract void setNMVersion(String version);
  public abstract void setContainerStatuses(
      List<NMContainerStatus> containerStatuses);
  
  /**
   * Setter for {@link RegisterNodeManagerRequest#getRunningApplications()}
   * @param runningApplications running application in this node
   */
  public abstract void setRunningApplications(
      List<ApplicationId> runningApplications);

  /**
   * Get the physical resources in the node to properly estimate resource
   * utilization.
   * @return Physical resources in the node.
   */
  public abstract Resource getPhysicalResource();

  /**
   * Set the physical resources in the node to properly estimate resource
   * utilization.
   * @param physicalResource Physical resources in the node.
   */
  public abstract void setPhysicalResource(Resource physicalResource);

  /**
   * 获取应用日志聚合报告列表
   * @return 应用日志聚合报告列表
   */
  public abstract List<LogAggregationReport> getLogAggregationReportsForApps();

  /**
   * 设置应用日志聚合报告列表
   * @param logAggregationReportsForApps 应用日志聚合报告列表
   */
  public abstract void setLogAggregationReportsForApps(
      List<LogAggregationReport> logAggregationReportsForApps);

  /**
   * 获取节点属性集合
   * @return 节点属性集合
   */
  public abstract Set<NodeAttribute> getNodeAttributes();

  /**
   * 设置节点属性集合
   * @param nodeAttributes 节点属性集合
   */
  public abstract void setNodeAttributes(Set<NodeAttribute> nodeAttributes);

  /**
   * Get the status of the node.
   * @return The status of the node.
   */
  public abstract NodeStatus getNodeStatus();

  /**
   * Set the status of the node.
   * @param nodeStatus The status of the node.
   */
  public abstract void setNodeStatus(NodeStatus nodeStatus);
}