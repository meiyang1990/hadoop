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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.List;
import java.util.Map;

/**
 * YARN应用历史服务中RM容器的持久化存储历史数据，保存容器全生命周期核心信息。
 */
@Public
@Unstable
public class ContainerHistoryData {

  private ContainerId containerId;

  private Resource allocatedResource;

  private NodeId assignedNode;

  private Priority priority;

  private long startTime;

  private long finishTime;

  private String diagnosticsInfo;

  private int containerExitStatus;

  private ContainerState containerState;

  private Map<String, List<Map<String, String>>> exposedPorts;

  /**
   * 创建容器历史数据实例，初始化所有核心字段。
   * @param containerId 容器ID
   * @param allocatedResource 分配给容器的资源
   * @param assignedNode 容器分配到的节点ID
   * @param priority 容器调度优先级
   * @param startTime 容器启动时间
   * @param finishTime 容器结束时间
   * @param diagnosticsInfo 容器诊断信息
   * @param containerExitCode 容器退出码
   * @param containerState 容器最终状态
   * @return 初始化完成的容器历史数据实例
   */
  @Public
  @Unstable
  public static ContainerHistoryData newInstance(ContainerId containerId,
      Resource allocatedResource, NodeId assignedNode, Priority priority,
      long startTime, long finishTime, String diagnosticsInfo,
      int containerExitCode, ContainerState containerState) {
    ContainerHistoryData containerHD = new ContainerHistoryData();
    containerHD.setContainerId(containerId);
    containerHD.setAllocatedResource(allocatedResource);
    containerHD.setAssignedNode(assignedNode);
    containerHD.setPriority(priority);
    containerHD.setStartTime(startTime);
    containerHD.setFinishTime(finish);
    containerHD.setDiagnosticsInfo(diagnosticsInfo);
    containerHD.setContainerExitStatus(containerExitCode);
    containerHD.setContainerState(containerState);

    return containerHD;
  }

  @Public
  @Unstable
  public ContainerId getContainerId() {
    return containerId;
  }

  @Public
  @Unstable
  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  @Public
  @Unstable
  public Resource getAllocatedResource() {
    return allocatedResource;
  }

  @Public
  @Unstable
  public void setAllocatedResource(Resource resource) {
    this.allocatedResource = resource;
  }

  @Public
  @Unstable
  public NodeId getAssignedNode() {
    return assignedNode;
  }

  @Public
  @Unstable
  public void setAssignedNode(NodeId nodeId) {
    this.assignedNode = nodeId;
  }

  @Public
  @Unstable
  public Priority getPriority() {
    return priority;
  }

  @Public
  @Unstable
  public void setPriority(Priority priority) {
    this.priority = priority;
  }

  @Public
  @Unstable
  public long getStartTime() {
    return startTime;
  }

  @Public
  @Unstable
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Public
  @Unstable
  public long getFinishTime() {
    return finishTime;
  }

  @Public
  @Unstable
  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  @Public
  @Unstable
  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  @Public
  @Unstable
  public void setDiagnosticsInfo(String diagnosticsInfo) {
    this.diagnosticsInfo = diagnosticsInfo;
  }

  @Public
  @Unstable
  public int getContainerExitStatus() {
    return containerExitStatus;
  }

  @Public
  @Unstable
  public void setContainerExitStatus(int containerExitStatus) {
    this.containerExitStatus = containerExitStatus;
  }

  @Public
  @Unstable
  public ContainerState getContainerState() {
    return containerState;
  }

  @Public
  @Unstable
  public void setContainerState(ContainerState containerState) {
    this.containerState = containerState;
  }

  /**
   * 获取容器暴露的端口映射信息。
   * @return 容器暴露端口信息
   */
  public Map<String, List<Map<String, String>>> getExposedPorts() {
    return exposedPorts;
  }

  /**
   * 设置容器暴露的端口映射信息。
   * @param ports 容器暴露端口信息
   */
  public void setExposedPorts(Map<String, List<Map<String, String>>> ports) {
    this.exposedPorts = ports;
  }
}