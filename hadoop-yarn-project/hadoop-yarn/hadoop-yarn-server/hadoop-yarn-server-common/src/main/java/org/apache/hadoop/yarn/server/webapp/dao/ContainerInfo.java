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

package org.apache.hadoop.yarn.server.webapp.dao;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.Times;

/**
 * YARN Web UI 容器信息数据访问对象，封装容器基本信息用于Web接口序列化返回
 */
@Public
@Evolving
@XmlRootElement(name = "container")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerInfo {

  // 容器ID字符串
  protected String containerId;
  // 分配的内存大小(MB)
  protected long allocatedMB;
  // 分配的虚拟CPU核心数
  protected long allocatedVCores;
  // 分配的节点ID
  protected String assignedNodeId;
  // 容器调度优先级
  protected int priority;
  // 容器启动时间
  protected long startedTime;
  // 容器结束时间
  protected long finishedTime;
  // 容器运行总耗时
  protected long elapsedTime;
  // 容器诊断信息
  protected String diagnosticsInfo;
  // 容器日志访问URL
  protected String logUrl;
  // 容器退出状态码
  protected int containerExitStatus;
  // 容器当前状态
  protected ContainerState containerState;
  // 节点HTTP服务地址
  protected String nodeHttpAddress;
  // 节点ID字符串
  protected String nodeId;
  // 所有已分配资源的映射表，key为资源名称，value为资源值
  protected Map<String, Long> allocatedResources;
  // 容器暴露的端口列表字符串
  private String exposedPorts;

  /**
   * JAXB反序列化需要的无参构造函数
   */
  public ContainerInfo() {
    // JAXB needs this
  }

  /**
   * 从ContainerReport构造ContainerInfo对象，提取并转换容器相关信息
   * @param container 容器报告对象，包含容器的完整状态信息
   */
  public ContainerInfo(ContainerReport container) {
    if (container.getAssignedNode() != null) {
      assignedNodeId = container.getAssignedNode().toString();
    }

    containerId = container.getContainerId().toString();
    priority = container.getPriority().getPriority();
    startedTime = container.getCreationTime();
    finishedTime = container.getFinishTime();
    elapsedTime = Times.elapsed(startedTime, finishedTime);
    diagnosticsInfo = container.getDiagnosticsInfo();
    logUrl = container.getLogUrl();
    containerExitStatus = container.getContainerExitStatus();
    containerState = container.getContainerState();
    nodeHttpAddress = container.getNodeHttpAddress();
    nodeId = container.getAssignedNode().toString();
    exposedPorts = container.getExposedPorts();

    Resource allocated = container.getAllocatedResource();
    if (allocated != null) {
      allocatedMB = allocated.getMemorySize();
      allocatedVCores = allocated.getVirtualCores();

      // 填充所有资源到映射表，包含内存和CPU，保持向后兼容旧API
      allocatedResources = new HashMap<>();

      for (ResourceInformation info : allocated.getResources()) {
        allocatedResources.put(info.getName(), info.getValue());
      }
    }
  }

  public String getContainerId() {
    return containerId;
  }

  public long getAllocatedMB() {
    return allocatedMB;
  }

  public long getAllocatedVCores() {
    return allocatedVCores;
  }

  public String getAssignedNodeId() {
    return assignedNodeId;
  }

  public int getPriority() {
    return priority;
  }

  public long getStartedTime() {
    return startedTime;
  }

  public long getFinishedTime() {
    return finishedTime;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  public String getLogUrl() {
    return logUrl;
  }

  public int getContainerExitStatus() {
    return containerExitStatus;
  }

  public ContainerState getContainerState() {
    return containerState;
  }

  public String getNodeHttpAddress() {
    return nodeHttpAddress;
  }

  public String getNodeId() {
    return nodeId;
  }

  /**
   * Return a map of the allocated resources. The map key is the resource name,
   * and the value is the resource value.
   *
   * @return the allocated resources map
   */
  public Map<String, Long> getAllocatedResources() {
    return Collections.unmodifiableMap(allocatedResources);
  }

  public String getExposedPorts() {
    return exposedPorts;
  }

  /**
   * 检查容器是否配置了内存和CPU之外的自定义资源
   * @return true 存在自定义资源，false 仅包含默认内存CPU资源
   */
  public boolean hasCustomResources() {
    return allocatedResources.size() > 2;
  }
}