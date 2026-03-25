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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN RM Web UI 节点信息数据访问对象，封装节点的完整信息用于Web响应序列化输出
 */
@XmlRootElement(name = "node")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeInfo {

  protected String rack;
  protected NodeState state;
  private String id;
  protected String nodeHostName;
  protected String nodeHTTPAddress;
  private long lastHealthUpdate;
  protected String version;
  protected String healthReport;
  protected int numContainers;
  protected long usedMemoryMB;
  protected long availMemoryMB;
  protected long usedVirtualCores;
  protected long availableVirtualCores;
  private float memUtilization;
  private float cpuUtilization;
  private int numRunningOpportContainers;
  private long usedMemoryOpportGB;
  private long usedVirtualCoresOpport;
  private int numQueuedContainers;
  protected ArrayList<String> nodeLabels = new ArrayList<String>();
  private AllocationTagsInfo allocationTags;
  protected ResourceUtilizationInfo resourceUtilization;
  protected ResourceInfo usedResource;
  protected ResourceInfo availableResource;
  protected NodeAttributesInfo nodeAttributesInfo;
  private ResourceInfo totalResource;
  private String subClusterId;

  /** JAXB要求的无参构造函数 */
  public NodeInfo() {
  } // JAXB needs this

  /**
   * 根据RM节点和调度器信息构造NodeInfo，从运行时对象提取Web需要展示的信息
   * @param ni RM节点运行时对象
   * @param sched 资源调度器对象
   */
  public NodeInfo(RMNode ni, ResourceScheduler sched) {
    NodeId id = ni.getNodeID();
    SchedulerNodeReport report = sched.getNodeReport(id);
    this.numContainers = 0;
    this.usedMemoryMB = 0;
    this.availMemoryMB = 0;
    // 调度报告存在时从报告提取资源使用信息
    if (report != null) {
      this.numContainers = report.getNumContainers();
      this.usedMemoryMB = report.getUsedResource().getMemorySize();
      this.availMemoryMB = report.getAvailableResource().getMemorySize();
      this.usedVirtualCores = report.getUsedResource().getVirtualCores();
      this.availableVirtualCores =
          report.getAvailableResource().getVirtualCores();
      this.usedResource = new ResourceInfo(report.getUsedResource());
      this.availableResource = new ResourceInfo(report.getAvailableResource());
      Resource totalPhysical = ni.getPhysicalResource();
      long nodeMem;
      long nodeCores;
      // 物理资源信息不存在时，推算总资源
      if (totalPhysical == null) {
        nodeMem =
            this.usedMemoryMB + this.availMemoryMB;
        // 如果不知道物理核心数，默认设为1，总比没有好
        nodeCores = 1;
      } else {
        nodeMem = totalPhysical.getMemorySize();
        nodeCores = totalPhysical.getVirtualCores();
      }
      // 计算内存利用率百分比
      this.memUtilization = nodeMem <= 0 ? 0
          : (float)report.getUtilization().getPhysicalMemory() * 100F / nodeMem;
      // 计算CPU利用率百分比
      this.cpuUtilization =
          (float)report.getUtilization().getCPU() * 100F / nodeCores;
    }
    this.id = id.toString();
    this.rack = ni.getRackName();
    this.nodeHostName = ni.getHostName();
    this.state = ni.getState();
    this.nodeHTTPAddress = ni.getHttpAddress();
    this.lastHealthUpdate = ni.getLastHealthReportTime();
    this.healthReport = String.valueOf(ni.getHealthReport());
    this.version = ni.getNodeManagerVersion();
    this.totalResource = new ResourceInfo(ni.getTotalCapability());

    // 初始化机会容器状态信息
    this.numRunningOpportContainers = 0;
    this.usedMemoryOpportGB = 0;
    this.usedVirtualCoresOpport = 0;
    this.numQueuedContainers = 0;
    OpportunisticContainersStatus opportStatus =
        ni.getOpportunisticContainersStatus();
    if (opportStatus != null) {
      this.numRunningOpportContainers =
          opportStatus.getRunningOpportContainers();
      this.usedMemoryOpportGB = opportStatus.getOpportMemoryUsed();
      this.usedVirtualCoresOpport = opportStatus.getOpportCoresUsed();
      this.numQueuedContainers = opportStatus.getQueuedOpportContainers();
    }

    // 处理节点标签，排序后存储
    Set<String> labelSet = ni.getNodeLabels();
    if (labelSet != null) {
      nodeLabels.addAll(labelSet);
      Collections.sort(nodeLabels);
    }

    // 处理节点属性
    Set<NodeAttribute> attrs = ni.getAllNodeAttributes();
    nodeAttributesInfo = new NodeAttributesInfo();
    for (NodeAttribute attribute : attrs) {
      NodeAttributeInfo info = new NodeAttributeInfo(attribute);
      this.nodeAttributesInfo.addNodeAttributeInfo(info);
    }

    // 处理分配标签
    allocationTags = new AllocationTagsInfo();
    Map<String, Long> allocationTagsInfo = ni.getAllocationTagsWithCount();
    if (allocationTagsInfo != null) {
      allocationTagsInfo.forEach((tag, count) ->
          allocationTags.addAllocationTag(new AllocationTagInfo(tag, count)));
    }

    // 更新节点和容器资源利用率信息
    this.resourceUtilization = new ResourceUtilizationInfo(ni);
  }

  public String getRack() {
    return this.rack;
  }

  public String getState() {
    return String.valueOf(this.state);
  }

  public String getNodeId() {
    return this.id;
  }

  public String getNodeHTTPAddress() {
    return this.nodeHTTPAddress;
  }

  public void setNodeHTTPAddress(String nodeHTTPAddress) {
    this.nodeHTTPAddress = nodeHTTPAddress;
  }

  public long getLastHealthUpdate() {
    return this.lastHealthUpdate;
  }

  public String getVersion() {
    return this.version;
  }

  public String getHealthReport() {
    return this.healthReport;
  }

  public int getNumContainers() {
    return this.numContainers;
  }

  public long getUsedMemory() {
    return this.usedMemoryMB;
  }

  public long getAvailableMemory() {
    return this.availMemoryMB;
  }

  public long getUsedVirtualCores() {
    return this.usedVirtualCores;
  }

  public long getAvailableVirtualCores() {
    return this.availableVirtualCores;
  }

  public int getNumRunningOpportContainers() {
    return numRunningOpportContainers;
  }

  public long getUsedMemoryOpportGB() {
    return usedMemoryOpportGB;
  }

  public long getUsedVirtualCoresOpport() {
    return usedVirtualCoresOpport;
  }

  public int getNumQueuedContainers() {
    return numQueuedContainers;
  }

  public ArrayList<String> getNodeLabels() {
    return this.nodeLabels;
  }

  public ResourceInfo getUsedResource() {
    return usedResource;
  }

  public void setUsedResource(ResourceInfo used) {
    this.usedResource = used;
  }

  public ResourceInfo getAvailableResource() {
    return availableResource;
  }

  public void setAvailableResource(ResourceInfo avail) {
    this.availableResource = avail;
  }

  public ResourceUtilizationInfo getResourceUtilization() {
    return this.resourceUtilization;
  }

  public float getMemUtilization() {
    return memUtilization;
  }

  public void setMemUtilization(float util) {
    this.memUtilization = util;
  }

  public float getVcoreUtilization() {
    return cpuUtilization;
  }

  public void setVcoreUtilization(float util) {
    this.cpuUtilization = util;
  }

  public String getAllocationTagsSummary() {
    return this.allocationTags == null ? "" :
        this.allocationTags.toString();
  }

  @VisibleForTesting
  public void setId(String id) {
    this.id = id;
  }

  @VisibleForTesting
  public void setLastHealthUpdate(long lastHealthUpdate) {
    this.lastHealthUpdate = lastHealthUpdate;
  }

  public void setTotalResource(ResourceInfo total) {
    this.totalResource = total;
  }

  public ResourceInfo getTotalResource() {
    return this.totalResource;
  }

  public String getSubClusterId() {
    return subClusterId;
  }

  public void setSubClusterId(String subClusterId) {
    this.subClusterId = subClusterId;
  }
}