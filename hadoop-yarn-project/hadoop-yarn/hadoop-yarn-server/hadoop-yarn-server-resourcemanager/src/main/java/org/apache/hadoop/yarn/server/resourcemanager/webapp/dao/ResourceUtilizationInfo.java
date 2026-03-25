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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * YARN RM Web UI 节点资源利用率数据访问对象，封装节点整体和容器聚合的资源使用信息
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ResourceUtilizationInfo {

  // 节点物理内存用量，单位MB
  protected int nodePhysicalMemoryMB;
  // 节点虚拟内存用量，单位MB
  protected int nodeVirtualMemoryMB;
  // 节点CPU使用率
  protected double nodeCPUUsage;
  // 所有容器聚合物理内存用量，单位MB
  protected int aggregatedContainersPhysicalMemoryMB;
  // 所有容器聚合虚拟内存用量，单位MB
  protected int aggregatedContainersVirtualMemoryMB;
  // 所有容器聚合CPU使用率
  protected double containersCPUUsage;

  public ResourceUtilizationInfo() {
  } // JAXB needs this

  /**
   * 从RMNode节点信息构造资源利用率数据对象
   * @param ni RM节点信息对象
   */
  public ResourceUtilizationInfo(RMNode ni) {

    // 获取节点整体资源利用率信息
    ResourceUtilization nodeUtilization = ni.getNodeUtilization();
    if (nodeUtilization != null) {
      this.nodePhysicalMemoryMB = nodeUtilization.getPhysicalMemory();
      this.nodeVirtualMemoryMB = nodeUtilization.getVirtualMemory();
      this.nodeCPUUsage = nodeUtilization.getCPU();
    }

    // 获取所有容器聚合资源利用率信息
    ResourceUtilization containerAggrUtilization = ni
        .getAggregatedContainersUtilization();
    if (containerAggrUtilization != null) {
      this.aggregatedContainersPhysicalMemoryMB = containerAggrUtilization
          .getPhysicalMemory();
      this.aggregatedContainersVirtualMemoryMB = containerAggrUtilization
          .getVirtualMemory();
      this.containersCPUUsage = containerAggrUtilization.getCPU();
    }
  }

  public int getNodePhysicalMemoryMB() {
    return nodePhysicalMemoryMB;
  }

  public int getNodeVirtualMemoryMB() {
    return nodeVirtualMemoryMB;
  }

  public int getAggregatedContainersPhysicalMemoryMB() {
    return aggregatedContainersPhysicalMemoryMB;
  }

  public int getAggregatedContainersVirtualMemoryMB() {
    return aggregatedContainersVirtualMemoryMB;
  }

  public double getNodeCPUUsage() {
    return nodeCPUUsage;
  }

  public double getContainersCPUUsage() {
    return containersCPUUsage;
  }
}