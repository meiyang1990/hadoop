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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 资源调度器侧的容器资源变更请求封装类，持有指向运行时对象的引用，方便调度器决策。
 * 封装了用户提交的UpdateContainerRequest，并关联了调度器运行时需要的各类对象。
 */
public class SchedContainerChangeRequest implements
    Comparable<SchedContainerChangeRequest> {
  private RMContext rmContext;
  private RMContainer rmContainer;
  private Resource targetCapacity;
  private SchedulerNode schedulerNode;
  private Resource deltaCapacity;

  /**
   * 构造调度器侧容器资源变更请求
   * @param rmContext RM上下文对象
   * @param schedulerNode 容器所在调度节点
   * @param rmContainer 待变更的容器运行时对象
   * @param targetCapacity 目标资源容量
   */
  public SchedContainerChangeRequest(
      RMContext rmContext, SchedulerNode schedulerNode,
      RMContainer rmContainer, Resource targetCapacity) {
    this.rmContext = rmContext;
    this.rmContainer = rmContainer;
    this.targetCapacity = targetCapacity;
    this.schedulerNode = schedulerNode;
  }
  
  /** 获取容器所在节点ID */
  public NodeId getNodeId() {
    return this.rmContainer.getAllocatedNode();
  }

  /** 获取待变更容器的运行时对象 */
  public RMContainer getRMContainer() {
    return this.rmContainer;
  }

  /** 获取变更后的目标资源容量 */
  public Resource getTargetCapacity() {
    return this.targetCapacity;
  }

  /** 获取RM上下文对象 */
  public RMContext getRmContext() {
    return this.rmContext;
  }

  /**
   * 获取资源变更量（目标资源 - 当前分配资源），缩容时为负值
   * @return 资源变更量
   */
  public synchronized Resource getDeltaCapacity() {
    // 增量只计算一次，延迟初始化
    if (deltaCapacity == null) {
      deltaCapacity = Resources.subtract(
          targetCapacity, rmContainer.getAllocatedResource());
    }
    return deltaCapacity;
  }
  
  /** 获取容器优先级 */
  public Priority getPriority() {
    return rmContainer.getContainer().getPriority();
  }
  
  /** 获取容器ID */
  public ContainerId getContainerId() {
    return rmContainer.getContainerId();
  }
  
  /** 获取节点所在分区 */
  public String getNodePartition() {
    return schedulerNode.getPartition();
  }
  
  /** 获取容器所在调度节点对象 */
  public SchedulerNode getSchedulerNode() {
    return schedulerNode;
  }

  @Override
  public int hashCode() {
    return (getContainerId().hashCode() << 16) + targetCapacity.hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SchedContainerChangeRequest)) {
      return false;
    }
    return compareTo((SchedContainerChangeRequest)other) == 0;
  }

  @Override
  public int compareTo(SchedContainerChangeRequest other) {
    if (other == null) {
      return -1;
    }
    // 先按优先级排序
    int rc = getPriority().compareTo(other.getPriority());
    if (0 != rc) {
      return rc;
    }
    // 同优先级按容器ID排序
    return getContainerId().compareTo(other.getContainerId());
  }
  
  @Override
  public String toString() {
    return "<container=" + getContainerId() + ", targetCapacity="
        + targetCapacity + ", node=" + getNodeId().toString() + ">";
  }
}