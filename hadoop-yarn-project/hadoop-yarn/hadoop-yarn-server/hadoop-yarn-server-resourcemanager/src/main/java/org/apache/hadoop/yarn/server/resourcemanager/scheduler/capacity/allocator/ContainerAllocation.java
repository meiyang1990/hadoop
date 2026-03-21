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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.List;

/**
 * 容量调度器容器分配结果封装类，用于表示一次容器分配尝试的结果状态与相关信息
 */
public class ContainerAllocation {
  /**
   * 跳过当前位置性查找，继续查找同优先级其他位置性请求
   */
  public static final ContainerAllocation LOCALITY_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.LOCALITY_SKIPPED);

  /**
   * 跳过当前优先级，继续查找同一应用的其他优先级请求
   */
  public static final ContainerAllocation PRIORITY_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.PRIORITY_SKIPPED);

  /**
   * 跳过当前应用，继续查找同一队列的其他应用请求
   */
  public static final ContainerAllocation APP_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.APP_SKIPPED);

  /**
   * 跳过当前叶子队列，继续查找同一父队列下的其他队列请求
   */
  public static final ContainerAllocation QUEUE_SKIPPED =
      new ContainerAllocation(null, null, AllocationState.QUEUE_SKIPPED);

  // 需要取消预留的容器
  RMContainer containerToBeUnreserved;
  // 本次待分配的资源量
  private Resource resourceToBeAllocated = Resources.none();
  // 分配状态
  private AllocationState state;
  // 容器所在节点类型
  NodeType containerNodeType = NodeType.NODE_LOCAL;
  // 请求要求的位置性类型
  NodeType requestLocalityType = null;

  /**
   * 当分配/预留新容器或扩容容器时，存储更新后的容器对象
   */
  RMContainer updatedContainer;
  // 需要杀死的容器列表（用于抢占等场景）
  private List<RMContainer> toKillContainers;

  /**
   * 构造容器分配结果对象
   * @param containerToBeUnreserved 需要取消预留的容器
   * @param resourceToBeAllocated 本次待分配的资源
   * @param state 分配状态
   */
  public ContainerAllocation(RMContainer containerToBeUnreserved,
      Resource resourceToBeAllocated, AllocationState state) {
    this.containerToBeUnreserved = containerToBeUnreserved;
    this.resourceToBeAllocated = resourceToBeAllocated;
    this.state = state;
  }

  /** 获取需要取消预留的容器
   * @return 需要取消预留的容器
   */
  public RMContainer getContainerToBeUnreserved() {
    return containerToBeUnreserved;
  }

  /** 获取本次待分配的资源量
   * @return 待分配资源量
   */
  public Resource getResourceToBeAllocated() {
    if (resourceToBeAllocated == null) {
      return Resources.none();
    }
    return resourceToBeAllocated;
  }

  /** 获取本次分配的状态
   * @return 分配状态
   */
  public AllocationState getAllocationState() {
    return state;
  }

  /** 获取容器所在节点类型
   * @return 节点类型
   */
  public NodeType getContainerNodeType() {
    return containerNodeType;
  }

  /** 获取更新后的容器对象
   * @return 更新后的容器对象
   */
  public RMContainer getUpdatedContainer() {
    return updatedContainer;
  }

  /** 设置需要杀死的容器列表
   * @param toKillContainers 需要杀死的容器列表
   */
  public void setToKillContainers(List<RMContainer> toKillContainers) {
    this.toKillContainers = toKillContainers;
  }

  /** 获取需要杀死的容器列表
   * @return 需要杀死的容器列表
   */
  public List<RMContainer> getToKillContainers() {
    return toKillContainers;
  }
}