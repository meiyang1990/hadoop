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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
attemptimport org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;

import java.util.Collections;
import java.util.List;

/**
 * 文件说明：YARN资源调度器容器分配提议类，代表一次容器分配或预留的调度提议
 * 核心职责：封装一次容器分配请求所需的所有信息，包括需要分配/预留的容器、需要提前释放的容器等
 */
public class ContainerAllocationProposal<A extends SchedulerApplicationAttempt,
    N extends SchedulerNode> {
  // 本次提议分配或预留的容器
  private SchedulerContainer<A, N> allocatedOrReservedContainer;

  // 分配或预留新容器前需要提前释放的容器列表
  private List<SchedulerContainer<A, N>> toRelease = Collections.emptyList();

  // 从预留容器分配时存储原预留容器，该容器不会被加入到释放列表
  private SchedulerContainer<A, N> allocateFromReservedContainer;

  // 本次分配的节点位置类型
  private NodeType allocationLocalityType;

  // 请求要求的节点位置类型
  private NodeType requestLocalityType;

  // 本次分配使用的调度模式
  private SchedulingMode schedulingMode;

  // 本次新分配的资源总量
  private Resource allocatedResource;

  /**
   * 构造容器分配提议
   * @param allocatedOrReservedContainer 本次分配或预留的容器
   * @param toRelease 分配前需要释放的容器列表
   * @param allocateFromReservedContainer 从中分配的原预留容器
   * @param allocationLocalityType 实际分配的位置类型
   * @param requestLocalityType 请求要求的位置类型
   * @param schedulingMode 调度模式
   * @param allocatedResource 本次分配的资源总量
   */
  public ContainerAllocationProposal(
      SchedulerContainer<A, N> allocatedOrReservedContainer,
      List<SchedulerContainer<A, N>> toRelease,
      SchedulerContainer<A, N> allocateFromReservedContainer,
      NodeType allocationLocalityType,
      NodeType requestLocalityType, SchedulingMode schedulingMode,
      Resource allocatedResource) {
    this.allocatedOrReservedContainer = allocatedOrReservedContainer;
    if (null != toRelease) {
      this.toRelease = toRelease;
    }
    this.allocateFromReservedContainer = allocateFromReservedContainer;
    this.allocationLocalityType = allocationLocalityType;
    this.requestLocalityType = requestLocalityType;
    this.schedulingMode = schedulingMode;
    this.allocatedResource = allocatedResource;
  }

  /** 获取本次分配的调度模式 */
  public SchedulingMode getSchedulingMode() {
    return schedulingMode;
  }

  /** 获取本次分配或预留的资源总量 */
  public Resource getAllocatedOrReservedResource() {
    return allocatedResource;
  }

  /** 获取实际分配的节点位置类型 */
  public NodeType getAllocationLocalityType() {
    return allocationLocalityType;
  }

  /** 获取从中分配的原预留容器 */
  public SchedulerContainer<A, N> getAllocateFromReservedContainer() {
    return allocateFromReservedContainer;
  }

  /** 获取本次分配或预留的容器 */
  public SchedulerContainer<A, N> getAllocatedOrReservedContainer() {
    return allocatedOrReservedContainer;
  }

  /** 获取本次分配前需要释放的容器列表 */
  public List<SchedulerContainer<A, N>> getToRelease() {
    return toRelease;
  }

  @Override
  public String toString() {
    return allocatedOrReservedContainer.toString();
  }

  /** 获取请求要求的节点位置类型 */
  public NodeType getRequestLocalityType() {
    return requestLocalityType;
  }
}