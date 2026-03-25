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
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * YARN容量调度器容器分配器入口，代理实际的容器分配逻辑，
 * 为容量调度器中的应用提供容器分配能力。
 */
public class ContainerAllocator extends AbstractContainerAllocator {
  // 持有常规容器分配器实例，实际分配逻辑委托给它处理
  private AbstractContainerAllocator regularContainerAllocator;

  /**
   * 构造容器分配器实例。
   * @param application 待分配容器的调度应用
   * @param rc 资源计算器，用于资源大小比较计算
   * @param rmContext RM上下文对象，持有全局资源信息
   */
  public ContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext) {
    this(application, rc, rmContext, null);
  }

  /**
   * 构造容器分配器实例，支持传入活动管理器。
   * @param application 待分配容器的调度应用
   * @param rc 资源计算器，用于资源大小比较计算
   * @param rmContext RM上下文对象，持有全局资源信息
   * @param activitiesManager 调度活动管理器，用于记录调度审计日志
   */
  public ContainerAllocator(FiCaSchedulerApp application, ResourceCalculator rc,
      RMContext rmContext, ActivitiesManager activitiesManager) {
    super(application, rc, rmContext);

    // 初始化常规容器分配器，处理普通容器分配请求
    regularContainerAllocator = new RegularContainerAllocator(application, rc,
        rmContext, activitiesManager);
  }

  /**
   * 分配容器，将请求委托给常规容器分配器处理。
   * @param clusterResource 集群总资源
   * @param candidates 候选节点集合，可分配容器的节点
   * @param schedulingMode 调度模式（独占/共享）
   * @param resourceLimits 资源使用限制
   * @param reservedContainer 预留容器（如果是处理预留容器分配则非空）
   * @return 容器分配结果
   */
  @Override
  public CSAssignment assignContainers(Resource clusterResource,
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      SchedulingMode schedulingMode, ResourceLimits resourceLimits,
      RMContainer reservedContainer) {
    return regularContainerAllocator.assignContainers(clusterResource,
        candidates, schedulingMode, resourceLimits, reservedContainer);
  }

}