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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 容量调度器容器分配抽象基类，定义容器分配的通用接口和基础能力，
 * 使得应用容器分配逻辑可扩展，支持不同的分配策略实现。
 */
public abstract class AbstractContainerAllocator {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContainerAllocator.class);

  /** 当前分配对应的调度应用 */
  FiCaSchedulerApp application;
  /** 应用调度信息 */
  AppSchedulingInfo appInfo;
  /** 资源计算器，用于资源比较和计算 */
  final ResourceCalculator rc;
  /** RM上下文对象，保存RM全局状态 */
  final RMContext rmContext;
  /** 调度活动日志管理器，用于记录分配过程审计日志 */
  ActivitiesManager activitiesManager;

  /**
   * 构造容器分配器实例
   * @param application 当前调度应用
   * @param rc 资源计算器
   * @param rmContext RM上下文
   */
  public AbstractContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext) {
    this(application, rc, rmContext, null);
  }

  /**
   * 构造容器分配器实例（带活动日志管理器）
   * @param application 当前调度应用
   * @param rc 资源计算器
   * @param rmContext RM上下文
   * @param activitiesManager 调度活动日志管理器
   */
  public AbstractContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext,
      ActivitiesManager activitiesManager) {
    this.application = application;
    this.appInfo =
        application == null ? null : application.getAppSchedulingInfo();
    this.rc = rc;
    this.rmContext = rmContext;
    this.activitiesManager = activitiesManager;
  }

  /**
   * 从容器分配结果对象转换生成容量调度分配结果对象
   * @param clusterResource 集群总资源
   * @param result 内部容器分配结果
   * @param rmContainer 预留容器（如果是满足预留分配则不为空）
   * @param node 候选分配节点
   * @return 容量调度分配结果对象，返回给调度核心逻辑处理
   */
  protected CSAssignment getCSAssignmentFromAllocateResult(
      Resource clusterResource, ContainerAllocation result,
      RMContainer rmContainer, FiCaSchedulerNode node) {
    // 处理跳过分配类型
    CSAssignment.SkippedType skipped =
        (result.getAllocationState() == AllocationState.APP_SKIPPED) ?
        CSAssignment.SkippedType.OTHER :
        CSAssignment.SkippedType.NONE;
    CSAssignment assignment = new CSAssignment(skipped);
    assignment.setApplication(application);

    // 设置需要释放的超额预留容器
    assignment.setExcessReservation(result.getContainerToBeUnreserved());

    // 设置请求位置类型
    assignment.setRequestLocalityType(result.requestLocalityType);

    // 存在可分配资源，处理分配结果
    if (Resources.greaterThan(rc, clusterResource,
        result.getResourceToBeAllocated(), Resources.none())) {
      Resource allocatedResource = result.getResourceToBeAllocated();
      RMContainer updatedContainer = result.getUpdatedContainer();

      assignment.setResource(allocatedResource);
      assignment.setType(result.getContainerNodeType());

      if (result.getAllocationState() == AllocationState.RESERVED) {
        if (LOG.isDebugEnabled()) {
          // 重复预留可能会反复发生，仅在debug级别打印日志
          LOG.debug("Reserved container " + " application=" + application
              .getApplicationId() + " resource=" + allocatedResource + " queue="
              + appInfo.getQueueName() + " cluster=" + clusterResource);
        }
        // 添加预留分配详情到分配信息中，用于后续统计和日志
        assignment.getAssignmentInformation().addReservationDetails(
            updatedContainer, application.getCSLeafQueue().getQueuePath());
        // 增加预留计数
        assignment.getAssignmentInformation().incrReservations();
        // 累加预留资源量
        Resources.addTo(assignment.getAssignmentInformation().getReserved(),
            allocatedResource);

        if (rmContainer != null) {
          // 原有存在预留容器，本次分配跳过原有预留，记录活动日志
          ActivitiesLogger.APP.finishSkippedAppAllocationRecording(
              activitiesManager, application.getApplicationId(),
              ActivityState.SKIPPED, ActivityDiagnosticConstant.EMPTY);
        } else {
          // 记录新预留的活动日志
          ActivitiesLogger.APP.finishAllocatedAppAllocationRecording(
              activitiesManager, application.getApplicationId(),
              updatedContainer.getContainerId(), ActivityState.RESERVED,
              ActivityDiagnosticConstant.EMPTY);
        }
      } else if (result.getAllocationState() == AllocationState.ALLOCATED){
        // 本次分配获得新容器，打印分配信息日志
        LOG.info("assignedContainer" + " application attempt=" + application
            .getApplicationAttemptId() + " container=" + updatedContainer
            .getContainerId() + " queue=" + appInfo.getQueueName()
            + " clusterResource=" + clusterResource
            + " type=" + assignment.getType() + " requestedPartition="
            + updatedContainer.getNodeLabelExpression());

        // 添加已分配容器详情
        assignment.getAssignmentInformation().addAllocationDetails(
            updatedContainer, application.getCSLeafQueue().getQueuePath());
        // 增加已分配计数
        assignment.getAssignmentInformation().incrAllocations();
        // 累加已分配资源量
        Resources.addTo(assignment.getAssignmentInformation().getAllocated(),
            allocatedResource);

        if (rmContainer != null) {
          // 标记本次分配满足了一个已有预留
          assignment.setFulfilledReservation(true);
          assignment.setFulfilledReservedContainer(rmContainer);
        }

        // 记录分配活动日志
        ActivitiesLogger.APP.recordAppActivityWithAllocation(activitiesManager,
            node, application, updatedContainer, ActivityState.ALLOCATED);
        ActivitiesLogger.APP.finishAllocatedAppAllocationRecording(
            activitiesManager, application.getApplicationId(),
            updatedContainer.getContainerId(), ActivityState.ALLOCATED,
            ActivityDiagnosticConstant.EMPTY);

        // 更新应用未确认资源量（等待节点确认分配）
        application.incUnconfirmedRes(allocatedResource);
      }

      // 设置分配后需要杀死的容器列表
      assignment.setContainersToKill(result.getToKillContainers());
    } else {
      // 无资源分配，判断是否是队列资源不足导致跳过
      if (result.getAllocationState() == AllocationState.QUEUE_SKIPPED) {
        assignment.setSkippedType(
            CSAssignment.SkippedType.QUEUE_LIMIT);
      }
      // 记录跳过分配的活动日志
      ActivitiesLogger.APP.finishSkippedAppAllocationRecording(
          activitiesManager, application.getApplicationId(),
          ActivityState.SKIPPED, ActivityDiagnosticConstant.EMPTY);
    }

    return assignment;
  }

  /**
   * 为应用分配容器，核心抽象方法，不同策略子类实现不同分配逻辑，需要处理三个核心步骤：
   * <ul>
   * <li>选择分配请求：根据优先级、位置性、需求选择待分配的资源请求</li>
   * <li>资源检查：基于资源可用性和队列限制检查是否可分配</li>
   * <li>执行分配：创建已分配/预留容器对象，更新相关指标和状态</li>
   * </ul>
   *
   * @param clusterResource 集群总资源
   * @param candidates 候选分配节点集合
   * @param schedulingMode 调度模式（独占/非独占）
   * @param resourceLimits 资源分配限制（队列资源限额等）
   * @param reservedContainer 需要满足的预留容器，如果不为空说明本次分配是为了满足已有预留
   * @return 容量调度分配结果对象，包含分配信息
   */
  public abstract CSAssignment assignContainers(Resource clusterResource,
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      SchedulingMode schedulingMode, ResourceLimits resourceLimits,
      RMContainer reservedContainer);
}