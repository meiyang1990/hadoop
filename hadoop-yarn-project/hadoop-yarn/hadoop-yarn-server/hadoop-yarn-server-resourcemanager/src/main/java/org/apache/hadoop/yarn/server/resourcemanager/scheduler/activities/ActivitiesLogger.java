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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.function.Supplier;

/**
 * 文件：YARN资源调度器调度活动日志工具类
 * 核心职责：提供统一的工具方法，记录调度过程中不同层级（队列/应用/请求/节点）的调度活动，
 * 用于问题排查和调度过程审计
 */
public class ActivitiesLogger {
  private static final Logger LOG =
      LoggerFactory.getLogger(ActivitiesLogger.class);

  /**
   * 应用层面调度活动记录工具类，提供各类应用调度事件的记录方法
   */
  public static class APP {

    /**
     * 记录应用未分配容器的跳过调度事件
     * @param activitiesManager 调度活动管理器
     * @param node 当前调度节点
     * @param application 当前尝试调度的应用
     * @param requestKey 调度请求key
     * @param diagnostic 诊断信息
     * @param level 记录层级
     */
    public static void recordSkippedAppActivityWithoutAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node,
        SchedulerApplicationAttempt application,
        SchedulerRequestKey requestKey,
        String diagnostic, ActivityLevel level) {
      recordAppActivityWithoutAllocation(activitiesManager, node, application,
          requestKey, diagnostic, ActivityState.SKIPPED, level);
    }

    /**
     * 记录应用因为队列容量或用户限制被拒绝调度的事件
     * @param activitiesManager 调度活动管理器
     * @param node 当前调度节点
     * @param application 当前尝试调度的应用
     * @param priority 应用优先级
     * @param diagnostic 诊断信息
     */
    public static void recordRejectedAppActivityFromLeafQueue(
        ActivitiesManager activitiesManager, SchedulerNode node,
        SchedulerApplicationAttempt application, Priority priority,
        String diagnostic) {
      if (activitiesManager == null) {
        return;
      }
      // 获取要记录的节点ID
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      // 判断是否需要记录该节点的活动
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        recordActivity(activitiesManager, nodeId, application.getQueueName(),
            application.getApplicationId().toString(), priority,
            ActivityState.REJECTED, diagnostic, ActivityLevel.APP);
      }
      // 结束本次应用分配记录，标记为拒绝
      finishSkippedAppAllocationRecording(activitiesManager,
          application.getApplicationId(), ActivityState.REJECTED, diagnostic);
    }

    /**
     * 记录未分配容器的应用调度活动
     * @param activitiesManager 调度活动管理器
     * @param node 当前调度节点
     * @param application 当前尝试调度的应用
     * @param schedulerKey 调度请求key
     * @param diagnostic 诊断信息
     * @param appState 活动状态
     * @param level 记录层级
     */
    public static void recordAppActivityWithoutAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node,
        SchedulerApplicationAttempt application,
        SchedulerRequestKey schedulerKey,
        String diagnostic, ActivityState appState, ActivityLevel level) {
      if (activitiesManager == null) {
        return;
      }
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        String requestName = null;
        Integer priority = null;
        Long allocationRequestId = null;
        // 节点/请求层级需要请求信息，提取请求相关信息
        if (level == ActivityLevel.NODE || level == ActivityLevel.REQUEST) {
          if (schedulerKey == null) {
            LOG.warn("Request key should not be null at " + level + " level.");
            return;
          }
          priority = getPriority(schedulerKey);
          allocationRequestId = schedulerKey.getAllocationRequestId();
          requestName = getRequestName(priority, allocationRequestId);
        }
        // 根据不同记录层级分发到对应方法
        switch (level) {
        case NODE:
          recordSchedulerActivityAtNodeLevel(activitiesManager, application,
              requestName, priority, allocationRequestId, null, nodeId,
              appState, diagnostic);
          break;
        case REQUEST:
          recordSchedulerActivityAtRequestLevel(activitiesManager, application,
              requestName, priority, allocationRequestId, nodeId, appState,
              diagnostic);
          break;
        case APP:
          recordSchedulerActivityAtAppLevel(activitiesManager, application,
              nodeId, appState, diagnostic);
          break;
        default:
          LOG.warn("Doesn't handle app activities at " + level + " level.");
          break;
        }
      }
      // 将活动添加到应用自身的分配记录中，未分配所以容器ID为null
      if (activitiesManager.shouldRecordThisApp(
          application.getApplicationId())) {
        activitiesManager.addSchedulingActivityForApp(
            application.getApplicationId(), null,
            getPriority(schedulerKey), appState,
            diagnostic, level, nodeId,
            schedulerKey == null ?
                null : schedulerKey.getAllocationRequestId());
      }
    }

    /**
     * 记录已分配容器的应用调度活动
     * @param activitiesManager 调度活动管理器
     * @param node 当前调度节点
     * @param application 当前尝试调度的应用
     * @param updatedContainer 已分配的容器
     * @param activityState 活动状态
     */
    public static void recordAppActivityWithAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node,
        SchedulerApplicationAttempt application, RMContainer updatedContainer,
        ActivityState activityState) {
      if (activitiesManager == null) {
        return;
      }
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      // 如果节点未提供，从容器信息中获取
      if (nodeId == null || nodeId == ActivitiesManager.EMPTY_NODE_ID) {
        nodeId = updatedContainer.getNodeId();
      }
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        // 提取容器请求信息
        Integer containerPriority =
            updatedContainer.getContainer().getPriority().getPriority();
        Long allocationRequestId =
            updatedContainer.getContainer().getAllocationRequestId();
        String requestName =
            getRequestName(containerPriority, allocationRequestId);
        // 记录节点、请求、应用三个层级的活动到调度活动列表
        recordSchedulerActivityAtNodeLevel(activitiesManager, application,
            requestName, containerPriority, allocationRequestId,
            updatedContainer.getContainer().toString(), nodeId, activityState,
            ActivityDiagnosticConstant.EMPTY);
      }
      // 将活动添加到应用自身的分配记录中
      if (activitiesManager.shouldRecordThisApp(
          application.getApplicationId())) {
        activitiesManager.addSchedulingActivityForApp(
            application.getApplicationId(),
            updatedContainer.getContainerId(),
            updatedContainer.getContainer().getPriority().getPriority(),
            activityState, ActivityDiagnosticConstant.EMPTY,
            ActivityLevel.NODE, nodeId,
            updatedContainer.getContainer().getAllocationRequestId());
      }
    }

    @SuppressWarnings("parameternumber")
    /**
     * 在节点层级记录调度活动，同时额外记录请求和应用层级活动
     * @param activitiesManager 调度活动管理器
     * @param app 当前应用
     * @param requestName 请求名称
     * @param priority 请求优先级
     * @param allocationRequestId 分配请求ID
     * @param containerId 容器ID
     * @param nodeId 节点ID
     * @param state 活动状态
     * @param diagnostic 诊断信息
     */
    private static void recordSchedulerActivityAtNodeLevel(
        ActivitiesManager activitiesManager, SchedulerApplicationAttempt app,
        String requestName, Integer priority, Long allocationRequestId,
        String containerId, NodeId nodeId, ActivityState state,
        String diagnostic) {
      activitiesManager
          .addSchedulingActivityForNode(nodeId, requestName, containerId, null,
              state, diagnostic, ActivityLevel.NODE, null);
      // 额外记录请求层级活动
      recordSchedulerActivityAtRequestLevel(activitiesManager, app, requestName,
          priority, allocationRequestId, nodeId, state,
          ActivityDiagnosticConstant.EMPTY);
    }

    @SuppressWarnings("parameternumber")
    /**
     * 在请求层级记录调度活动，同时额外记录应用层级活动
     * @param activitiesManager 调度活动管理器
     * @param app 当前应用
     * @param requestName 请求名称
     * @param priority 请求优先级
     * @param allocationRequestId 分配请求ID
     * @param nodeId 节点ID
     * @param state 活动状态
     * @param diagnostic 诊断信息
     */
    private static void recordSchedulerActivityAtRequestLevel(
        ActivitiesManager activitiesManager, SchedulerApplicationAttempt app,
        String requestName, Integer priority, Long allocationRequestId,
        NodeId nodeId, ActivityState state, String diagnostic) {
      activitiesManager.addSchedulingActivityForNode(nodeId,
          app.getApplicationId().toString(), requestName, priority,
          state, diagnostic, ActivityLevel.REQUEST,
          allocationRequestId);
      // 额外记录应用层级活动
      recordSchedulerActivityAtAppLevel(activitiesManager, app, nodeId, state,
          ActivityDiagnosticConstant.EMPTY);
    }

    /**
     * 在应用层级记录调度活动
     * @param activitiesManager 调度活动管理器
     * @param app 当前应用
     * @param nodeId 节点ID
     * @param state 活动状态
     * @param diagnostic 诊断信息
     */
    private static void recordSchedulerActivityAtAppLevel(
        ActivitiesManager activitiesManager, SchedulerApplicationAttempt app,
        NodeId nodeId, ActivityState state, String diagnostic) {
      activitiesManager.addSchedulingActivityForNode(nodeId, app.getQueueName(),
          app.getApplicationId().toString(), app.getPriority().getPriority(),
          state, diagnostic, ActivityLevel.APP, null);
    }

    /**
     * 开始记录一次应用分配过程，在调度器开始处理该应用时调用
     * @param activitiesManager 调度活动管理器
     * @param node 当前调度节点
     * @param currentTime 当前时间
     * @param application 当前应用
     */
    public static void startAppAllocationRecording(
        ActivitiesManager activitiesManager, FiCaSchedulerNode node,
        long currentTime,
        SchedulerApplicationAttempt application) {
      if (activitiesManager == null) {
        return;
      }
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      activitiesManager
          .startAppAllocationRecording(nodeId, currentTime,
              application);
    }

    /**
     * 结束记录一次应用分配过程，已成功分配容器时调用
     * @param activitiesManager 调度活动管理器
     * @param applicationId 应用ID
     * @param containerId 分配的容器ID
     * @param containerState 容器状态
     * @param diagnostic 诊断信息
     */
    public static void finishAllocatedAppAllocationRecording(
        ActivitiesManager activitiesManager, ApplicationId applicationId,
        ContainerId containerId, ActivityState containerState,
        String diagnostic) {
      if (activitiesManager == null) {
        return;
      }

      if (activitiesManager.shouldRecordThisApp(applicationId)) {
        activitiesManager.finishAppAllocationRecording(applicationId,
            containerId, containerState, diagnostic);
      }
    }

    /**
     * 结束记录一次应用分配过程，未分配容器时调用
     * @param activitiesManager 调度活动管理器
     * @param applicationId 应用ID
     * @param containerState 容器状态
     * @param diagnostic 诊断信息
     */
    public static void finishSkippedAppAllocationRecording(
        ActivitiesManager activitiesManager, ApplicationId applicationId,
        ActivityState containerState, String diagnostic) {
      finishAllocatedAppAllocationRecording(activitiesManager, applicationId,
          null, containerState, diagnostic);
    }
  }

  /**
   * 队列层面调度活动记录工具类
   */
  public static class QUEUE {
    /**
     * 记录队列调度活动
     * @param activitiesManager 调度活动管理器
     * @param node 当前调度节点
     * @param parentQueueName 父队列名称
     * @param queueName 当前队列名称
     * @param state 活动状态
     * @param diagnostic 诊断信息
     */
    public static void recordQueueActivity(ActivitiesManager activitiesManager,
        SchedulerNode node, String parentQueueName, String queueName,
        ActivityState state, String diagnostic) {
      recordQueueActivity(activitiesManager, node, parentQueueName, queueName,
          state, () -> diagnostic);
    }

    /**
     * 记录队列调度活动，使用Supplier延迟生成诊断信息
     * @param activitiesManager 调度活动管理器
     * @param node 当前调度节点
     * @param parentQueueName 父队列名称
     * @param queueName 当前队列名称
     * @param state 活动状态
     * @param diagnosticSupplier 诊断信息供应者
     */
    public static void recordQueueActivity(ActivitiesManager activitiesManager,
        SchedulerNode node, String parentQueueName, String queueName,
        ActivityState state, Supplier<String> diagnosticSupplier) {
      if (activitiesManager == null) {
        return;
      }
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        recordActivity(activitiesManager, nodeId, parentQueueName, queueName,
            null, state, diagnosticSupplier.get(), ActivityLevel.QUEUE);
      }
    }
  }

  /**
   * 节点更新层面调度活动记录工具类，处理节点心跳分配过程的记录
   */
  public static class NODE {

    /**
     * 结束节点分配记录，本次分配未分配/预留任何容器时调用
     * @param activitiesManager 调度活动管理器
     * @param node 当前节点
     */
    public static void finishSkippedNodeAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node) {
      finishAllocatedNodeAllocation(activitiesManager, node, null,
          AllocationState.SKIPPED);
    }

    /**
     * 结束节点分配记录，本次分配已分配/预留容器时调用
     * @param activitiesManager 调度活动管理器
     * @param node 当前节点
     * @param containerId 分配的容器ID
     * @param containerState 分配状态
     */
    public static void finishAllocatedNodeAllocation(
        ActivitiesManager activitiesManager, SchedulerNode node,
        ContainerId containerId, AllocationState containerState) {
      NodeId nodeId = getRecordingNodeId(activitiesManager, node);
      if (nodeId == null) {
        return;
      }
      if (activitiesManager.shouldRecordThisNode(nodeId)) {
        activitiesManager.updateAllocationFinalState(nodeId,
            containerId, containerState);
      }
    }

    /**
     * 结束节点心跳更新记录，在节点心跳处理完成后调用
     * @param activitiesManager 调度活动管理器
     * @param nodeID 节点ID
     * @param partition 节点分区
     */
    public static void finishNodeUpdateRecording(
        ActivitiesManager activitiesManager, NodeId nodeID, String partition) {
      if (activitiesManager == null) {
        return;
      }
      activitiesManager.finishNodeUpdateRecording(nodeID, partition);
    }

    /**
     * 开始节点心跳更新记录，在节点心跳处理开始时调用
     * @param activitiesManager 调度活动管理器
     * @param nodeID 节点ID
     */
    public static void startNodeUpdateRecording(
        ActivitiesManager activitiesManager, NodeId nodeID) {
      if (activitiesManager == null) {
        return;
      }
      activitiesManager.startNodeUpdateRecording(nodeID);
    }
  }

  /**
   * 通用活动记录方法，将队列/应用/容器活动添加到对应节点分配记录中
   * @param activitiesManager 调度活动管理器
   * @param nodeId 节点ID
   * @param parentName 父层级名称
   * @param childName 当前层级名称
   * @param priority 优先级
   * @param state 活动状态