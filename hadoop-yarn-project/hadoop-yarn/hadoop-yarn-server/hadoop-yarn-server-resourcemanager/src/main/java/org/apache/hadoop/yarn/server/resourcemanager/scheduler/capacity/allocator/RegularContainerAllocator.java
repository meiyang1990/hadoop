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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityLevel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.DiagnosticsCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAMContainerLaunchDiagnosticsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSetUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.AM_ALLOW_NON_EXCLUSIVE_ALLOCATION;

/**
 * 普通新容器分配器，考虑数据局部性、节点标签等约束，使用延迟调度机制获得更好的数据局部性
 */
public class RegularContainerAllocator extends AbstractContainerAllocator {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegularContainerAllocator.class);

  public RegularContainerAllocator(FiCaSchedulerApp application,
      ResourceCalculator rc, RMContext rmContext,
      ActivitiesManager activitiesManager) {
    super(application, rc, rmContext, activitiesManager);
  }

  /**
   * 检查队列剩余资源是否足够分配本次请求，考虑可解除预留的资源
   */
  private boolean checkHeadroom(ResourceLimits currentResourceLimits,
                                Resource required, String nodePartition) {
    // If headroom + currentReservation < required, we cannot allocate this
    // require
    Resource resourceCouldBeUnReserved =
        application.getAppAttemptResourceUsage().getReserved(nodePartition);
    if (!application.getCSLeafQueue().isReservationsContinueLooking()) {
      // If we don't allow reservation continuous looking,
      // we won't allow to unreserve before allocation.
      resourceCouldBeUnReserved = Resources.none();
    }
    return Resources.fitsIn(rc, required,
        Resources.add(currentResourceLimits.getHeadroom(), resourceCouldBeUnReserved));
  }

  /*
   * Pre-check if we can allocate a pending resource request
   * (given schedulerKey) to a given CandidateNodeSet.
   * We will consider stuffs like exclusivity, pending resource, node partition,
   * headroom, etc.
   */
  /**
   * 对候选节点集合做分配前预检查，检查排他性、剩余资源、分区等约束
   */
  private ContainerAllocation preCheckRequest(
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      SchedulingMode schedulingMode, ResourceLimits resourceLimits,
      SchedulerRequestKey schedulerKey) {
    PendingAsk offswitchPendingAsk = application.getPendingAsk(schedulerKey,
        ResourceRequest.ANY);
    SchedulerNode recordNode =
        CandidateNodeSetUtils.getSingleNode(candidates);

    if (offswitchPendingAsk.getCount() <= 0) {
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, recordNode, application, schedulerKey,
          ActivityDiagnosticConstant.REQUEST_DO_NOT_NEED_RESOURCE,
          ActivityLevel.REQUEST);
      return ContainerAllocation.PRIORITY_SKIPPED;
    }

    // Required resource
    Resource required = offswitchPendingAsk.getPerAllocationResource();

    // Do we need containers at this 'priority'?
    if (application.getOutstandingAsksCount(schedulerKey) <= 0) {
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, recordNode, application, schedulerKey,
          ActivityDiagnosticConstant.REQUEST_DO_NOT_NEED_RESOURCE,
          ActivityLevel.REQUEST);
      return ContainerAllocation.PRIORITY_SKIPPED;
    }

    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      if (application.isWaitingForAMContainer() && !rmContext.getYarnConfiguration()
          .getBoolean(AM_ALLOW_NON_EXCLUSIVE_ALLOCATION, false)) {
        LOG.debug("Skip allocating AM container to app_attempt={},"
                + " don't allow to allocate AM container in non-exclusive mode",
            application.getApplicationAttemptId());
        application.updateAppSkipNodeDiagnostics(
            "Skipping assigning to Node in Ignore Exclusivity mode. ");
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, recordNode, application, schedulerKey,
            ActivityDiagnosticConstant.
                REQUEST_SKIPPED_IN_IGNORE_EXCLUSIVITY_MODE,
            ActivityLevel.REQUEST);
        return ContainerAllocation.APP_SKIPPED;
      }
    }

    if (!application.getCSLeafQueue().isReservationsContinueLooking()) {
      if (!shouldAllocOrReserveNewContainer(schedulerKey, required)) {
        LOG.debug("doesn't need containers based on reservation algo!");
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, recordNode, application, schedulerKey,
            ActivityDiagnosticConstant.REQUEST_SKIPPED_BECAUSE_OF_RESERVATION,
            ActivityLevel.REQUEST);
        return ContainerAllocation.PRIORITY_SKIPPED;
      }
    }

    if (!checkHeadroom(resourceLimits, required, candidates.getPartition())) {
      LOG.debug("cannot allocate required resource={} because of headroom",
          required);
      ActivitiesLogger.APP.recordAppActivityWithoutAllocation(
          activitiesManager, recordNode, application, schedulerKey,
          ActivityDiagnosticConstant.QUEUE_DO_NOT_HAVE_ENOUGH_HEADROOM,
          ActivityState.REJECTED,
          ActivityLevel.REQUEST);
      return ContainerAllocation.QUEUE_SKIPPED;
    }

    // Increase missed-non-partitioned-resource-request-opportunity.
    // This is to make sure non-partitioned-resource-request will prefer
    // to be allocated to non-partitioned nodes
    int missedNonPartitionedRequestSchedulingOpportunity = 0;
    AppPlacementAllocator appPlacementAllocator =
        appInfo.getAppPlacementAllocator(schedulerKey);
    if (null == appPlacementAllocator){
      // This is possible when #pending resource decreased by a different
      // thread.
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, recordNode, application, schedulerKey,
          ActivityDiagnosticConstant.REQUEST_SKIPPED_BECAUSE_NULL_ANY_REQUEST,
          ActivityLevel.REQUEST);
      return ContainerAllocation.PRIORITY_SKIPPED;
    }
    String requestPartition =
        appPlacementAllocator.getPrimaryRequestedNodePartition();

    // Only do this when request associated with given scheduler key accepts
    // NO_LABEL under RESPECT_EXCLUSIVITY mode
    if (StringUtils.equals(RMNodeLabelsManager.NO_LABEL, requestPartition)) {
      missedNonPartitionedRequestSchedulingOpportunity =
          application.addMissedNonPartitionedRequestSchedulingOpportunity(
              schedulerKey);
    }

    if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      // Before doing allocation, we need to check scheduling opportunity to
      // make sure : non-partitioned resource request should be scheduled to
      // non-partitioned partition first.
      if (missedNonPartitionedRequestSchedulingOpportunity < rmContext
          .getScheduler().getNumClusterNodes()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip app_attempt=" + application.getApplicationAttemptId()
              + " priority=" + schedulerKey.getPriority()
              + " because missed-non-partitioned-resource-request"
              + " opportunity under required:" + " Now="
              + missedNonPartitionedRequestSchedulingOpportunity + " required="
              + rmContext.getScheduler().getNumClusterNodes());
        }
        ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
            activitiesManager, recordNode, application, schedulerKey,
            ActivityDiagnosticConstant.
                REQUEST_SKIPPED_BECAUSE_NON_PARTITIONED_PARTITION_FIRST,
            ActivityLevel.REQUEST);
        return ContainerAllocation.APP_SKIPPED;
      }
    }
    return null;
  }

  /*
   * Pre-check if we can allocate a pending resource request
   * (given schedulerKey) to a given node.
   * We will consider stuffs like placement-constraints, etc.
   */
  /**
   * 对单个节点做分配前预检查，检查分区匹配和放置约束
   */
  private ContainerAllocation preCheckForNode(FiCaSchedulerNode node,
      SchedulingMode schedulingMode, SchedulerRequestKey schedulerKey) {

    // Is the nodePartition of pending request matches the node's partition
    // If not match, jump to next priority.
    Optional<DiagnosticsCollector> dcOpt = activitiesManager == null ?
        Optional.empty() :
        activitiesManager.getOptionalDiagnosticsCollector();
    if (!appInfo.precheckNode(schedulerKey, node, schedulingMode, dcOpt)) {
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.
              NODE_DO_NOT_MATCH_PARTITION_OR_PLACEMENT_CONSTRAINTS
              + ActivitiesManager.getDiagnostics(dcOpt),
          ActivityLevel.NODE);
      return ContainerAllocation.PRIORITY_SKIPPED;
    }

    return null;
  }

  /**
   * 检查节点是否被应用拉入黑名单，黑名单节点跳过分配
   */
  private ContainerAllocation checkIfNodeBlackListed(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey) {
    if (SchedulerAppUtils.isPlaceBlacklisted(application, node, LOG)) {
      application.updateAppSkipNodeDiagnostics(
          CSAMContainerLaunchDiagnosticsConstants.SKIP_AM_ALLOCATION_IN_BLACK_LISTED_NODE);
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.NODE_IS_BLACKLISTED,
          ActivityLevel.NODE);
      return ContainerAllocation.APP_SKIPPED;
    }

    return null;
  }

  /**
   * 尝试在指定节点上分配容器，完成前置检查后尝试分配
   */
  ContainerAllocation tryAllocateOnNode(Resource clusterResource,
      FiCaSchedulerNode node, SchedulingMode schedulingMode,
      ResourceLimits resourceLimits, SchedulerRequestKey schedulerKey,
      RMContainer reservedContainer) {
    ContainerAllocation result;

    // 黑名单检查
    result = checkIfNodeBlackListed(node, schedulerKey);
    if (null != result) {
      return result;
    }

    // 增加一次调度机会计数，用于延迟调度判断
    // TODO, we may need to revisit here to see if we should add scheduling
    // opportunity here
    application.addSchedulingOpportunity(schedulerKey);

    // 在节点上按局部性优先级尝试分配容器
    result =
        assignContainersOnNode(clusterResource, node, schedulerKey,
            reservedContainer, schedulingMode, resourceLimits);
    
    if (null == reservedContainer) {
      if (result.getAllocationState() == AllocationState.PRIORITY_SKIPPED) {
        // 跳过的节点不计入调度机会，避免错误触发延迟
        application.subtractSchedulingOpportunity(schedulerKey);
      }
    }
    
    return result;
  }
  
  /**
   * 计算局部性等待因子，用于动态计算延迟调度阈值
   */
  public float getLocalityWaitFactor(int uniqAsks, int clusterNodes) {
    // 估算所需唯一资源数量（主机 + 机架）
    int requiredResources = Math.max(uniqAsks - 1, 0);
    
    // 等待因子最大为1，不超过集群节点总数
    // i.e. no point skipping more than clustersize opportunities
    return Math.min(((float)requiredResources / clusterNodes), 1.0f);
  }
  
  /**
   * 获取实际节点局部性延迟阈值，不超过集群节点总数
   */
  private int getActualNodeLocalityDelay() {
    return Math.min(rmContext.getScheduler().getNumClusterNodes(), application
        .getCSLeafQueue().getNodeLocalityDelay());
  }

  /**
   * 获取实际机架局部性延迟阈值，不超过集群节点总数
   */
  private int getActualRackLocalityDelay() {
    return Math.min(rmContext.getScheduler().getNumClusterNodes(),
        application.getCSLeafQueue().getNodeLocalityDelay()
        + application.getCSLeafQueue().getRackLocalityAdditionalDelay());
  }

  /**
   * 根据延迟调度规则，判断是否可以在当前局部性级别分配容器
   */
  private boolean canAssign(SchedulerRequestKey schedulerKey,
      FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer) {

    // 离开关局部性（跨节点/跨机架）处理
    if (type == NodeType.OFF_SWITCH) {
      // 已有预留容器，直接允许分配
      if (reservedContainer != null) {
        return true;
      }
      // 集群无节点，拒绝分配
      if (rmContext.getScheduler().getNumClusterNodes() == 0) {
        return false;
      }

      int uniqLocationAsks = 0;
      AppPlacementAllocator appPlacementAllocator =
          application.getAppPlacementAllocator(schedulerKey);
      if (appPlacementAllocator != null) {
        uniqLocationAsks = appPlacementAllocator.getUniqueLocationAsks();
      }
      // 如果只有ANY请求，不需要延迟调度，直接分配
      if (uniqLocationAsks == 1) {
        return true;
      }

      // 获取已错过的调度机会次数
      long missedOpportunities =
          application.getSchedulingOpportunities(schedulerKey);

      // 如果启用了机架额外延迟配置
      if (application.getCSLeafQueue().getRackLocalityAdditionalDelay() > -1) {
        return missedOpportunities > getActualRackLocalityDelay();
      } else {
        // 动态计算延迟阈值，基于待分配容器数和局部性等待因子
        long requiredContainers =
            application.getOutstandingAsksCount(schedulerKey);
        float localityWaitFactor = getLocalityWaitFactor(uniqLocationAsks,
            rmContext.getScheduler().getNumClusterNodes());
        // 超过阈值才允许离开关分配
        return (Math.min(rmContext.getScheduler().getNumClusterNodes(),
            (int)(requiredContainers * localityWaitFactor)) < missedOpportunities);
      }
    }

    // 检查当前机架是否有未满足的请求
    if (application.getOutstandingAsksCount(schedulerKey,
        node.getRackName()) <= 0) {
      return false;
    }

    // 机架局部性处理
    if (type == NodeType.RACK_LOCAL) {
      // 延迟判断：错过次数超过节点局部性阈值才允许机架局部性分配
      long missedOpportunities =
          application.getSchedulingOpportunities(schedulerKey);
      return getActualNodeLocalityDelay() < missedOpportunities;