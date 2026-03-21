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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.DiagnosticsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.SchedulingRequestPBImpl;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResourceRequestException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintsUtil;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.api.resource.PlacementConstraint.TargetExpression.TargetType.NODE_ATTRIBUTE;
import static org.apache.hadoop.yarn.api.resource.PlacementConstraints.NODE_PARTITION;

/**
 * 单约束应用放置分配器，实现应用内/应用间亲和性/反亲和性的简单放置策略。
 * 支持基于单个放置约束的容器节点分配。
 */
public class SingleConstraintAppPlacementAllocator<N extends SchedulerNode>
    extends AppPlacementAllocator<N> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SingleConstraintAppPlacementAllocator.class);

  private ReentrantReadWriteLock.ReadLock readLock;
  private ReentrantReadWriteLock.WriteLock writeLock;

  private SchedulingRequest schedulingRequest = null;
  private String targetNodePartition;
  private AllocationTagsManager allocationTagsManager;
  private PlacementConstraintManager placementConstraintManager;

  /**
   * 构造函数，初始化读写锁。
   */
  public SingleConstraintAppPlacementAllocator() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  public PendingAskUpdateResult updatePendingAsk(
      Collection<ResourceRequest> requests,
      boolean recoverPreemptedRequestForAContainer) {
    if (requests != null && !requests.isEmpty()) {
      throw new SchedulerInvalidResourceRequestException(
          this.getClass().getName()
              + " not be able to handle ResourceRequest, there exists a "
              + "SchedulingRequest with the same scheduler key="
              + SchedulerRequestKey.create(requests.iterator().next())
              + ", please send ResourceRequest with a different allocationId and "
              + "priority");
    }

    // Do nothing
    return null;
  }

  /**
   * 内部更新待分配请求的核心逻辑。
   * @param newSchedulingRequest 新的调度请求
   * @param recoverContainer 是否是恢复被抢占的容器
   * @return 更新结果
   */
  private PendingAskUpdateResult internalUpdatePendingAsk(
      SchedulingRequest newSchedulingRequest, boolean recoverContainer) {
    // 恢复容器时必须已存在对应的调度请求
    if (recoverContainer && schedulingRequest == null) {
      throw new SchedulerInvalidResourceRequestException("Trying to recover a "
          + "container request=" + newSchedulingRequest.toString() + ", however"
          + "there's no existing scheduling request, this should not happen.");
    }

    if (schedulingRequest != null) {
      // 已有旧调度请求，仅允许修改分配数量，其他字段不允许修改
      // 为避免不必要的数据结构拷贝，先替换新请求中的分配数量再比较两个请求
      ResourceSizing sizing = newSchedulingRequest.getResourceSizing();
      int existingNumAllocations =
          schedulingRequest.getResourceSizing().getNumAllocations();

      // 恢复容器场景，新分配数量 = 原有数量 + 1
      int newNumAllocations;
      if (recoverContainer) {
        newNumAllocations = existingNumAllocations + 1;
      } else {
        newNumAllocations = sizing.getNumAllocations();
      }
      sizing.setNumAllocations(existingNumAllocations);

      // 比较两个请求对象是否一致
      if (!schedulingRequest.equals(newSchedulingRequest)) {
        // 回滚分配数量修改
        sizing.setNumAllocations(newNumAllocations);
        throw new SchedulerInvalidResourceRequestException(
            "Invalid updated SchedulingRequest added to scheduler, "
                + " we only allows changing numAllocations for the updated "
                + "SchedulingRequest. Old=" + schedulingRequest.toString()
                + " new=" + newSchedulingRequest.toString()
                + ", if any fields need to be updated, please cancel the "
                + "old request (by setting numAllocations to 0) and send a "
                + "SchedulingRequest with different combination of "
                + "priority/allocationId");
      } else {
        if (newNumAllocations == existingNumAllocations) {
          // 待分配数量无变化，返回空表示无更新
          return null;
        }
      }

      // 回滚分配数量修改
      sizing.setNumAllocations(newNumAllocations);

      // 基础合法性检查
      if (newNumAllocations < 0) {
        throw new SchedulerInvalidResourceRequestException(
            "numAllocation in ResourceSizing field must be >= 0, "
                + "updating schedulingRequest failed.");
      }

      PendingAskUpdateResult updateResult = new PendingAskUpdateResult(
          new PendingAsk(schedulingRequest.getResourceSizing()),
          new PendingAsk(newSchedulingRequest.getResourceSizing()),
          targetNodePartition, targetNodePartition);

      // 所有检查通过，更新分配数量
      this.schedulingRequest.getResourceSizing().setNumAllocations(
          newNumAllocations);
      LOG.info(
          "Update numAllocation from old=" + existingNumAllocations + " to new="
              + newNumAllocations);

      return updateResult;
    }

    // 处理新增的调度请求，先验证合法性再更新内部状态
    validateAndSetSchedulingRequest(newSchedulingRequest);

    return new PendingAskUpdateResult(null,
        new PendingAsk(newSchedulingRequest.getResourceSizing()), null,
        targetNodePartition);
  }

  @Override
  public PendingAskUpdateResult updatePendingAsk(
      SchedulerRequestKey schedulerRequestKey,
      SchedulingRequest newSchedulingRequest,
      boolean recoverPreemptedRequestForAContainer) {
    writeLock.lock();
    try {
      return internalUpdatePendingAsk(newSchedulingRequest,
          recoverPreemptedRequestForAContainer);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 抛出带应用和调度请求元信息的异常。
   * @param message 原始异常消息
   * @return 永远抛出异常，无返回
   */
  private String throwExceptionWithMetaInfo(String message) {
    StringBuilder sb = new StringBuilder();
    sb.append("AppId=").append(appSchedulingInfo.getApplicationId()).append(
        " Key=").append(this.schedulerRequestKey).append(". Exception message:")
        .append(message);
    throw new SchedulerInvalidResourceRequestException(sb.toString());
  }

  /**
   * 验证新调度请求合法性，并设置内部状态。
   * @param newSchedulingRequest 新调度请求
   * @throws SchedulerInvalidResourceRequestException 请求非法时抛出
   */
  private void validateAndSetSchedulingRequest(SchedulingRequest
      newSchedulingRequest)
      throws SchedulerInvalidResourceRequestException {
    // 检查资源大小信息是否存在
    if (newSchedulingRequest.getResourceSizing() == null
        || newSchedulingRequest.getResourceSizing().getResources() == null) {
      throwExceptionWithMetaInfo(
          "No ResourceSizing found in the scheduling request, please double "
              + "check");
    }

    // 检查执行类型，目前仅支持保障型(GUARANTEED)
    if (newSchedulingRequest.getExecutionType() != null
        && newSchedulingRequest.getExecutionType().getExecutionType()
        != ExecutionType.GUARANTEED) {
      throwExceptionWithMetaInfo(
          "Only GUARANTEED execution type is supported.");
    }

    // 从放置约束中提取并验证目标节点分区
    this.targetNodePartition = validateAndGetTargetNodePartition(
        newSchedulingRequest.getPlacementConstraint());
    // 深度拷贝调度请求，避免外部修改内部状态
    this.schedulingRequest = new SchedulingRequestPBImpl(
        ((SchedulingRequestPBImpl) newSchedulingRequest).getProto());


    LOG.info("Successfully added SchedulingRequest to app="
        + appSchedulingInfo.getApplicationAttemptId()
        + " placementConstraint=["
        + schedulingRequest.getPlacementConstraint()
        + "]. nodePartition=" + targetNodePartition);
  }

  /**
   * 从放置约束中提取目标节点分区，并验证约束格式合法性。
   * 目前仅处理单个约束，最多支持一个节点分区。
   * @param placementConstraint 放置约束
   * @return 验证后的目标节点分区
   */
  private String validateAndGetTargetNodePartition(
      PlacementConstraint placementConstraint) {
    String defaultNodeLabelExpression =
        appSchedulingInfo.getDefaultNodeLabelExpression();
    // 默认使用应用默认分区，无默认标签则使用无标签
    String nodePartition = defaultNodeLabelExpression == null ?
        RMNodeLabelsManager.NO_LABEL : defaultNodeLabelExpression;
    if (placementConstraint != null &&
        placementConstraint.getConstraintExpr() != null) {
      PlacementConstraint.AbstractConstraint ac =
          placementConstraint.getConstraintExpr();
      if (ac != null && ac instanceof PlacementConstraint.SingleConstraint) {
        PlacementConstraint.SingleConstraint singleConstraint =
            (PlacementConstraint.SingleConstraint) ac;
        // 遍历所有目标表达式，查找节点分区约束
        for (PlacementConstraint.TargetExpression targetExpression :
            singleConstraint.getTargetExpressions()) {
          // 处理节点分区属性
          if (targetExpression.getTargetType().equals(NODE_ATTRIBUTE) &&
              targetExpression.getTargetKey().equals(NODE_PARTITION)) {
            Set<String> values = targetExpression.getTargetValues();
            if (values == null || values.isEmpty()) {
              continue;
            }
            // 目前仅支持单个分区值
            if (values.size() > 1) {
              throwExceptionWithMetaInfo(
                  "Inside one targetExpression, we only support"
                      + " affinity to at most one node partition now");
            }
            nodePartition = values.iterator().next();
            if (nodePartition != null) {
              break;
            }
          }
        }
      }
    }
    return nodePartition;
  }

  @Override
  public Map<String, ResourceRequest> getResourceRequests() {
    return Collections.emptyMap();
  }

  @Override
  public PendingAsk getPendingAsk(String resourceName) {
    readLock.lock();
    try {
      if (resourceName.equals("*") && schedulingRequest != null) {
        return new PendingAsk(schedulingRequest.getResourceSizing());
      }
      return PendingAsk.ZERO;
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public int getOutstandingAsksCount(String resourceName) {
    readLock.lock();
    try {
      if (resourceName.equals("*") && schedulingRequest != null) {
        return schedulingRequest.getResourceSizing().getNumAllocations();
      }
      return 0;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 减少待分配容器数量，更新应用待分配资源统计。
   */
  private void decreasePendingNumAllocation() {
    // 待分配数量减1
    ResourceSizing sizing = schedulingRequest.getResourceSizing();
    sizing.setNumAllocations(sizing.getNumAllocations() - 1);

    appSchedulingInfo.decPendingResource(targetNodePartition, sizing.getResources());
  }

  @Override
  public ContainerRequest allocate(SchedulerRequestKey schedulerKey,
      NodeType type, SchedulerNode node) {
    writeLock.lock();
    try {
      // 构造容器分配请求，复制原调度请求，设置分配数量为1
      SchedulingRequest containerSchedulingRequest = new SchedulingRequestPBImpl(
          ((SchedulingRequestPBImpl) schedulingRequest).getProto());
      containerSchedulingRequest.getResourceSizing().setNumAllocations(1);

      // 减少待分配数量
      decreasePendingNumAllocation();

      return new ContainerRequest(containerSchedulingRequest);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 检查是否还有待分配资源，以及节点是否满足放置约束。
   * @param node 待检查节点
   * @param dcOpt 诊断信息收集器
   * @return 满足约束且有待分配资源返回true，否则返回false
   */
  private boolean checkCardinalityAndPending(SchedulerNode node,
      Optional<DiagnosticsCollector> dcOpt) {
    // 检查是否还有待分配资源
    if (schedulingRequest.getResourceSizing().getNumAllocations() <= 0) {
      return false;
    }

    // 调用放置约束工具检查节点是否满足约束
    try {
      return PlacementConstraintsUtil.canSatisfyConstraints(
          appSchedulingInfo.getApplicationId(), schedulingRequest, node,
          placementConstraintManager, allocationTagsManager, dcOpt);
    } catch (InvalidAllocationTagsQueryException e) {
      LOG.warn("Failed to query node cardinality:", e);
      this.incrementPlacementAttempt();
      return false;
    }
  }

  @Override
  public boolean canAllocate(NodeType type, SchedulerNode node) {
    readLock.lock();
    try {
      return checkCardinalityAndPending(node, Optional.empty());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean canDelayTo(String resourceName) {
    return true;
  }

  @Override
  public boolean precheckNode(SchedulerNode schedulerNode,
      SchedulingMode schedulingMode) {
    return precheckNode(schedulerNode, schedulingMode, Optional.empty());
  }

  @Override
  public boolean precheckNode(SchedulerNode schedulerNode,
      SchedulingMode schedulingMode,
      Optional<DiagnosticsCollector> dcOpt) {
    // 根据调度模式确定需要检查的节点分区
    String nodePartitionToLookAt;
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      nodePartitionToLookAt = schedulerNode.getPartition();
    } else{
      nodePartitionToLookAt = RMNodeLabelsManager.NO_LABEL;
    }

    readLock.lock();
    try {
      // 先检查节点分区是否匹配，再检查约束和待分配资源
      boolean rst = this.targetNodePartition.equals(nodePartitionToLookAt);
      if (!rst) {
        if (dcOpt.isPresent()) {
          dcOpt.get().collectPartitionDiagnostics(targetNodePartition,
              nodePartitionToLookAt);
        }
        return rst;
      }
      return checkCardinalityAndPending(schedulerNode, dcOpt);
    } finally {
      readLock.unlock();
    }

  }

  @Override
  public String getPrimaryRequestedNodePartition() {
    return targetNodePartition;
  }

  @Override
  public int getUniqueLocationAsks() {
    return 1;
  }

  @Override
  public void showRequests() {
    readLock.lock();
    try {
      if (schedulingRequest != null) {
        LOG.info(schedulingRequest.toString());
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public SchedulingRequest getSchedulingRequest() {