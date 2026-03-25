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

import org.apache.commons.collections4.IteratorUtils;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.DiagnosticsCollector;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ApplicationSchedulingConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * YARN应用容器放置分配器抽象基类，核心功能包括：
 * 1) 跟踪待分配资源请求，处理新请求加入、容器分配完成等事件
 * 2) 决定候选节点集合中节点的分配顺序，实现不同的放置策略
 * </p>
 *
 * <p>
 * 每个具有相同调度键的资源请求集合对应一个该类实例，不同实例可根据请求特性
 * 实现不同的节点排序和放置策略，支持灵活扩展。
 * </p>
 */
public abstract class AppPlacementAllocator<N extends SchedulerNode> {
  // 应用调度信息，保存应用整体调度上下文
  protected AppSchedulingInfo appSchedulingInfo;
  // 当前分配器对应的调度请求键，标识一组相同属性的资源请求
  protected SchedulerRequestKey schedulerRequestKey;
  // RM全局上下文对象
  protected RMContext rmContext;
  // 放置尝试次数计数器，用于统计放置重试次数
  private AtomicInteger placementAttempt = new AtomicInteger(0);
  // 多节点排序管理器，支持自定义节点排序策略
  private MultiNodeSortingManager<N> multiNodeSortingManager = null;
  // 多节点排序策略名称，从应用配置中读取
  private String multiNodeSortPolicyName;

  private static final Logger LOG =
      LoggerFactory.getLogger(AppPlacementAllocator.class);

  /**
   * 根据请求需求和节点可用性获取偏好节点迭代器
   * @param candidateNodeSet 输入候选节点集合
   * @return 偏好节点迭代器
   */
  public Iterator<N> getPreferredNodeIterator(
      CandidateNodeSet<N> candidateNodeSet) {
    // 当前仅处理候选集合中只有单个节点的情况
    // TODO 支持候选集合中包含多个节点的场景

    // 从候选集合中获取单个节点
    N singleNode = CandidateNodeSetUtils.getSingleNode(candidateNodeSet);
    if (singleNode != null) {
      // 返回仅包含该节点的单元素迭代器
      return IteratorUtils.singletonIterator(singleNode);
    }

    // 当启用多节点放置查找时，singleNode会为null，此时使用多节点排序策略
    return multiNodeSortingManager.getMultiNodeSortIterator(
        candidateNodeSet.getAllNodes().values(),
        candidateNodeSet.getPartition(),
        multiNodeSortPolicyName);
  }

  /**
   * 用新的资源请求替换现有的待分配请求
   *
   * @param requests 新的待分配请求集合
   * @param recoverPreemptedRequestForAContainer 是否为被抢占容器恢复资源请求
   * @return 待分配资源总量是否发生变化
   */
  public abstract PendingAskUpdateResult updatePendingAsk(
      Collection<ResourceRequest> requests,
      boolean recoverPreemptedRequestForAContainer);

  /**
   * 用新的调度请求替换现有的待分配请求
   *
   * @param schedulerRequestKey                  调度请求键
   * @param schedulingRequest                    新的待分配调度请求
   * @param recoverPreemptedRequestForAContainer 是否为被抢占容器恢复资源请求
   * @return 待分配资源总量是否发生变化
   */
  public abstract PendingAskUpdateResult updatePendingAsk(
      SchedulerRequestKey schedulerRequestKey,
      SchedulingRequest schedulingRequest,
      boolean recoverPreemptedRequestForAContainer);

  /**
   * 根据调度请求键获取待分配资源请求映射
   * @return 资源名称到资源请求的映射
   */
  public abstract Map<String, ResourceRequest> getResourceRequests();

  /**
   * 根据资源名称获取待分配请求，如果没有对应请求返回ZERO
   *
   * @param resourceName 资源名称
   * @return 待分配请求对象
   */
  public abstract PendingAsk getPendingAsk(String resourceName);

  /**
   * 根据资源名称获取待分配容器数量，如果没有对应请求返回0
   *
   * @param resourceName 资源名称
   * @return 待分配容器数量
   */
  public abstract int getOutstandingAsksCount(String resourceName);

  /**
   * 通知分配器容器已完成分配，更新待分配请求
   * @param schedulerKey 该资源请求对应的调度请求键
   * @param type 分配类型（节点位置类型）
   * @param node 容器分配到的节点
   * @return 关联了资源请求的容器请求对象，供调度器后续恢复请求使用
   */
  public abstract ContainerRequest allocate(SchedulerRequestKey schedulerKey,
      NodeType type, SchedulerNode node);

  /**
   * 检查指定节点类型和节点是否仍有待分配需求
   * @param type 位置类型
   * @param node 待检查节点
   * @return 是否存在待分配需求
   */
  public abstract boolean canAllocate(NodeType type, SchedulerNode node);

  /**
   * 是否可以延迟分配以等待更好的位置
   * TODO: 该方法应移出此类，归属于特定延迟调度策略实现
   * 详见YARN-7457
   *
   * @param resourceName 资源名称
   * @return 是否可以延迟
   */
  public abstract boolean canDelayTo(String resourceName);

  /**
   * 检查该分配器是否接受在指定节点上分配资源
   *
   * @param schedulerNode 待检查节点
   * @param schedulingMode 调度模式
   * @param dcOpt 可选诊断信息收集器
   * @return 是否接受
   */
  public abstract boolean precheckNode(SchedulerNode schedulerNode,
      SchedulingMode schedulingMode,
      Optional<DiagnosticsCollector> dcOpt);

  public abstract boolean precheckNode(SchedulerNode schedulerNode,
      SchedulingMode schedulingMode);

  /**
   * 由于一个请求可以接受多个节点分区，该方法返回用于待分配资源/空闲计算
   * 的主节点分区
   *
   * @return 主请求节点分区
   */
  public abstract String getPrimaryRequestedNodePartition();

  /**
   * @return 待分配数量大于0的唯一位置请求数量（如rack1、host1等）
   *
   * TODO: 该方法应移出此类，归属于特定延迟调度策略实现
   * 详见YARN-7457
   */
  public abstract int getUniqueLocationAsks();

  /**
   * 将请求信息打印到调试日志，方便问题排查
   */
  public abstract void showRequests();

  /**
   * 初始化分配器，由工厂自动调用
   *
   * @param appSchedulingInfo 应用调度信息
   * @param schedulerRequestKey 调度请求键
   * @param rmContext RM全局上下文
   */
  public void initialize(AppSchedulingInfo appSchedulingInfo,
      SchedulerRequestKey schedulerRequestKey, RMContext rmContext) {
    this.appSchedulingInfo = appSchedulingInfo;
    this.rmContext = rmContext;
    this.schedulerRequestKey = schedulerRequestKey;
    // 从应用调度环境中读取多节点排序策略配置
    multiNodeSortPolicyName = appSchedulingInfo
        .getApplicationSchedulingEnvs().get(
        ApplicationSchedulingConfig.ENV_MULTI_NODE_SORTING_POLICY_CLASS);
    // 从RM上下文获取多节点排序管理器实例
    multiNodeSortingManager = (MultiNodeSortingManager<N>) rmContext
        .getMultiNodeSortingManager();
    // 调试日志：打印当前应用使用的多节点排序策略
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "nodeLookupPolicy used for " + appSchedulingInfo.getApplicationId()
          + " is " + ((multiNodeSortPolicyName != null)
          ? multiNodeSortPolicyName : ""));
    }
  }

  /**
   * 获取待处理调度请求
   * @return 调度请求对象
   */
  public abstract SchedulingRequest getSchedulingRequest();

  /**
   * 获取当前放置尝试次数
   * @return 放置尝试次数
   */
  public int getPlacementAttempt() {
    return placementAttempt.get();
  }

  /**
   * 放置尝试次数自增
   */
  public void incrementPlacementAttempt() {
    placementAttempt.getAndIncrement();
  }
}