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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.DiagnosticsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResourceRequestException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 文件说明：感知数据局部性的应用容器放置分配器实现
 * 核心职责：在容器分配时，尊重应用指定的节点局部性、机架局部性偏好，按照优先级尝试分配
 */
/**
 * This is an implementation of the {@link AppPlacementAllocator} that takes
 * into account locality preferences (node, rack, any) when allocating
 * containers.
 */
public class LocalityAppPlacementAllocator <N extends SchedulerNode>
    extends AppPlacementAllocator<N> {
  private static final Logger LOG =
      LoggerFactory.getLogger(LocalityAppPlacementAllocator.class);

  // 按资源位置（节点名/机架名/ANY）存储资源请求
  private final Map<String, ResourceRequest> resourceRequestMap =
      new ConcurrentHashMap<>();
  // 应用请求的主节点分区（节点标签表达式）
  private volatile String primaryRequestedPartition =
      RMNodeLabelsManager.NO_LABEL;

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  /**
   * 构造函数，初始化读写锁用于保护资源请求映射的并发访问
   */
  public LocalityAppPlacementAllocator() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @SuppressWarnings("unchecked")
  @Override
  /**
   * 初始化分配器，调用父类初始化逻辑
   */
  public void initialize(AppSchedulingInfo appSchedulingInfo,
      SchedulerRequestKey schedulerRequestKey, RMContext rmContext) {
    super.initialize(appSchedulingInfo, schedulerRequestKey, rmContext);
  }

  /**
   * 检查两个资源请求的节点标签表达式是否发生变化
   * @param requestOne 旧请求
   * @param requestTwo 新请求
   * @return 标签是否变化
   */
  private boolean hasRequestLabelChanged(ResourceRequest requestOne,
      ResourceRequest requestTwo) {
    String requestOneLabelExp = requestOne.getNodeLabelExpression();
    String requestTwoLabelExp = requestTwo.getNodeLabelExpression();
    // First request label expression can be null and second request
    // is not null then we have to consider it as changed.
    if ((null == requestOneLabelExp) && (null != requestTwoLabelExp)) {
      return true;
    }
    // If the label is not matching between both request when
    // requestOneLabelExp is not null.
    return ((null != requestOneLabelExp) && !(requestOneLabelExp
        .equals(requestTwoLabelExp)));
  }

  /**
   * 根据ANY请求更新所有资源请求的节点标签，保持标签一致性
   * @param request 新入资源请求
   */
  private void updateNodeLabels(ResourceRequest request) {
    String resourceName = request.getResourceName();
    if (resourceName.equals(ResourceRequest.ANY)) {
      ResourceRequest previousAnyRequest =
          getResourceRequest(resourceName);

      // When there is change in ANY request label expression, we should
      // update label for all resource requests already added of same
      // priority as ANY resource request.
      if ((null == previousAnyRequest) || hasRequestLabelChanged(
          previousAnyRequest, request)) {
        for (ResourceRequest r : resourceRequestMap.values()) {
          if (!r.getResourceName().equals(ResourceRequest.ANY)) {
            r.setNodeLabelExpression(request.getNodeLabelExpression());
          }
        }
      }
    } else{
      // 非ANY请求继承ANY请求的节点标签
      ResourceRequest anyRequest = getResourceRequest(ResourceRequest.ANY);
      if (anyRequest != null) {
        request.setNodeLabelExpression(anyRequest.getNodeLabelExpression());
      }
    }
  }

  @Override
  /**
   * 更新待分配资源请求信息，处理新增/恢复的资源请求
   * @param requests 待更新的资源请求集合
   * @param recoverPreemptedRequestForAContainer 是否恢复被抢占的请求
   * @return 更新结果，包含新旧待分配请求信息
   */
  public PendingAskUpdateResult updatePendingAsk(
      Collection<ResourceRequest> requests,
      boolean recoverPreemptedRequestForAContainer) {

    this.writeLock.lock();
    try {
      PendingAskUpdateResult updateResult = null;

      // 遍历更新每个资源请求
      for (ResourceRequest request : requests) {
        String resourceName = request.getResourceName();

        // 按需更新节点标签保证一致性
        updateNodeLabels(request);

        // Increment number of containers if recovering preempted resources
        ResourceRequest lastRequest = resourceRequestMap.get(resourceName);
        if (recoverPreemptedRequestForAContainer && lastRequest != null) {
          request.setNumContainers(lastRequest.getNumContainers() + 1);
        }

        // 更新资源请求到缓存
        resourceRequestMap.put(resourceName, request);

        // ANY请求更新主分区信息
        if (resourceName.equals(ResourceRequest.ANY)) {
          String partition = request.getNodeLabelExpression() == null ?
              RMNodeLabelsManager.NO_LABEL :
              request.getNodeLabelExpression();

          this.primaryRequestedPartition = partition;

          // 更新应用请求分区集合
          appSchedulingInfo.addRequestedPartition(partition);

          // 构建更新结果对象
          PendingAsk lastPendingAsk =
              lastRequest == null ? null : new PendingAsk(
                  lastRequest.getCapability(), lastRequest.getNumContainers());
          String lastRequestedNodePartition =
              lastRequest == null ? null : lastRequest.getNodeLabelExpression();

          updateResult = new PendingAskUpdateResult(lastPendingAsk,
              new PendingAsk(request.getCapability(),
                  request.getNumContainers()), lastRequestedNodePartition,
              request.getNodeLabelExpression());
        }
      }
      return updateResult;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  /**
   * 不支持处理新版本SchedulingRequest，抛出异常
   */
  public PendingAskUpdateResult updatePendingAsk(
      SchedulerRequestKey schedulerRequestKey,
      SchedulingRequest schedulingRequest,
      boolean recoverPreemptedRequestForAContainer)
      throws SchedulerInvalidResourceRequestException {
    throw new SchedulerInvalidResourceRequestException(this.getClass().getName()
        + " not be able to handle SchedulingRequest, there exists a "
        + "ResourceRequest with the same scheduler key=" + schedulerRequestKey
        + ", please send SchedulingRequest with a different allocationId and "
        + "priority");
  }

  @Override
  /**
   * 获取所有资源请求映射
   * @return 资源位置到请求的映射
   */
  public Map<String, ResourceRequest> getResourceRequests() {
    return resourceRequestMap;
  }

  /**
   * 根据资源位置获取对应资源请求
   * @param resourceName 资源位置名称
   * @return 对应资源请求
   */
  private ResourceRequest getResourceRequest(String resourceName) {
    return resourceRequestMap.get(resourceName);
  }

  @Override
  /**
   * 获取指定位置的待分配请求信息
   * @param resourceName 资源位置名称
   * @return 待分配请求对象
   */
  public PendingAsk getPendingAsk(String resourceName) {
    readLock.lock();
    try {
      ResourceRequest request = getResourceRequest(resourceName);
      if (null == request) {
        return PendingAsk.ZERO;
      } else{
        return new PendingAsk(request.getCapability(),
            request.getNumContainers());
      }
    } finally {
      readLock.unlock();
    }

  }

  @Override
  /**
   * 获取指定位置剩余待分配容器数量
   * @param resourceName 资源位置名称
   * @return 剩余待分配容器数
   */
  public int getOutstandingAsksCount(String resourceName) {
    readLock.lock();
    try {
      ResourceRequest request = getResourceRequest(resourceName);
      if (null == request) {
        return 0;
      } else{
        return request.getNumContainers();
      }
    } finally {
      readLock.unlock();
    }

  }

  /**
   * 减少ANY位置剩余待分配容器数量，处理分配后状态变更
   * @param schedulerRequestKey 调度请求键
   * @param offSwitchRequest ANY位置资源请求
   */
  private void decrementOutstanding(SchedulerRequestKey schedulerRequestKey,
      ResourceRequest offSwitchRequest) {
    int numOffSwitchContainers = offSwitchRequest.getNumContainers() - 1;
    offSwitchRequest.setNumContainers(numOffSwitchContainers);

    // Do we have any outstanding requests?
    // If there is nothing, we need to deactivate this application
    if (numOffSwitchContainers == 0) {
      // 无剩余请求，移除调度键并检查是否需要停用应用
      appSchedulingInfo.getSchedulerKeys().remove(schedulerRequestKey);
      appSchedulingInfo.checkForDeactivation();
      resourceRequestMap.remove(ResourceRequest.ANY);
      if (resourceRequestMap.isEmpty()) {
        appSchedulingInfo.removeAppPlacement(schedulerRequestKey);
      }
    }
    // 减少应用待分配资源统计
    appSchedulingInfo.decPendingResource(
        offSwitchRequest.getNodeLabelExpression(),
        offSwitchRequest.getCapability());
  }

  /**
   * 克隆资源请求，设置容器数为1，用于恢复场景
   * @param request 原始资源请求
   * @return 克隆后的新请求
   */
  public ResourceRequest cloneResourceRequest(ResourceRequest request) {
    ResourceRequest newRequest = ResourceRequest.clone(request);
    newRequest.setNumContainers(1);
    return newRequest;
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  /**
   * 处理机架局部性容器分配，更新剩余请求状态
   * @param schedulerKey 调度请求键
   * @param node 目标节点
   * @param rackLocalRequest 机架局部性资源请求
   * @param resourceRequests 保存克隆后的请求用于恢复
   */
  private void allocateRackLocal(SchedulerRequestKey schedulerKey,
      SchedulerNode node, ResourceRequest rackLocalRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getRackName(), rackLocalRequest);

    ResourceRequest offRackRequest = resourceRequestMap.get(
        ResourceRequest.ANY);
    decrementOutstanding(schedulerKey, offRackRequest);

    // 保存克隆请求用于后续恢复
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  /**
   * 处理无局部性偏好（任意节点）容器分配，更新剩余请求状态
   * @param schedulerKey 调度请求键
   * @param offSwitchRequest ANY位置资源请求
   * @param resourceRequests 保存克隆后的请求用于恢复
   */
  private void allocateOffSwitch(SchedulerRequestKey schedulerKey,
      ResourceRequest offSwitchRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decrementOutstanding(schedulerKey, offSwitchRequest);
    // Update cloned OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(offSwitchRequest));
  }


  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  /**
   * 处理节点局部性容器分配，更新剩余请求状态
   * @param schedulerKey 调度请求键
   * @param node 目标节点
   * @param nodeLocalRequest 节点局部性资源请求
   * @param resourceRequests 保存克隆后的请求用于恢复
   */
  private void allocateNodeLocal(SchedulerRequestKey schedulerKey,
      SchedulerNode node, ResourceRequest nodeLocalRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getNodeName(), nodeLocalRequest);

    ResourceRequest rackLocalRequest = resourceRequestMap.get(
        node.getRackName());
    decResourceRequest(node.getRackName(), rackLocalRequest);

    ResourceRequest offRackRequest = resourceRequestMap.get(
        ResourceRequest.ANY);
    decrementOutstanding(schedulerKey, offRackRequest);

    // Update cloned NodeLocal, RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(nodeLocalRequest));
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  /**
   * 减少指定位置资源请求的剩余容器数，无剩余则移除请求
   * @param resourceName 资源位置名称
   * @param request 资源请求对象
   */
  private void decResourceRequest(String resourceName,
      ResourceRequest request) {
    request.setNumContainers(request.getNumContainers() - 1);
    if (request.getNumContainers() == 0) {
      resourceRequestMap.remove(resourceName);
    }
  }

  @Override
  /**
   * 检查当前是否可以分配指定局部性类型的容器
   * @param type 局部性类型（节点/机架/任意）
   * @param node 目标节点
   * @return 是否可分配
   */
  public boolean canAllocate(NodeType type, SchedulerNode node) {
    readLock.lock();
    try {
      ResourceRequest r = resourceRequestMap.get(
          ResourceRequest.ANY);
      // 检查ANY位置是否还有待分配请求
      if (r == null || r.getNumContainers() <= 0) {
        return false;
      }
      // 机架或节点局部性需要检查对应位置是否还有待分配请求
      if (type == NodeType.RACK_LOCAL || type == NodeType.NODE_LOCAL) {
        r = resourceRequestMap.get(node.getRackName());
        if (r == null || r.getNumContainers() <= 0) {
          return false;
        }
        if (type == NodeType.NODE_LOCAL) {
          r = resourceRequestMap.get(node.getNodeName());
          if (r == null || r.getNumContainers() <= 0) {
            return false;
          }
        }
      }

      return true;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  /**
   * 检查是否可以延迟分配指定位置的请求（是否允许放宽局部性）
   * @param resourceName 目标资源位置
   * @return 是否允许延迟放宽局部性
   */
  public boolean canDelayTo(String resourceName) {
    readLock.lock();
    try {
      ResourceRequest request = getResourceRequest(resourceName);
      return request == null || request.getRelaxLocality();
    } finally {
      readLock.unlock();
    }

  }


  @Override
  /**
   * 预检查节点是否符合应用请求的分区要求
   * @param schedulerNode 目标节点
   * @param schedulingMode 调度模式（是否尊重分区独占性）
   * @param dcOpt 诊断信息收集器
   * @return 是否符合分区要求
   */
  public boolean precheckNode(SchedulerNode schedulerNode,
      SchedulingMode schedulingMode,
      Optional<DiagnosticsCollector> dcOpt) {