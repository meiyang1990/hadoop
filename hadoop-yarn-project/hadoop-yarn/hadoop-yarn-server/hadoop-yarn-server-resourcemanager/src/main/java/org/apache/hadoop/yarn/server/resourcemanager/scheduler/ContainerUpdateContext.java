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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer
    .RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 容器更新上下文类，封装应用所有待处理的容器资源增减请求。
 * 用于跟踪和管理容器动态资源调整过程中的待处理状态。
 */
public class ContainerUpdateContext {

  /** 未定义容器ID，用于占位场景 */
  public static final ContainerId UNDEFINED =
      ContainerId.newContainerId(ApplicationAttemptId.newInstance(
              ApplicationId.newInstance(-1, -1), -1), -1);
  protected static final RecordFactory RECORD_FACTORY =
      RecordFactoryProvider.getRecordFactory(null);

  // 按调度请求键 -> 资源 -> 节点 -> 容器ID，跟踪待处理的资源扩容/升级容器
  private final Map<SchedulerRequestKey, Map<Resource,
      Map<NodeId, Set<ContainerId>>>> outstandingIncreases = new HashMap<>();

  /** 待处理的资源缩减请求，key为容器ID，value为目标资源 */
  private final Map<ContainerId, Resource> outstandingDecreases =
      new HashMap<>();
  private final AppSchedulingInfo appSchedulingInfo;

  /**
   * 构造函数，关联对应应用的调度信息。
   * @param appSchedulingInfo 应用调度信息
   */
  ContainerUpdateContext(AppSchedulingInfo appSchedulingInfo) {
    this.appSchedulingInfo = appSchedulingInfo;
  }

  /**
   * 检查并将容器加入待处理资源缩减列表。
   * @param updateReq 容器更新请求
   * @param schedulerNode 调度节点信息
   * @param container 目标容器
   * @return 添加成功返回true，已存在则返回false
   */
  public synchronized boolean checkAndAddToOutstandingDecreases(
      UpdateContainerRequest updateReq, SchedulerNode schedulerNode,
      Container container) {
    if (outstandingDecreases.containsKey(container.getId())) {
      return false;
    }
    if (ContainerUpdateType.DECREASE_RESOURCE ==
        updateReq.getContainerUpdateType()) {
      // 创建更新请求对应的调度请求键
      SchedulerRequestKey updateKey = new SchedulerRequestKey
          (container.getPriority(),
              container.getAllocationRequestId(), container.getId());
      // 取消之前未完成的请求
      cancelPreviousRequest(schedulerNode, updateKey);
      outstandingDecreases.put(container.getId(), updateReq.getCapability());
    } else {
      outstandingDecreases.put(container.getId(), container.getResource());
    }
    return true;
  }

  /**
   * 检查并将容器加入待处理资源扩容/升级列表。
   * @param rmContainer RM容器对象
   * @param schedulerNode 调度节点信息
   * @param updateRequest 容器更新请求
   * @return 添加成功返回true，失败返回false
   */
  public synchronized boolean checkAndAddToOutstandingIncreases(
      RMContainer rmContainer, SchedulerNode schedulerNode,
      UpdateContainerRequest updateRequest) {
    Container container = rmContainer.getContainer();
    // 根据更新请求创建调度请求键
    SchedulerRequestKey schedulerKey =
        SchedulerRequestKey.create(updateRequest,
            rmContainer.getAllocatedSchedulerKey());
    Map<Resource, Map<NodeId, Set<ContainerId>>> resourceMap =
        outstandingIncreases.get(schedulerKey);
    // 当前调度键无待处理请求，新建存储结构
    if (resourceMap == null) {
      resourceMap = new HashMap<>();
      outstandingIncreases.put(schedulerKey, resourceMap);
    } else {
      // 已经存在请求，如果是扩容则取消旧请求，否则添加失败
      if (ContainerUpdateType.INCREASE_RESOURCE ==
          updateRequest.getContainerUpdateType()) {
        cancelPreviousRequest(schedulerNode, schedulerKey);
      } else {
        return false;
      }
    }
    // 计算需要增加的资源量
    Resource resToIncrease = getResourceToIncrease(updateRequest, rmContainer);
    Map<NodeId, Set<ContainerId>> locationMap =
        resourceMap.get(resToIncrease);
    if (locationMap == null) {
      locationMap = new HashMap<>();
      resourceMap.put(resToIncrease, locationMap);
    }
    Set<ContainerId> containerIds = locationMap.get(container.getNodeId());
    if (containerIds == null) {
      containerIds = new HashSet<>();
      locationMap.put(container.getNodeId(), containerIds);
    }
    // 该容器同时存在待缩减请求，无法扩容
    if (outstandingDecreases.containsKey(container.getId())) {
      return false;
    }

    containerIds.add(container.getId());
    // 如果需要增加资源，向应用调度信息添加资源请求
    if (!Resources.isNone(resToIncrease)) {
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> updateResReqs =
          new HashMap<>();
      Map<String, ResourceRequest> resMap =
          createResourceRequests(rmContainer, schedulerNode,
              schedulerKey, resToIncrease);
      updateResReqs.put(schedulerKey, resMap);
      appSchedulingInfo.updateResourceRequests(updateResReqs, false);
    }
    return true;
  }

  /**
   * 取消之前未完成的容器更新请求，释放已申请的pending资源。
   * @param schedulerNode 调度节点
   * @param schedulerKey 调度请求键
   */
  private void cancelPreviousRequest(SchedulerNode schedulerNode,
      SchedulerRequestKey schedulerKey) {
    AppPlacementAllocator<SchedulerNode> appPlacementAllocator =
        appSchedulingInfo.getAppPlacementAllocator(schedulerKey);
    if (appPlacementAllocator != null) {
      PendingAsk pendingAsk = appPlacementAllocator.getPendingAsk(
          ResourceRequest.ANY);
      // 如果存在未分配的pending请求，通过分配虚拟容器来消耗掉pending计数
      if (pendingAsk != null && pendingAsk.getCount() > 0) {
        Container container = Container.newInstance(UNDEFINED,
            schedulerNode.getNodeID(), "host:port",
            pendingAsk.getPerAllocationResource(),
            schedulerKey.getPriority(), null);
        appSchedulingInfo.allocate(NodeType.OFF_SWITCH, schedulerNode,
            schedulerKey,
            new RMContainerImpl(container, schedulerKey,
                appSchedulingInfo.getApplicationAttemptId(),
                schedulerNode.getNodeID(), appSchedulingInfo.getUser(),
                appSchedulingInfo.getRMContext(),
                appPlacementAllocator.getPrimaryRequestedNodePartition()));
      }
    }
  }

  /**
   * 创建容器扩容所需的资源请求，包含本机、机架、任意位置三个层级。
   * @param rmContainer RM容器对象
   * @param schedulerNode 调度节点
   * @param schedulerKey 调度请求键
   * @param resToIncrease 需要增加的资源量
   * @return 按位置组织的资源请求映射
   */
  private Map<String, ResourceRequest> createResourceRequests(
      RMContainer rmContainer, SchedulerNode schedulerNode,
      SchedulerRequestKey schedulerKey, Resource resToIncrease) {
    Map<String, ResourceRequest> resMap = new HashMap<>();
    // 添加节点本地资源请求
    resMap.put(rmContainer.getContainer().getNodeId().getHost(),
        createResourceReqForIncrease(schedulerKey, resToIncrease,
            RECORD_FACTORY.newRecordInstance(ResourceRequest.class),
            rmContainer, rmContainer.getContainer().getNodeId().getHost()));
    // 添加机架资源请求
    resMap.put(schedulerNode.getRackName(),
        createResourceReqForIncrease(schedulerKey, resToIncrease,
            RECORD_FACTORY.newRecordInstance(ResourceRequest.class),
            rmContainer, schedulerNode.getRackName()));
    // 添加任意位置资源请求
    resMap.put(ResourceRequest.ANY,
        createResourceReqForIncrease(schedulerKey, resToIncrease,
            RECORD_FACTORY.newRecordInstance(ResourceRequest.class),
            rmContainer, ResourceRequest.ANY));
    return resMap;
  }

  /**
   * 计算本次更新需要新增申请的资源量。
   * @param updateReq 容器更新请求
   * @param rmContainer RM容器对象
   * @return 需要新增的资源量
   */
  private Resource getResourceToIncrease(UpdateContainerRequest updateReq,
      RMContainer rmContainer) {
    // 执行类型升级（例如 Opportunistic -> Guaranteed），需要申请整个容器的资源
    if (updateReq.getContainerUpdateType() ==
        ContainerUpdateType.PROMOTE_EXECUTION_TYPE) {
      return rmContainer.getContainer().getResource();
    }
    // 资源扩容，计算超出原有资源的增量部分
    if (updateReq.getContainerUpdateType() ==
        ContainerUpdateType.INCREASE_RESOURCE) {
      Resource maxCap = Resources.componentwiseMax(updateReq.getCapability(),
          rmContainer.getContainer().getResource());
      return Resources.add(maxCap,
          Resources.negate(rmContainer.getContainer().getResource()));
    }
    return null;
  }

  /**
   * 创建单个位置的容器扩容资源请求。
   * @param schedulerRequestKey 调度请求键
   * @param resToIncrease 需要增加的资源量
   * @param rr 资源请求对象
   * @param rmContainer RM容器对象
   * @param resourceName 请求位置（节点主机/机架/ANY）
   * @return 填充完成的资源请求
   */
  private static ResourceRequest createResourceReqForIncrease(
      SchedulerRequestKey schedulerRequestKey, Resource resToIncrease,
      ResourceRequest rr, RMContainer rmContainer, String resourceName) {
    rr.setResourceName(resourceName);
    rr.setNumContainers(1);
    rr.setRelaxLocality(false);
    rr.setPriority(rmContainer.getContainer().getPriority());
    rr.setAllocationRequestId(schedulerRequestKey.getAllocationRequestId());
    rr.setCapability(resToIncrease);
    rr.setNodeLabelExpression(rmContainer.getNodeLabelExpression());
    // 扩容后容器需要是保证型资源，所以设置执行类型请求
    rr.setExecutionTypeRequest(ExecutionTypeRequest.newInstance(
        ExecutionType.GUARANTEED, true));
    return rr;
  }

  /**
   * 从待处理更新列表中移除容器，完成本次更新流程。
   * @param schedulerKey 调度请求键
   * @param container 已完成更新的容器
   */
  public synchronized void removeFromOutstandingUpdate(
      SchedulerRequestKey schedulerKey, Container container) {
    Map<Resource, Map<NodeId, Set<ContainerId>>> resourceMap =
        outstandingIncreases.get(schedulerKey);
    if (resourceMap != null) {
      Map<NodeId, Set<ContainerId>> locationMap =
          resourceMap.get(container.getResource());
      if (locationMap != null) {
        Set<ContainerId> containerIds = locationMap.get(container.getNodeId());
        if (containerIds != null && !containerIds.isEmpty()) {
          containerIds.remove(container.getId());
          // 清空后移除对应节点条目
          if (containerIds.isEmpty()) {
            locationMap.remove(container.getNodeId());
          }
        }
        if (locationMap.isEmpty()) {
          resourceMap.remove(container.getResource());
        }
      }
      if (resourceMap.isEmpty()) {
        outstandingIncreases.remove(schedulerKey);
      }
    }
    outstandingDecreases.remove(container.getId());
  }

  /**
   * 将新分配的容器匹配到待处理的扩容请求，返回对应待更新的容器ID。
   * @param node 调度节点
   * @param schedulerKey 调度请求键
   * @param rmContainer 新分配的RM容器
   * @return 匹配到的待更新容器ID，如果分配位置不匹配返回UNDEFINED，无匹配返回null
   */
  public ContainerId matchContainerToOutstandingIncreaseReq(
      SchedulerNode node, SchedulerRequestKey schedulerKey,
      RMContainer rmContainer) {
    ContainerId retVal = null;
    Container container = rmContainer.getContainer();
    Map<Resource, Map<NodeId, Set<ContainerId>>> resourceMap =
        outstandingIncreases.get(schedulerKey);
    if (resourceMap != null) {
      Map<NodeId, Set<ContainerId>> locationMap =
          resourceMap.get(container.getResource());
      if (locationMap != null) {
        Set<ContainerId> containerIds = locationMap.get(container.getNodeId());
        if (containerIds != null && !containerIds.isEmpty()) {
          // 取第一个匹配的待处理容器
          retVal = containerIds.iterator().next();
        }
      }
    }
    // 虽然找到了对应请求，但分配到了错误的NM节点，需要重新发起请求
    // 返回UNDEFINED通知调用方释放本次分配的临时容器
    if (resourceMap != null && retVal == null) {
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> reqsToUpdate =
          new HashMap<>();
      Map<String, ResourceRequest> resMap = createResourceRequests
          (rmContainer, node, schedulerKey,
          rmContainer.getContainer().getResource());
      reqsToUpdate.put(schedulerKey, resMap);
      appSchedulingInfo.updateResourceRequests(reqsToUpdate, true);
      return UNDEFINED;
    }
    return retVal;
  }

  /**
   * 交换临时容器和原有容器的资源信息，完成更新。
   * @param tempRMContainer 新分配的临时RM容器
   * @param existingRMContainer 已存在的待更新RM容器
   * @param updateType 更新类型
   * @return 更新完成的原有RM容器
   */
  public RMContainer swapContainer(RMContainer tempRMContainer,
      RMContainer existingRMContainer, ContainerUpdateType updateType) {
    ContainerId matchedContainerId = existingRMContainer.getContainerId();
    // 交换前获取临时容器信息
    Container tempContainer = tempRMContainer.getContainer();

    // 计算更新后的最终资源
    Resource updatedResource = createUpdatedResource(
        tempContainer, existingRMContainer.getContainer(), updateType);
    // 计算需要释放回集群的资源
    Resource resourceToRelease = createResourceToRelease(
        existingRMContainer.getContainer(), updateType);
    // 基于原有容器创建新容器对象，更新资源和执行类型
    Container newContainer = Container.newInstance(matchedContainerId,
        existingRMContainer.getContainer().getNodeId(),
        existingRMContainer.getContainer().getNodeHttpAddress(),
        updatedResource,
        existingRMContainer.getContainer().getPriority(), null,
        tempContainer.getExecutionType());
    newContainer.setExposedPorts(
        existingRMContainer.getContainer().getExposedPorts());
    newContainer.setAllocationRequestId(
        existingRMContainer.getContainer().getAllocationRequestId());
    newContainer.setVersion(existingRMContainer.getContainer().getVersion());

    // 临时容器设置需要释放的资源，后续会归还资源给集群
    tempRMContainer.getContainer().setResource(resourceToRelease);
    tempRMContainer.getContainer().setExecutionType(
        existingRMContainer.getContainer().getExecutionType());

    // 更新原有RM容器指向新容器对象
    ((RMContainerImpl)existingRMContainer).setContainer(newContainer);
    return existingRMContainer;
  }

  /**
   * 计算更新完成后容器的最终资源量。
   * @param tempContainer 临时容器
   * @param existingContainer 原有容器
   * @param updateType 更新类型
   * @return 最终资源量
   */
  private Resource createUpdatedResource(Container tempContainer,
      Container existingContainer, ContainerUpdateType updateType) {
    if (ContainerUpdateType.INCREASE_RESOURCE == updateType) {
      // 资源扩容：