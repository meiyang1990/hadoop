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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 公平调度器专属的调度节点实现，扩展基础调度节点能力，支持抢占机制。
 */
@Private
@Unstable
public class FSSchedulerNode extends SchedulerNode {

  private static final Logger LOG =
      LoggerFactory.getLogger(FSSchedulerNode.class);
  // 当前节点上已预留资源的应用
  private FSAppAttempt reservedAppSchedulable;
  // 存储待抢占的容器列表
  @VisibleForTesting
  final Set<RMContainer> containersForPreemption =
      new ConcurrentSkipListSet<>();
  // 存储每个应用已抢占预留的资源总量
  @VisibleForTesting
  final Map<FSAppAttempt, Resource>
      resourcesPreemptedForApp = new LinkedHashMap<>();
  // 应用尝试ID到应用调度实例的映射
  private final Map<ApplicationAttemptId, FSAppAttempt> appIdToAppMap =
      new HashMap<>();
  // 节点上所有已抢占预留的资源总量，即所有计划被抢占的资源总和
  private Resource totalResourcesPreempted = Resource.newInstance(0, 0);

  /**
   * 构造公平调度器节点实例。
   * @param node 底层RM节点对象
   * @param usePortForNodeName 是否将端口包含进节点名称
   */
  public FSSchedulerNode(RMNode node, boolean usePortForNodeName) {
    super(node, usePortForNodeName);
  }

  /**
   * 获取节点上所有预留资源总和，包含常规预留容器和抢占预留资源。
   * @return 总预留资源
   */
  Resource getTotalReserved() {
    // 克隆基础预留容器的资源，如果没有预留容器则初始化为0
    Resource totalReserved = Resources.clone(getReservedContainer() != null
        ? getReservedContainer().getAllocatedResource()
        : Resource.newInstance(0, 0));
    // 加上抢占预留的总资源
    Resources.addTo(totalReserved, totalResourcesPreempted);
    return totalReserved;
  }

  @Override
  public synchronized void reserveResource(
      SchedulerApplicationAttempt application, SchedulerRequestKey schedulerKey,
      RMContainer container) {
    // 检查是否已有预留容器
    RMContainer reservedContainer = getReservedContainer();
    if (reservedContainer != null) {
      // 完整性检查：确保预留容器的节点ID匹配当前节点
      if (!container.getContainer().getNodeId().equals(getNodeID())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container +
            " on node " + container.getReservedNode() + 
            " when currently" + " reserved resource " + reservedContainer +
            " on node " + reservedContainer.getReservedNode());
      }
      
      // 单个节点同一时间只能为一个应用预留资源，检查是否为同一应用
      if (!reservedContainer.getContainer().getId().getApplicationAttemptId()
          .equals(container.getContainer().getId().getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container + 
            " for application " + application.getApplicationId() + 
            " when currently" +
            " reserved container " + reservedContainer +
            " on node " + this);
      }

      LOG.info("Updated reserved container " + container.getContainer().getId()
          + " on node " + this + " for application "
          + application.getApplicationId());
    } else {
      LOG.info("Reserved container " + container.getContainer().getId()
          + " on node " + this + " for application "
          + application.getApplicationId());
    }
    // 更新预留容器
    setReservedContainer(container);
    // 保存预留应用的调度实例引用
    this.reservedAppSchedulable = (FSAppAttempt) application;
  }

  @Override
  public synchronized void unreserveResource(
      SchedulerApplicationAttempt application) {
    // 检查取消预留的应用是否匹配当前预留的应用
    ApplicationAttemptId reservedApplication = 
        getReservedContainer().getContainer().getId()
            .getApplicationAttemptId();
    if (!reservedApplication.equals(
        application.getApplicationAttemptId())) {
      throw new IllegalStateException("Trying to unreserve " +  
          " for application " + application.getApplicationId() + 
          " when currently reserved " + 
          " for application " + reservedApplication.getApplicationId() + 
          " on node " + this);
    }
    
    // 清空预留容器和应用引用
    setReservedContainer(null);
    this.reservedAppSchedulable = null;
  }

  /**
   * 获取当前节点预留资源对应的应用调度实例。
   * @return 预留应用调度实例，无预留则返回null
   */
  synchronized FSAppAttempt getReservedAppSchedulable() {
    return reservedAppSchedulable;
  }

  /**
   * 清理后获取抢占预留列表，按FIFO顺序返回每个应用对应的抢占预留资源，用于分配。
   * @return 应用到抢占预留资源的映射
   */
  @VisibleForTesting
  synchronized LinkedHashMap<FSAppAttempt, Resource> getPreemptionList() {
    cleanupPreemptionList();
    return new LinkedHashMap<>(resourcesPreemptedForApp);
  }

  /**
   * 检查指定应用是否在本节点有抢占预留资源。
   * @return 是否存在抢占预留资源
   */
  synchronized boolean isPreemptedForApp(FSAppAttempt app){
    return resourcesPreemptedForApp.containsKey(app);
  }

  /**
   * 清理抢占列表中不再需要资源的应用，释放已完成抢占预留的资源记录。
   */
  private void cleanupPreemptionList() {
    // 单独加锁获取候选列表，避免死锁，该方式可能会导致清理延迟，是可接受的
    LinkedList<FSAppAttempt> candidates;
    synchronized (this) {
      candidates = Lists.newLinkedList(resourcesPreemptedForApp.keySet());
    }
    // 遍历检查每个应用是否还需要资源
    for (FSAppAttempt app : candidates) {
      // 应用已停止、不再饥饿，且最小资源和公平资源都已满足，则移除
      if (app.isStopped() || !app.isStarved() ||
          (Resources.isNone(app.getFairshareStarvation()) &&
           Resources.isNone(app.getMinshareStarvation()))) {
        // 应用不再需要更多资源，移除该应用的抢占记录
        synchronized (this) {
          Resource removed = resourcesPreemptedForApp.remove(app);
          if (removed != null) {
            // 从总抢占资源中扣除
            Resources.subtractFrom(totalResourcesPreempted,
                removed);
            // 移除ID映射
            appIdToAppMap.remove(app.getApplicationAttemptId());
          }
        }
      }
    }
  }

  /**
   * 将一批容器标记为待抢占，避免重复加入抢占队列，对应释放容器时需要调用releaseContainer清理。
   * @param containers 待抢占容器集合
   * @param app 抢占后资源将分配给的目标应用
   */
  void addContainersForPreemption(Collection<RMContainer> containers,
                                  FSAppAttempt app) {

    // 累计当前批次为该应用抢占的资源总量
    Resource appReserved = Resources.createResource(0);

    for(RMContainer container : containers) {
      if(containersForPreemption.add(container)) {
        // 新增成功则累加资源
        Resources.addTo(appReserved, container.getAllocatedResource());
      }
    }

    synchronized (this) {
      // 如果有新增抢占资源，更新统计
      if (!Resources.isNone(appReserved)) {
        // 累加到总抢占资源
        Resources.addTo(totalResourcesPreempted,
            appReserved);
        // 维护ID到应用的映射
        appIdToAppMap.putIfAbsent(app.getApplicationAttemptId(), app);
        // 初始化或累加应用对应的抢占资源
        resourcesPreemptedForApp.
            putIfAbsent(app, Resource.newInstance(0, 0));
        Resources.addTo(resourcesPreemptedForApp.get(app), appReserved);
      }
    }
  }

  /**
   * 获取所有标记为待抢占的容器集合。
   * @return 待抢占容器集合
   */
  Set<RMContainer> getContainersForPreemption() {
    return containersForPreemption;
  }

  /**
   * 容器分配完成后的处理，更新抢占预留统计，扣除已满足的抢占资源。
   * @param rmContainer 已分配的容器
   * @param launchedOnNode 容器是否已在节点启动
   */
  @Override
  protected synchronized void allocateContainer(RMContainer rmContainer,
                                                boolean launchedOnNode) {
    super.allocateContainer(rmContainer, launchedOnNode);
    if (LOG.isDebugEnabled()) {
      final Container container = rmContainer.getContainer();
      LOG.debug("Assigned container " + container.getId() + " of capacity "
          + container.getResource() + " on host " + getRMNode().getNodeAddress()
          + ", which has " + getNumContainers() + " containers, "
          + getAllocatedResource() + " used and " + getUnallocatedResource()
          + " available after allocation");
    }

    Resource allocated = rmContainer.getAllocatedResource();
    if (!Resources.isNone(allocated)) {
      // 检查该分配是否满足某个应用的抢占预留请求
      FSAppAttempt app =
          appIdToAppMap.get(rmContainer.getApplicationAttemptId());
      if (app != null) {
        Resource reserved = resourcesPreemptedForApp.get(app);
        // 计算本次分配满足了多少抢占预留资源，取最小值避免扣超
        Resource fulfilled = Resources.componentwiseMin(reserved, allocated);
        // 从应用预留资源中扣除已满足的部分
        Resources.subtractFrom(reserved, fulfilled);
        // 从总抢占资源中扣除已满足的部分
        Resources.subtractFrom(totalResourcesPreempted, fulfilled);
        // 如果应用预留资源已全部满足，移除该应用的抢占记录
        if (Resources.isNone(reserved)) {
          resourcesPreemptedForApp.remove(app);
          appIdToAppMap.remove(rmContainer.getApplicationAttemptId());
        }
      }
    } else {
      LOG.error("Allocated empty container" + rmContainer.getContainerId());
    }
  }

  /**
   * 释放容器，同步从待抢占列表中移除，处理内存泄漏。
   * @param containerId 要释放的容器ID
   * @param releasedByNode 是否由节点更新发起的释放
   */
  @Override
  public synchronized void releaseContainer(ContainerId containerId,
                                            boolean releasedByNode) {
    RMContainer container = getContainer(containerId);
    super.releaseContainer(containerId, releasedByNode);
    if (container != null) {
      // 从待抢占列表中移除已释放的容器
      containersForPreemption.remove(container);
    }
  }
}