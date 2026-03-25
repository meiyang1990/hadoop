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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Fair/Capacity 调度器通用调度节点实现，继承基础SchedulerNode，扩展支持抢占相关能力
 */
public class FiCaSchedulerNode extends SchedulerNode {

  private static final Logger LOG =
      LoggerFactory.getLogger(FiCaSchedulerNode.class);
  // 可被抢占杀死的容器列表，key为容器ID，value为对应的RMContainer
  private Map<ContainerId, RMContainer> killableContainers = new HashMap<>();
  // 当前节点上所有可抢占容器的总资源量
  private Resource totalKillableResources = Resource.newInstance(0, 0);
  
  /**
   * 构造FiCaSchedulerNode实例
   * @param node 底层RMNode对象
   * @param usePortForNodeName 是否在节点名称中包含端口
   * @param nodeLabels 节点标签集合
   */
  public FiCaSchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> nodeLabels) {
    super(node, usePortForNodeName, nodeLabels);
  }

  /**
   * 构造FiCaSchedulerNode实例（使用空节点标签）
   * @param node 底层RMNode对象
   * @param usePortForNodeName 是否在节点名称中包含端口
   */
  public FiCaSchedulerNode(RMNode node, boolean usePortForNodeName) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET);
  }

  @Override
  public synchronized void reserveResource(
      SchedulerApplicationAttempt application, SchedulerRequestKey priority,
      RMContainer container) {
    // 检查当前节点是否已经预留了资源
    RMContainer reservedContainer = getReservedContainer();
    if (reservedContainer != null) {
      // 完整性检查：确保要预留的容器确实分配在本节点上
      if (!container.getContainer().getNodeId().equals(getNodeID())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container +
            " on node " + container.getReservedNode() + 
            " when currently" + " reserved resource " + reservedContainer +
            " on node " + reservedContainer.getReservedNode());
      }
      
      // 一个节点同一时间只能为一个应用尝试预留资源
      // 预留绑定到应用尝试级别
      if (!reservedContainer.getContainer().getId().getApplicationAttemptId()
          .equals(container.getContainer().getId().getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container + 
            " for application " + application.getApplicationAttemptId() + 
            " when currently" +
            " reserved container " + reservedContainer +
            " on node " + this);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updated reserved container "
            + container.getContainer().getId() + " on node " + this
            + " for application attempt "
            + application.getApplicationAttemptId());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reserved container "
            + container.getContainer().getId() + " on node " + this
            + " for application attempt "
            + application.getApplicationAttemptId());
      }
    }
    // 更新预留容器信息
    setReservedContainer(container);
  }

  @Override
  public synchronized void unreserveResource(
      SchedulerApplicationAttempt application) {
    // 添加空指针检查，因为现在抢占场景也可能调用此方法
    if (getReservedContainer() != null
        && getReservedContainer().getContainer() != null
        && getReservedContainer().getContainer().getId() != null
        && getReservedContainer().getContainer().getId()
          .getApplicationAttemptId() != null) {

      // 获取当前预留容器所属的应用尝试ID
      ApplicationAttemptId reservedApplication =
          getReservedContainer().getContainer().getId()
            .getApplicationAttemptId();
      // 检查调用方是否确实是当前预留资源所属的应用尝试
      if (!reservedApplication.equals(
          application.getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to unreserve " +
            " for application " + application.getApplicationAttemptId() +
            " when currently reserved " +
            " for application " + reservedApplication.getApplicationId() +
            " on node " + this);
      }
    }
    // 清空预留容器信息，完成取消预留
    setReservedContainer(null);
  }

  /**
   * 根据抢占策略，将指定容器标记为可被抢占杀死
   * @param containerId 目标容器ID
   */
  // According to decisions from preemption policy, mark the container to killable
  public synchronized void markContainerToKillable(ContainerId containerId) {
    RMContainer c = getContainer(containerId);
    if (c != null && !killableContainers.containsKey(containerId)) {
      killableContainers.put(containerId, c);
      Resources.addTo(totalKillableResources, c.getAllocatedResource());
    }
  }

  /**
   * 根据抢占策略，将指定容器标记为不可被抢占杀死
   * @param containerId 目标容器ID
   */
  // According to decisions from preemption policy, mark the container to
  // non-killable
  public synchronized void markContainerToNonKillable(ContainerId containerId) {
    RMContainer c = getContainer(containerId);
    if (c != null && killableContainers.containsKey(containerId)) {
      killableContainers.remove(containerId);
      Resources.subtractFrom(totalKillableResources, c.getAllocatedResource());
    }
  }

  @Override
  protected synchronized void updateResourceForReleasedContainer(
      Container container) {
    super.updateResourceForReleasedContainer(container);
    // 如果释放的容器在可抢占列表中，同步更新可抢占资源统计
    if (killableContainers.containsKey(container.getId())) {
      Resources.subtractFrom(totalKillableResources, container.getResource());
      killableContainers.remove(container.getId());
    }
  }

  /**
   * 获取当前节点所有可抢占容器的总资源量
   * @return 总可抢占资源量
   */
  public synchronized Resource getTotalKillableResources() {
    return totalKillableResources;
  }

  /**
   * 获取当前节点所有可抢占容器的不可修改映射
   * @return 可抢占容器映射表（只读）
   */
  public synchronized Map<ContainerId, RMContainer> getKillableContainers() {
    return Collections.unmodifiableMap(killableContainers);
  }

  @Override
  protected synchronized void allocateContainer(RMContainer rmContainer,
      boolean launchedOnNode) {
    super.allocateContainer(rmContainer, launchedOnNode);

    final Container container = rmContainer.getContainer();
    // 记录容器分配日志，输出分配后节点资源使用情况
    LOG.info("Assigned container " + container.getId() + " of capacity "
          + container.getResource() + " on host " + getRMNode().getNodeAddress()
          + ", which has " + getNumContainers() + " containers, "
          + getAllocatedResource() + " used and " + getUnallocatedResource()
          + " available after allocation");
  }

}