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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;


/**
 * 从调度器视角表示YARN集群节点，封装节点资源管理、容器生命周期管理相关状态和操作
 */
@Private
@Unstable
public abstract class SchedulerNode {

  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulerNode.class);

  // 节点可用未分配资源
  private Resource unallocatedResource = Resource.newInstance(0, 0);
  // 节点已分配资源
  private Resource allocatedResource = Resource.newInstance(0, 0);
  // 节点总资源
  private Resource totalResource;
  // 节点预留容器（用于调度延迟分配）
  private RMContainer reservedContainer;
  // 节点上运行容器数量
  private volatile int numContainers;
  // 所有容器合计资源利用率
  private volatile ResourceUtilization containersUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);
  // 节点整体资源利用率（包含容器和节点系统进程）
  private volatile ResourceUtilization nodeUtilization =
      ResourceUtilization.newInstance(0, 0, 0f);
  /** Time stamp for overcommitted resources to time out. */
  // 超资源分配超时时间戳
  private long overcommitTimeout = -1;

  /* set of containers that are allocated containers */
  // 已分配到该节点的已启动容器集合，key为容器ID
  private final Map<ContainerId, ContainerInfo> launchedContainers =
      new HashMap<>();

  // 关联的RMNode对象（RM维护的节点元数据）
  private final RMNode rmNode;
  // 供调度匹配使用的节点名称
  private final String nodeName;
  // RM上下文对象
  private final RMContext rmContext;

  // 节点标签集合
  private volatile Set<String> labels = null;

  // 节点属性集合
  private volatile Set<NodeAttribute> nodeAttributes = null;

  // 上次心跳时间（单调时间）
  private volatile long lastHeartbeatMonotonicTime;

  /**
   * 构造调度节点对象，初始化资源、标签和节点名称
   * @param node 关联的RMNode节点元数据
   * @param usePortForNodeName 是否在节点名称中包含端口
   * @param labels 节点标签集合
   */
  public SchedulerNode(RMNode node, boolean usePortForNodeName,
      Set<String> labels) {
    this.rmNode = node;
    this.rmContext = node.getRMContext();
    this.unallocatedResource = Resources.clone(node.getTotalCapability());
    this.totalResource = Resources.clone(node.getTotalCapability());
    if (usePortForNodeName) {
      nodeName = rmNode.getHostName() + ":" + node.getNodeID().getPort();
    } else {
      nodeName = rmNode.getHostName();
    }
    this.labels = ImmutableSet.copyOf(labels);
    this.lastHeartbeatMonotonicTime = Time.monotonicNow();
  }

  /**
   * 使用空标签构造调度节点对象
   * @param node 关联的RMNode节点元数据
   * @param usePortForNodeName 是否在节点名称中包含端口
   */
  public SchedulerNode(RMNode node, boolean usePortForNodeName) {
    this(node, usePortForNodeName, CommonNodeLabelsManager.EMPTY_STRING_SET);
  }

  /**
   * 获取关联的RMNode节点元数据
   * @return 关联的RMNode对象
   */
  public RMNode getRMNode() {
    return this.rmNode;
  }

  /**
   * Set total resources on the node.
   * @param resource Total resources on the node.
   */
  /**
   * 更新节点总资源，重新计算可用未分配资源
   * @param resource 节点新总资源
   */
  public synchronized void updateTotalResource(Resource resource){
    this.totalResource = resource;
    this.unallocatedResource = Resources.subtract(totalResource,
        this.allocatedResource);
  }

  /**
   * Set the timeout for the node to stop overcommitting the resources. After
   * this time the scheduler will start killing containers until the resources
   * are not overcommitted anymore. This may reset a previous timeout.
   * @param timeOut Time out in milliseconds.
   */
  /**
   * 设置超资源分配超时时间，超过该时间后开始清理超分容器
   * @param timeOut 超时时间，单位毫秒
   */
  public synchronized void setOvercommitTimeOut(long timeOut) {
    if (timeOut >= 0) {
      if (this.overcommitTimeout != -1) {
        LOG.debug("The overcommit timeout for {} was already set to {}",
            getNodeID(), this.overcommitTimeout);
      }
      this.overcommitTimeout = Time.now() + timeOut;
    }
  }

  /**
   * Check if the time out has passed.
   * @return If the node is overcommitted.
   */
  /**
   * 检查超资源分配是否已超时
   * @return 超分是否已超时
   */
  public synchronized boolean isOvercommitTimedOut() {
    return this.overcommitTimeout >= 0 && Time.now() >= this.overcommitTimeout;
  }

  /**
   * Check if the node has a time out for overcommit resources.
   * @return If the node has a time out for overcommit resources.
   */
  /**
   * 检查是否已设置超资源分配超时
   * @return 是否已设置超分超时
   */
  public synchronized boolean isOvercommitTimeOutSet() {
    return this.overcommitTimeout >= 0;
  }

  /**
   * Get the ID of the node which contains both its hostname and port.
   * @return The ID of the node.
   */
  /**
   * 获取节点ID（包含主机名和端口）
   * @return 节点ID
   */
  public NodeId getNodeID() {
    return this.rmNode.getNodeID();
  }

  /**
   * Get HTTP address for the node.
   * @return HTTP address for the node.
   */
  /**
   * 获取节点HTTP服务地址
   * @return 节点HTTP地址
   */
  public String getHttpAddress() {
    return this.rmNode.getHttpAddress();
  }

  /**
   * Get the name of the node for scheduling matching decisions.
   * <p>
   * Typically this is the 'hostname' reported by the node, but it could be
   * configured to be 'hostname:port' reported by the node via the
   * {@link YarnConfiguration#RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} constant.
   * The main usecase of this is YARN minicluster to be able to differentiate
   * node manager instances by their port number.
   * @return Name of the node for scheduling matching decisions.
   */
  /**
   * 获取用于调度匹配的节点名称
   * @return 调度用节点名称
   */
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Get rackname.
   * @return rackname
   */
  /**
   * 获取节点所在机架名称
   * @return 机架名称
   */
  public String getRackName() {
    return this.rmNode.getRackName();
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * @param rmContainer Allocated container
   */
  /**
   * 在节点上分配容器，默认容器未启动
   * @param rmContainer 已分配容器对象
   */
  public void allocateContainer(RMContainer rmContainer) {
    allocateContainer(rmContainer, false);
  }

  /**
   * The Scheduler has allocated containers on this node to the given
   * application.
   * @param rmContainer Allocated container
   * @param launchedOnNode True if the container has been launched
   */
  /**
   * 在节点上分配容器，可指定容器是否已启动
   * @param rmContainer 已分配容器对象
   * @param launchedOnNode 容器是否已在节点启动
   */
  protected synchronized void allocateContainer(RMContainer rmContainer,
      boolean launchedOnNode) {
    Container container = rmContainer.getContainer();
    // 保障型容器才扣除可用资源
    if (rmContainer.getExecutionType() == ExecutionType.GUARANTEED) {
      deductUnallocatedResource(container.getResource());
      ++numContainers;
    }

    launchedContainers.put(container.getId(),
        new ContainerInfo(rmContainer, launchedOnNode));
  }

  /**
   * Get unallocated resources on the node.
   * @return Unallocated resources on the node
   */
  /**
   * 获取节点未分配可用资源
   * @return 节点未分配资源
   */
  public synchronized Resource getUnallocatedResource() {
    return this.unallocatedResource;
  }

  /**
   * Get allocated resources on the node.
   * @return Allocated resources on the node
   */
  /**
   * 获取节点已分配资源
   * @return 节点已分配资源
   */
  public synchronized Resource getAllocatedResource() {
    return this.allocatedResource;
  }

  /**
   * Get total resources on the node.
   * @return Total resources on the node.
   */
  /**
   * 获取节点总资源
   * @return 节点总资源
   */
  public synchronized Resource getTotalResource() {
    return this.totalResource;
  }

  /**
   * Check if a container is launched by this node.
   *
   * @param containerId containerId.
   * @return If the container is launched by the node.
   */
  /**
   * 检查容器是否在该节点上分配
   * @param containerId 容器ID
   * @return 容器是否分配在该节点
   */
  public synchronized boolean isValidContainer(ContainerId containerId) {
    if (launchedContainers.containsKey(containerId)) {
      return true;
    }
    return false;
  }

  /**
   * Update the resources of the node when releasing a container.
   * @param container Container to release.
   */
  /**
   * 释放容器时更新节点资源统计
   * @param container 待释放容器
   */
  protected synchronized void updateResourceForReleasedContainer(
      Container container) {
    if (container.getExecutionType() == ExecutionType.GUARANTEED) {
      addUnallocatedResource(container.getResource());
      --numContainers;
    }
  }

  /**
   * Release an allocated container on this node.
   * @param containerId ID of container to be released.
   * @param releasedByNode whether the release originates from a node update.
   */
  /**
   * 释放节点上已分配的容器
   * @param containerId 待释放容器ID
   * @param releasedByNode 释放请求是否来自节点心跳上报
   */
  public synchronized void releaseContainer(ContainerId containerId,
      boolean releasedByNode) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info == null) {
      return;
    }
    // 如果不是节点发起的释放且容器已启动，等待节点上报完成后再释放
    if (!releasedByNode && info.launchedOnNode) {
      // wait until node reports container has completed
      return;
    }

    // 从已启动容器集合中移除
    launchedContainers.remove(containerId);
    Container container = info.container.getContainer();

    // We remove allocation tags when a container is actually
    // released on NM. This is to avoid running into situation
    // when AM releases a container and NM has some delay to
    // actually release it, then the tag can still be visible
    // at RM so that RM can respect it during scheduling new containers.
    // 容器真正释放后移除分配标签，避免调度时仍被占用
    if (rmContext != null && rmContext.getAllocationTagsManager() != null) {
      rmContext.getAllocationTagsManager()
          .removeContainer(container.getNodeId(),
              container.getId(), container.getAllocationTags());
    }

    // 更新节点资源统计
    updateResourceForReleasedContainer(container);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Released container " + container.getId() + " of capacity "
              + container.getResource() + " on host " + rmNode.getNodeAddress()
              + ", which currently has " + numContainers + " containers, "
              + getAllocatedResource() + " used and " + getUnallocatedResource()
              + " available" + ", release resources=" + true);
    }
  }

  /**
   * Inform the node that a container has launched.
   * @param containerId ID of the launched container
   */
  /**
   * 标记容器已在节点上启动
   * @param containerId 已启动容器ID
   */
  public synchronized void containerStarted(ContainerId containerId) {
    ContainerInfo info = launchedContainers.get(containerId);
    if (info != null) {
      info.launchedOnNode = true;
    }
  }

  /**
   * Add unallocated resources to the node. This is used when unallocating a
   * container.
   * @param resource Resources to add.
   */
  /**
   * 增加节点未分配资源，释放容器时调用
   * @param resource 待增加的资源量
   */
  private synchronized void addUnallocatedResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid resource addition of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.addTo(unallocatedResource, resource);
    Resources.subtractFrom(allocatedResource, resource);
  }

  /**
   * Deduct unallocated resources from the node. This is used when allocating a
   * container.
   * @param resource Resources to deduct.
   */
  /**
   * 扣除节点未分配资源，分配容器时调用
   * @param resource 待扣除的资源量
   */
  @VisibleForTesting
  public synchronized void deductUnallocatedResource(Resource resource) {
    if (resource == null) {
      LOG.error("Invalid deduction of null resource for "
          + rmNode.getNodeAddress());
      return;
    }
    Resources.subtractFrom(unallocatedResource, resource);
    Resources.addTo(allocatedResource, resource);
  }

  /**
   * Reserve container for the attempt on this node.
   * @param attempt Application attempt asking for the reservation.
   * @param schedulerKey Priority of the reservation.
   * @param container Container reserving resources for.
   */
  /**
   * 为应用尝试在节点上预留容器资源
   * @param attempt 申请预留的应用尝试
   * @param schedulerKey 预留请求优先级键
   * @param container 待预留容器
   */
  public abstract void reserveResource(SchedulerApplicationAttempt attempt,
      SchedulerRequestKey schedulerKey, RMContainer container);

  /**
   * Unres