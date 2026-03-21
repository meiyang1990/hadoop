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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 容量调度抢占计算时使用的临时数据结构，跟踪单个队列在单个节点分区上的资源可用情况、待分配资源需求和当前资源使用情况
 */
public class TempQueuePerPartition extends AbstractPreemptionEntity {
  // 当前队列所属的节点分区
  final String partition;

  private final Resource killable;
  private final float absCapacity;
  private final float absMaxCapacity;
  // 该分区的总资源量
  final Resource totalPartitionResource;

  // 以下字段由抢占候选选择策略计算和使用
  // 不可抢占的超额资源
  Resource untouchableExtra;
  // 可抢占的超额资源
  Resource preemptableExtra;

  // 各资源类型的归一化保证资源占比
  double[] normalizedGuarantee;

  // 生效的最小资源配额
  private Resource effMinRes;
  // 生效的最大资源配额
  private Resource effMaxRes;

  // 子队列临时数据列表
  final ArrayList<TempQueuePerPartition> children;
  private Collection<TempAppPerPartition> apps;
  // 叶队列引用，若为父队列则为null
  AbstractLeafQueue leafQueue;
  // 父队列引用，若为根队列则为null
  AbstractParentQueue parentQueue;
  // 当前队列是否禁用抢占
  boolean preemptionDisabled;

  // 扣除预留资源后的待分配资源
  protected Resource pendingDeductReserved;

  // 相对于父队列的优先级，若父队列不支持优先级排序则恒为0
  int relativePriority = 0;
  TempQueuePerPartition parent = null;

  // 当前队列在该分区下各用户的临时数据，存储用户限额、理想分配、已用资源等信息
  Map<String, TempUserPerPartition> usersPerPartition = new LinkedHashMap<>();

  @SuppressWarnings("checkstyle:parameternumber")
  public TempQueuePerPartition(String queueName, Resource current,
      boolean preemptionDisabled, String partition, Resource killable,
      float absCapacity, float absMaxCapacity, Resource totalPartitionResource,
      Resource reserved, CSQueue queue, Resource effMinRes,
      Resource effMaxRes) {
    super(queueName, current, Resource.newInstance(0, 0), reserved,
        Resource.newInstance(0, 0));

    // 从原调度队列获取待分配资源信息
    if (queue instanceof AbstractLeafQueue) {
      AbstractLeafQueue l = (AbstractLeafQueue) queue;
      pending = l.getTotalPendingResourcesConsideringUserLimit(
          totalPartitionResource, partition, false);
      pendingDeductReserved = l.getTotalPendingResourcesConsideringUserLimit(
          totalPartitionResource, partition, true);
      leafQueue = l;
    } else {
      // 父队列待分配资源由子队列聚合，初始化为0
      pending = Resources.createResource(0);
      pendingDeductReserved = Resources.createResource(0);
    }

    if (queue != null && ParentQueue.class.isAssignableFrom(queue.getClass())) {
      parentQueue = (ParentQueue) queue;
    }

    this.normalizedGuarantee = new double[ResourceUtils
        .getNumberOfCountableResourceTypes()];
    this.children = new ArrayList<>();
    this.apps = new ArrayList<>();
    this.untouchableExtra = Resource.newInstance(0, 0);
    this.preemptableExtra = Resource.newInstance(0, 0);
    this.preemptionDisabled = preemptionDisabled;
    this.partition = partition;
    this.killable = killable;
    this.absCapacity = absCapacity;
    this.absMaxCapacity = absMaxCapacity;
    this.totalPartitionResource = totalPartitionResource;
    this.effMinRes = effMinRes;
    this.effMaxRes = effMaxRes;
  }

  /**
   * 设置叶队列引用，仅允许空子队列队列（即本身为叶队列）调用。
   * @param l 叶队列对象
   */
  public void setLeafQueue(AbstractLeafQueue l) {
    assert children.size() == 0;
    this.leafQueue = l;
  }

  /**
   * 添加子队列，同时聚合子队列的待分配资源需求。
   *
   * @param q 要添加的子队列临时数据
   */
  public void addChild(TempQueuePerPartition q) {
    assert leafQueue == null;
    children.add(q);
    Resources.addTo(pending, q.pending);
    Resources.addTo(pendingDeductReserved, q.pendingDeductReserved);
  }

  public ArrayList<TempQueuePerPartition> getChildren() {
    return children;
  }

  /**
   * 接受可用资源分配，计算当前队列可接受的理想资源量，返回剩余未分配资源。
   * @param avail 可供分配的剩余资源
   * @param rc 资源计算器
   * @param clusterResource 集群总资源
   * @param considersReservedResource 是否考虑预留资源
   * @param allowQueueBalanceAfterAllSafisfied 所有队列满足后是否允许继续均衡分配
   * @return 分配后剩余的资源
   */
  Resource offer(Resource avail, ResourceCalculator rc,
      Resource clusterResource, boolean considersReservedResource,
      boolean allowQueueBalanceAfterAllSafisfied) {
    // 计算理想分配可增加的最大资源量（最大配额-当前理想分配），不小于0
    Resource absMaxCapIdealAssignedDelta = Resources.componentwiseMax(
        Resources.subtract(getMax(), idealAssigned),
        Resource.newInstance(0, 0));
    // 计算当前可接受的资源量，取最小值限制：1)最大可增量 2)可用资源 3)(已用+待分配) - 当前理想分配
    Resource accepted = Resources.componentwiseMin(
        absMaxCapIdealAssignedDelta,
        Resources.min(rc, clusterResource, avail, Resources
            .subtract(Resources.add(getUsed(),
                (considersReservedResource ? pending : pendingDeductReserved)),
                idealAssigned)));

    // 所有队列满足后仍允许均衡，不做额外限制
    if (!allowQueueBalanceAfterAllSafisfied) {
      accepted = filterByMaxDeductAssigned(rc, clusterResource, accepted);
    }

    // 根据节点位置过滤可接受资源量，子类可覆盖实现
    accepted = acceptedByLocality(rc, accepted);

    // 确保可接受资源不小于0
    accepted = Resources.componentwiseMax(accepted, Resources.none());

    // 确保不超过提供的可用资源
    accepted = Resources.componentwiseMin(accepted, avail);

    // 计算剩余资源，更新当前队列理想分配量，返回剩余
    Resource remain = Resources.subtract(avail, accepted);
    Resources.addTo(idealAssigned, accepted);
    return remain;
  }

  public float getAbsCapacity() {
    return absCapacity;
  }

  /**
   * 获取当前队列的保证资源配额。
   * @return 保证资源对象
   */
  public Resource getGuaranteed() {
    if(!effMinRes.equals(Resources.none())) {
      return Resources.clone(effMinRes);
    }

    return Resources.multiply(totalPartitionResource, absCapacity);
  }

  /**
   * 获取当前队列的最大资源配额。
   * @return 最大资源对象
   */
  public Resource getMax() {
    if(!effMaxRes.equals(Resources.none())) {
      return Resources.clone(effMaxRes);
    }

    return Resources.multiply(totalPartitionResource, absMaxCapacity);
  }

  /**
   * 更新当前队列可抢占和不可抢占的超额资源量，递归聚合子队列资源。
   * @param rc 资源计算器
   */
  public void updatePreemptableExtras(ResourceCalculator rc) {
    // 重置统计值
    untouchableExtra = Resources.none();
    preemptableExtra = Resources.none();

    // 计算超额资源 = 已用资源 - 保证资源，不小于0
    Resource extra = Resources.subtract(getUsed(), getGuaranteed());
    if (Resources.lessThan(rc, totalPartitionResource, extra,
        Resources.none())) {
      extra = Resources.none();
    }

    if (null == children || children.isEmpty()) {
      // 叶队列：禁用抢占则全部不可抢占，否则全部可抢占
      if (preemptionDisabled) {
        untouchableExtra = extra;
      } else {
        preemptableExtra = extra;
      }
    } else {
      // 父队列：聚合所有子队列的可抢占超额资源
      Resource childrensPreemptable = Resource.newInstance(0, 0);
      for (TempQueuePerPartition child : children) {
        Resources.addTo(childrensPreemptable, child.preemptableExtra);
      }
      // 本层不可抢占超额 = max(总超额 - 子队列可抢占超额, 0)
      if (Resources.greaterThanOrEqual(rc, totalPartitionResource,
          childrensPreemptable, extra)) {
        untouchableExtra = Resource.newInstance(0, 0);
      } else {
        untouchableExtra = Resources.subtract(extra, childrensPreemptable);
      }
      // 本层可抢占超额为子队列可抢占和总超额的较小值
      preemptableExtra = Resources.min(rc, totalPartitionResource,
          childrensPreemptable, extra);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" NAME: " + queueName).append(" CUR: ").append(current)
        .append(" PEN: ").append(pending).append(" RESERVED: ").append(reserved)
        .append(" GAR: ").append(getGuaranteed()).append(" NORM: ")
        .append(Arrays.toString(normalizedGuarantee))
        .append(" IDEAL_ASSIGNED: ").append(idealAssigned)
        .append(" IDEAL_PREEMPT: ").append(toBePreempted)
        .append(" ACTUAL_PREEMPT: ").append(getActuallyToBePreempted())
        .append(" UNTOUCHABLE: ").append(untouchableExtra)
        .append(" PREEMPTABLE: ").append(preemptableExtra).append("\n");

    return sb.toString();
  }

  /**
   * 计算当前队列需要被抢占的资源量。
   * @param scalingFactor 抢占缩放比例
   * @param rc 资源计算器
   * @param clusterResource 集群总资源
   */
  public void assignPreemption(float scalingFactor, ResourceCalculator rc,
      Resource clusterResource) {
    // 扣除可kill资源后的已用资源
    Resource usedDeductKillable = Resources.subtract(getUsed(), killable);
    // 已用+待分配总资源
    Resource totalResource = Resources.add(getUsed(), pending);

    // 计算队列需要保留的最小资源：max(理想分配, min(总资源, 保证资源))
    // 避免在队列资源已低于保证配额时仍然执行抢占
    Resource minimumQueueResource = Resources.max(rc, clusterResource,
        Resources.min(rc, clusterResource, totalResource, getGuaranteed()),
        idealAssigned);

    // 若扣除可kill后仍大于最小保留资源，则计算需要抢占的资源量
    if (Resources.greaterThan(rc, clusterResource, usedDeductKillable,
        minimumQueueResource)) {
      toBePreempted = Resources.multiply(
          Resources.subtract(usedDeductKillable, minimumQueueResource),
          scalingFactor);
    } else {
      // 不需要抢占
      toBePreempted = Resources.none();
    }
  }

  /**
   * 扣除实际已完成抢占的资源量，更新待抢占资源。
   * @param rc 资源计算器
   * @param cluster 集群总资源
   * @param toBeDeduct 需要扣除的资源量
   */
  public void deductActuallyToBePreempted(ResourceCalculator rc,
      Resource cluster, Resource toBeDeduct) {
    if (Resources.greaterThan(rc, cluster, getActuallyToBePreempted(),
        toBeDeduct)) {
      Resources.subtractFrom(getActuallyToBePreempted(), toBeDeduct);
    }
    setActuallyToBePreempted(Resources.max(rc, cluster,
        getActuallyToBePreempted(), Resources.none()));
  }

  void appendLogString(StringBuilder sb) {
    sb.append(queueName).append(", ").append(current.getMemorySize())
        .append(", ").append(current.getVirtualCores()).append(", ")
        .append(pending.getMemorySize()).append(", ")
        .append(pending.getVirtualCores()).append(", ")
        .append(getGuaranteed().getMemorySize()).append(", ")
        .append(getGuaranteed().getVirtualCores()).append(", ")
        .append(idealAssigned.getMemorySize()).append(", ")
        .append(idealAssigned.getVirtualCores()).append(", ")
        .append(toBePreempted.getMemorySize()).append(", ")
        .append(toBePreempted.getVirtualCores()).append(", ")
        .append(getActuallyToBePreempted().getMemorySize()).append(", ")
        .append(getActuallyToBePreempted().getVirtualCores());
  }

  public void addAllApps(Collection<TempAppPerPartition> orderedApps) {
    this.apps = orderedApps;
  }

  public Collection<TempAppPerPartition> getApps() {
    return apps;
  }

  public void addUserPerPartition(String userName,
      TempUserPerPartition tmpUser) {
    this.usersPerPartition.put(userName, tmpUser);
  }

  public Map<String, TempUserPerPartition> getUsersPerPartition() {
    return usersPerPartition;
  }

  public void setPending(Resource pending) {
    this.pending = pending;
  }

  public Resource getIdealAssigned() {
    return idealAssigned;
  }

  public String toGlobalString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n").append(toString());
    for (TempQueuePerPartition c : children) {
      sb.append(c.toGlobalString());
    }
    return sb.toString();
  }

  /**
   * 根据节点位置过滤可接受资源量，子类可覆盖实现自定义限制。
   * 默认实现直接返回输入资源，不做限制。
   *
   * @param rc 资源计算器
   * @param offered 分配给当前队列的资源
   * @return 考虑位置限制后可接受的资源量
   */
  protected Resource acceptedByLocality(ResourceCalculator rc,
      Resource offered) {
    return offered;
  }

  /**
   * 根据最大允许增量过滤可接受资源量，子类可覆盖实现（如联邦场景修改此逻辑）。
   * 叶队列限制：可接受资源不超过 max(保证资源, 已用资源) - 已分配理想资源，避免过度抢占。
   *
   * @param rc 资源计算器
   * @param clusterResource 集群总资源
   * @param offered 待过滤的可接受资源
   * @return 过滤后的可接受资源量
   */
  protected Resource filterByMaxDeductAssigned(ResourceCalculator rc,
      Resource clusterResource, Resource offered) {
    if (null == children || children.isEmpty()) {
      Resource maxOfGuranteedAndUsedDeductAssigned = Resources.subtract(
          Resources.max(rc, clusterResource, getUsed(), getGuaranteed()),
          idealAssigned);
      maxOfGuranteedAndUsedDeductAssigned = Resources.max(rc, clusterResource,
          maxOfGuranteedAndUsedDeductAssigned, Resources.none());
      offered = Resources.min(rc, clusterResource, offered,
          maxOfGuranteedAndUsedDeductAssigned);
    }
    return offered;
  }

  /**
   * 初始化根队列的理想分配为保证资源，子类可覆盖实现（如联邦场景需要初始化多个根队列）。
   */
  protected void initializeRootIdealWithGuarangeed() {