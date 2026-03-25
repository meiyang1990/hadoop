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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 公平调度器队列抽象基类，定义了公平调度中所有队列通用的属性和行为
 * 是叶队列（应用直接提交的队列）和父队列（包含子队列的队列）的公共父类
 */
@Private
@Unstable
public abstract class FSQueue implements Queue, Schedulable {
  private static final Logger LOG = LoggerFactory.getLogger(
      FSQueue.class.getName());

  private Resource fairShare = Resources.createResource(0, 0);
  private Resource steadyFairShare = Resources.createResource(0, 0);
  private Resource reservedResource = Resources.createResource(0, 0);
  private final Resource resourceUsage = Resource.newInstance(0, 0);
  private final String name;
  protected final FairScheduler scheduler;
  private final YarnAuthorizationProvider authorizer;
  private final PrivilegedEntity queueEntity;
  private final FSQueueMetrics metrics;
  
  protected final FSParentQueue parent;
  protected final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  protected SchedulingPolicy policy = SchedulingPolicy.DEFAULT_POLICY;

  protected float weights;
  protected Resource minShare;
  private ConfigurableResource maxShare;
  protected int maxRunningApps;
  private ConfigurableResource maxChildQueueResource;

  // maxAMShare is a value between 0 and 1.
  protected float maxAMShare;

  private long fairSharePreemptionTimeout = Long.MAX_VALUE;
  private long minSharePreemptionTimeout = Long.MAX_VALUE;
  private float fairSharePreemptionThreshold = 0.5f;
  private boolean preemptable = true;
  private boolean isDynamic = true;
  protected Resource maxContainerAllocation;

  /**
   * 构造一个公平调度队列实例
   * @param name 队列名称
   * @param scheduler 所属公平调度器实例
   * @param parent 父队列，根队列父队列为null
   */
  public FSQueue(String name, FairScheduler scheduler, FSParentQueue parent) {
    this.name = name;
    this.scheduler = scheduler;
    this.authorizer =
        YarnAuthorizationProvider.getInstance(scheduler.getConf());
    this.queueEntity = new PrivilegedEntity(EntityType.QUEUE, name);
    this.metrics = FSQueueMetrics.forQueue(getName(), parent, true, scheduler.getConf());
    this.parent = parent;
    setPolicy(scheduler.getAllocationConfiguration().getSchedulingPolicy(name));
    reinit(false);
  }

  /**
   * Initialize a queue by setting its queue-specific properties and its
   * metrics. This method is invoked when creating a new queue or reloading
   * the allocation file.
   * This method does not set policies for queues when reloading the allocation
   * file since we need to either set all new policies or nothing, which is
   * handled by method {@link #verifyAndSetPolicyFromConf}.
   *
   * @param recursive whether child queues should be reinitialized recursively
   */
  public final void reinit(boolean recursive) {
    // 获取最新的分配配置
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    // 从配置初始化当前队列属性
    allocConf.initFSQueue(this);
    // 更新抢占相关配置参数
    updatePreemptionVariables();

    // 如果需要递归，重新初始化所有子队列
    if (recursive) {
      for (FSQueue child : getChildQueues()) {
        child.reinit(recursive);
      }
    }
  }

  public String getName() {
    return name;
  }

  @Override
  public String getQueueName() {
    return name;
  }

  public SchedulingPolicy getPolicy() {
    return policy;
  }

  public FSParentQueue getParent() {
    return parent;
  }

  /**
   * 设置队列使用的调度策略
   * @param policy 调度策略实例
   */
  public void setPolicy(SchedulingPolicy policy) {
    policy.initialize(scheduler.getContext());
    this.policy = policy;
  }

  public void setWeights(float weights) {
    this.weights = weights;
  }

  @Override
  public float getWeight() {
    return weights;
  }

  public void setMinShare(Resource minShare){
    this.minShare = minShare;
  }

  @Override
  public Resource getMinShare() {
    return minShare;
  }

  public void setMaxShare(ConfigurableResource maxShare){
    this.maxShare = maxShare;
  }

  public void setMaxContainerAllocation(Resource maxContainerAllocation){
    this.maxContainerAllocation = maxContainerAllocation;
  }

  /**
   * 获取队列允许的单个容器最大分配资源量，由子类实现
   * @return 单个容器最大允许分配资源
   */
  public abstract Resource getMaximumContainerAllocation();

  @Override
  public Resource getMaxShare() {
    // 根据集群总资源计算当前队列配置的最大资源
    Resource maxResource = maxShare.getResource(scheduler.getClusterResource());

    // 保证最大资源不小于最小资源，取两者分量最大值
    Resource result = Resources.componentwiseMax(maxResource, minShare);

    // 如果最大资源配置小于最小资源，输出警告日志
    if (!Resources.equals(maxResource, result)) {
      LOG.warn(String.format("Queue %s has max resources %s less than "
          + "min resources %s", getName(), maxResource, minShare));
    }
    return result;
  }

  public ConfigurableResource getRawMaxShare() {
    return maxShare;
  }

  /**
   * 从指标中获取最新预留资源并返回
   * @return 当前队列预留资源总和
   */
  public Resource getReservedResource() {
    reservedResource.setMemorySize(metrics.getReservedMB());
    reservedResource.setVirtualCores(metrics.getReservedVirtualCores());
    return reservedResource;
  }

  public void setMaxChildQueueResource(ConfigurableResource maxChildShare){
    this.maxChildQueueResource = maxChildShare;
  }

  public ConfigurableResource getMaxChildQueueResource() {
    return maxChildQueueResource;
  }

  public void setMaxRunningApps(int maxRunningApps){
    this.maxRunningApps = maxRunningApps;
  }

  public int getMaxRunningApps() {
    return maxRunningApps;
  }

  @VisibleForTesting
  public float getMaxAMShare() {
    return maxAMShare;
  }

  public void setMaxAMShare(float maxAMShare){
    this.maxAMShare = maxAMShare;
  }

  @Override
  public long getStartTime() {
    return 0;
  }

  @Override
  public Priority getPriority() {
    Priority p = recordFactory.newRecordInstance(Priority.class);
    p.setPriority(1);
    return p;
  }
  
  @Override
  public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
    // 创建队列信息实例
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    // 设置调度器类型为公平调度
    queueInfo.setSchedulerType("FairScheduler");
    queueInfo.setQueueName(getQueueName());

    // 计算队列容量占集群总资源比例
    if (scheduler.getClusterResource().getMemorySize() == 0) {
      queueInfo.setCapacity(0.0f);
    } else {
      queueInfo.setCapacity((float) getFairShare().getMemorySize() /
          scheduler.getClusterResource().getMemorySize());
    }

    // 计算当前队列已使用资源占公平份额的比例
    if (getFairShare().getMemorySize() == 0) {
      queueInfo.setCurrentCapacity(0.0f);
    } else {
      queueInfo.setCurrentCapacity((float) getResourceUsage().getMemorySize() /
          getFairShare().getMemorySize());
    }

    // 设置队列权重
    queueInfo.setWeight(getWeight());

    // 设置最小资源份额信息
    Resource minShareResource = getMinShare();
    queueInfo.setMinResourceVCore(minShareResource.getVirtualCores());
    queueInfo.setMinResourceMemory(minShareResource.getMemorySize());

    // 设置最大资源份额信息，不超过集群总资源
    Resource maxShareResource =
        Resources.componentwiseMin(getMaxShare(), scheduler.getClusterResource());
    queueInfo.setMaxResourceVCore(maxShareResource.getVirtualCores());
    queueInfo.setMaxResourceMemory(maxShareResource.getMemorySize());

    // 设置预留资源信息
    Resource newReservedResource = getReservedResource();
    queueInfo.setReservedResourceVCore(newReservedResource.getVirtualCores());
    queueInfo.setReservedResourceMemory(newReservedResource.getMemorySize());

    // 设置稳定公平份额信息
    Resource newSteadyFairShare = getSteadyFairShare();
    queueInfo.setSteadyFairShareVCore(newSteadyFairShare.getVirtualCores());
    queueInfo.setSteadyFairShareMemory(newSteadyFairShare.getMemorySize());

    // 设置最大运行应用数
    queueInfo.setMaxRunningApp(getMaxRunningApps());

    // 设置抢占是否禁用
    queueInfo.setPreemptionDisabled(isPreemptable());

    // 如果需要包含子队列，递归收集子队列信息
    ArrayList<QueueInfo> childQueueInfos = new ArrayList<>();
    if (includeChildQueues) {
      Collection<FSQueue> childQueues = getChildQueues();
      for (FSQueue child : childQueues) {
        childQueueInfos.add(child.getQueueInfo(recursive, recursive));
      }
    }
    // 设置子队列列表和队列状态
    queueInfo.setChildQueues(childQueueInfos);
    queueInfo.setQueueState(QueueState.RUNNING);
    // 设置队列统计信息
    queueInfo.setQueueStatistics(getQueueStatistics());
    return queueInfo;
  }

  /**
   * 组装并返回队列统计信息
   * @return 填充完成的队列统计对象
   */
  public QueueStatistics getQueueStatistics() {
    QueueStatistics stats =
        recordFactory.newRecordInstance(QueueStatistics.class);
    // 从指标系统拉取各类统计数据填充
    stats.setNumAppsSubmitted(getMetrics().getAppsSubmitted());
    stats.setNumAppsRunning(getMetrics().getAppsRunning());
    stats.setNumAppsPending(getMetrics().getAppsPending());
    stats.setNumAppsCompleted(getMetrics().getAppsCompleted());
    stats.setNumAppsKilled(getMetrics().getAppsKilled());
    stats.setNumAppsFailed(getMetrics().getAppsFailed());
    stats.setNumActiveUsers(getMetrics().getActiveUsers());
    stats.setAvailableMemoryMB(getMetrics().getAvailableMB());
    stats.setAllocatedMemoryMB(getMetrics().getAllocatedMB());
    stats.setPendingMemoryMB(getMetrics().getPendingMB());
    stats.setReservedMemoryMB(getMetrics().getReservedMB());
    stats.setAvailableVCores(getMetrics().getAvailableVirtualCores());
    stats.setAllocatedVCores(getMetrics().getAllocatedVirtualCores());
    stats.setPendingVCores(getMetrics().getPendingVirtualCores());
    stats.setReservedVCores(getMetrics().getReservedVirtualCores());
    stats.setAllocatedContainers(getMetrics().getAllocatedContainers());
    stats.setPendingContainers(getMetrics().getPendingContainers());
    stats.setReservedContainers(getMetrics().getReservedContainers());
    return stats;
  }
  
  @Override
  public FSQueueMetrics getMetrics() {
    return metrics;
  }

  /** Get the fair share assigned to this Schedulable. */
  public Resource getFairShare() {
    return fairShare;
  }

  @Override
  public void setFairShare(Resource fairShare) {
    this.fairShare = fairShare;
    metrics.setFairShare(fairShare);
    LOG.debug("The updated fairShare for {} is {}", getName(), fairShare);
  }

  /**
   * Get the steady fair share assigned to this Schedulable.
   * @return the steady fair share assigned to this Schedulable.
   */
  public Resource getSteadyFairShare() {
    return steadyFairShare;
  }

  void setSteadyFairShare(Resource steadyFairShare) {
    this.steadyFairShare = steadyFairShare;
    metrics.setSteadyFairShare(steadyFairShare);
  }

  /**
   * 检查用户对当前队列是否有指定ACL权限
   * @param acl 需要检查的队列权限
   * @param user 待检查用户
   * @return true 有权限，false 无权限
   */
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return authorizer.checkPermission(
        new AccessRequest(queueEntity, user,
            SchedulerUtils.toAccessType(acl), null, null,
            Server.getRemoteAddress(), null));
  }

  long getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }

  void setFairSharePreemptionTimeout(long fairSharePreemptionTimeout) {
    this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
  }

  long getMinSharePreemptionTimeout() {
    return minSharePreemptionTimeout;
  }

  void setMinSharePreemptionTimeout(long minSharePreemptionTimeout) {
    this.minSharePreemptionTimeout = minSharePreemptionTimeout;
  }

  float getFairSharePreemptionThreshold() {
    return fairSharePreemptionThreshold;
  }

  void setFairSharePreemptionThreshold(float fairSharePreemptionThreshold) {
    this.fairSharePreemptionThreshold = fairSharePreemptionThreshold;
  }

  @Override
  public boolean isPreemptable() {
    return preemptable;
  }

  /**
   * Recomputes the shares for all child queues and applications based on this
   * queue's current share.
   *
   * To be called holding the scheduler writelock.
   */
  abstract void updateInternal();

  /**
   * Set the queue's fairshare and update the demand/fairshare of child
   * queues/applications.
   *
   * To be called holding the scheduler writelock.
   *
   * @param fairShare queue's fairshare.
   */
  public void update(Resource fairShare) {
    setFairShare(fairShare);
    updateInternal();
  }

  /**
   * Update the min/fair share preemption timeouts, threshold and preemption
   * disabled flag for this queue.
   */
  private void updatePreemptionVariables() {
    // 获取最小份额抢占超时时间，如果队列未配置，继承父队列配置
    minSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getMinSharePreemptionTimeout(getName());
    if (minSharePreemptionTimeout == -1 && parent != null) {
      minSharePreemptionTimeout = parent.getMinSharePreemptionTimeout();
    }
    // 获取公平份额抢占超时时间，如果队列未配置，继承父队列配置
    fairSharePreemptionTimeout = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionTimeout(getName());
    if (fairSharePreemptionTimeout == -1 && parent != null) {
      fairSharePreemptionTimeout = parent.getFairSharePreemptionTimeout();
    }
    // 获取公平份额抢占阈值，如果队列未配置，继承父队列配置
    fairSharePreemptionThreshold = scheduler.getAllocationConfiguration()
        .getFairSharePreemptionThreshold(getName());
    if (fairSharePreemptionThreshold < 0 && parent != null) {
      fairSharePreemptionThreshold = parent.getFairSharePreemptionThreshold();
    }
    // 是否允许抢占，父队列不可