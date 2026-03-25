// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .queuemanagement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractAutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueueConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedQueueManagementPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueManagementChange;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.CSQueueUtils.EPSILON;

/**
 * 自动创建叶子队列的容量管理策略
 * <p>
 * 按照应用提交顺序为叶子队列分配可用容量，即基于应用提交时间以FCFS顺序为叶子队列分配容量。
 * 当叶子队列下没有待处理或运行中的应用时，将该队列容量更新为0。
 */
public class GuaranteedOrZeroCapacityOverTimePolicy
    implements AutoCreatedQueueManagementPolicy {

  private static final int DEFAULT_QUEUE_PRINT_SIZE_LIMIT = 25;
  private ManagedParentQueue managedParentQueue;

  private static final Logger LOG =
      LoggerFactory.getLogger(GuaranteedOrZeroCapacityOverTimePolicy.class);

  private ReentrantReadWriteLock.WriteLock writeLock;

  private ReentrantReadWriteLock.ReadLock readLock;

  private ParentQueueState parentQueueState = new ParentQueueState();

  private AutoCreatedLeafQueueConfig leafQueueTemplate;

  private QueueCapacities leafQueueTemplateCapacities;

  private Set<String> leafQueueTemplateNodeLabels;

  private LeafQueueState leafQueueState = new LeafQueueState();

  private Clock clock = new MonotonicClock();

  /**
   * 存储所有叶子队列按分区划分的状态信息
   */
  private class LeafQueueState {

    // 分区 -> 队列名 -> 叶子队列状态 的映射
    private Map<String, Map<String, LeafQueueStatePerPartition>>
        leafQueueStateMap = new HashMap<>();

    public boolean containsLeafQueue(String leafQueueName, String partition) {
      if (leafQueueStateMap.containsKey(partition)) {
        return leafQueueStateMap.get(partition).containsKey(leafQueueName);
      }
      return false;
    }

    private boolean containsPartition(String partition) {
      if (leafQueueStateMap.containsKey(partition)) {
        return true;
      }
      return false;
    }

    private boolean addLeafQueueStateIfNotExists(String leafQueuePath,
        String partition, LeafQueueStatePerPartition leafQueueState) {
      if (!containsPartition(partition)) {
        leafQueueStateMap.put(partition, new HashMap<>());
      }
      if (!containsLeafQueue(leafQueuePath, partition)) {
        leafQueueStateMap.get(partition).put(leafQueuePath, leafQueueState);
        return true;
      }
      return false;
    }

    public boolean createLeafQueueStateIfNotExists(AbstractLeafQueue leafQueue,
        String partition) {
      return addLeafQueueStateIfNotExists(leafQueue.getQueuePath(), partition,
          new LeafQueueStatePerPartition());
    }

    public LeafQueueStatePerPartition getLeafQueueStatePerPartition(
        String leafQueuePath, String partition) {
      if (leafQueueStateMap.get(partition) != null) {
        return leafQueueStateMap.get(partition).get(leafQueuePath);
      }
      return null;
    }

    public Map<String, Map<String, LeafQueueStatePerPartition>>
    getLeafQueueStateMap() {
      return leafQueueStateMap;
    }

    private void clear() {
      leafQueueStateMap.clear();
    }
  }

  /**
   * 单个叶子队列单个分区的状态信息
   */
  private class LeafQueueStatePerPartition {

    private AtomicBoolean isActive = new AtomicBoolean(false);

    private long mostRecentActivationTime;

    private long mostRecentDeactivationTime;

    public long getMostRecentActivationTime() {
      return mostRecentActivationTime;
    }

    public long getMostRecentDeactivationTime() {
      return mostRecentDeactivationTime;
    }

    /**
     * 队列当前是否处于激活状态？
     *
     * @return true 表示激活，否则表示未激活
     */
    public boolean isActive() {
      return isActive.get();
    }

    private boolean activate() {
      boolean ret = isActive.compareAndSet(false, true);
      mostRecentActivationTime = clock.getTime();
      return ret;
    }

    private boolean deactivate() {
      boolean ret = isActive.compareAndSet(true, false);
      mostRecentDeactivationTime = clock.getTime();
      return ret;
    }
  }

  /**
   * 父队列的状态信息，按节点标签统计已激活子队列总容量
   */
  private class ParentQueueState {

    private Map<String, Float> totalAbsoluteActivatedChildQueueCapacityByLabel =
        new HashMap<String, Float>();

    private float getAbsoluteActivatedChildQueueCapacity(String nodeLabel) {
      readLock.lock();
      try {
        Float totalActivatedCapacity = getAbsActivatedChildQueueCapacityByLabel(
            nodeLabel);
        if (totalActivatedCapacity != null) {
          return totalActivatedCapacity;
        } else{
          return 0;
        }
      } finally {
        readLock.unlock();
      }
    }

    private void incAbsoluteActivatedChildCapacity(String nodeLabel,
        float childQueueCapacity) {
      writeLock.lock();
      try {
        Float activatedChildCapacity = getAbsActivatedChildQueueCapacityByLabel(
            nodeLabel);
        if (activatedChildCapacity != null) {
          setAbsActivatedChildQueueCapacityByLabel(nodeLabel,
              activatedChildCapacity + childQueueCapacity);
        } else{
          setAbsActivatedChildQueueCapacityByLabel(nodeLabel,
              childQueueCapacity);
        }
      } finally {
        writeLock.unlock();
      }
    }

    private void decAbsoluteActivatedChildCapacity(String nodeLabel,
        float childQueueCapacity) {
      writeLock.lock();
      try {
        Float activatedChildCapacity = getAbsActivatedChildQueueCapacityByLabel(
            nodeLabel);
        if (activatedChildCapacity != null) {
          setAbsActivatedChildQueueCapacityByLabel(nodeLabel,
              activatedChildCapacity - childQueueCapacity);
        } else{
          setAbsActivatedChildQueueCapacityByLabel(nodeLabel,
              childQueue);
        }
      } finally {
        writeLock.unlock();
      }
    }

    Float getAbsActivatedChildQueueCapacityByLabel(String label) {
      return totalAbsoluteActivatedChildQueueCapacityByLabel.get(label);
    }

    Float setAbsActivatedChildQueueCapacityByLabel(String label, float val) {
      return totalAbsoluteActivatedChildQueueCapacityByLabel.put(label, val);
    }

    void clear() {
      totalAbsoluteActivatedChildQueueCapacityByLabel.clear();
    }
  }

  @Override
  public void init(final AbstractParentQueue parentQueue) throws IOException {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    if (!(parentQueue instanceof ManagedParentQueue)) {
      throw new IllegalArgumentException(
          "Expected instance of type " + ManagedParentQueue.class);
    }

    this.managedParentQueue = (ManagedParentQueue) parentQueue;

    initializeLeafQueueTemplate(this.managedParentQueue);

    LOG.info(
        "Initialized queue management policy for parent queue " + parentQueue
            .getQueuePath() + " with leaf queue template capacities : ["
            + leafQueueTemplate.getQueueCapacities() + "]");
  }

  /**
   * 初始化叶子队列模板，验证节点标签合法性
   */
  private void initializeLeafQueueTemplate(ManagedParentQueue parentQueue)
      throws IOException {
    leafQueueTemplate = parentQueue.getLeafQueueTemplate();

    leafQueueTemplateCapacities = leafQueueTemplate.getQueueCapacities();

    Set<String> parentQueueLabels = parentQueue.getNodeLabelsForQueue();
    // 遍历模板配置的所有节点标签
    for (String nodeLabel : leafQueueTemplateCapacities
        .getExistingNodeLabels()) {
      // 检查父队列是否包含该标签，不包含则抛出异常
      if (!parentQueueLabels.contains(nodeLabel)) {
        LOG.error("Invalid node label " + nodeLabel
            + " on configured leaf template on parent" + " queue " + parentQueue
            .getQueuePath());
        throw new IOException("Invalid node label " + nodeLabel
            + " on configured leaf template on parent" + " queue " + parentQueue
            .getQueuePath());
      }
    }

    leafQueueTemplateNodeLabels =
        leafQueueTemplateCapacities.getExistingNodeLabels();

  }

  /**
   * 计算自动创建叶子队列的容量调整。本方法只计算队列权益，不更新叶子队列状态或队列容量。
   * 调度器会在验证通过后调用commitQueueManagementChanges提交变更，状态更新在commitQueueManagementChanges中完成。
   *
   * @return 队列管理变更建议列表，调度器可能因验证失败拒绝或回滚这些变更
   * @throws SchedulerDynamicEditException 当计算队列管理变更失败时抛出
   */
  @Override
  public List<QueueManagementChange> computeQueueManagementChanges()
      throws SchedulerDynamicEditException {

    // 更新模板绝对容量，因为权重模式下容量可能已发生变化
    updateTemplateAbsoluteCapacities(managedParentQueue.getQueueCapacities(),
        (GuaranteedOrZeroCapacityOverTimePolicy)
            managedParentQueue.getAutoCreatedQueueManagementPolicy());

    //TODO : Add support for node labels on leaf queue template configurations
    // 同步状态，添加缺失的叶子队列到状态中
    updateLeafQueueState();

    readLock.lock();
    try {
      LeafQueueEntitlements leafQueueEntitlements = new LeafQueueEntitlements();
      // 遍历所有节点标签分区
      for (String nodeLabel : leafQueueTemplateNodeLabels) {
        // 对当前分区，停用不需要的叶子队列
        DeactivatedLeafQueuesByLabel deactivatedLeafQueues =
            deactivateLeafQueues(nodeLabel, leafQueueEntitlements);
        deactivatedLeafQueues.printToDebug(LOG);

        // 检查是否还有空间可以激活新队列
        if (deactivatedLeafQueues.canActivateLeafQueues()) {
          activateLeafQueues(leafQueueEntitlements, nodeLabel, deactivatedLeafQueues);
        }
      }

      // 将计算好的权益转换为队列管理变更列表返回
      return leafQueueEntitlements.mapToQueueManagementChanges((leafQueueName, capacities) -> {
        AutoCreatedLeafQueue leafQueue =
            (AutoCreatedLeafQueue) managedParentQueue.getQueueContext().getQueueManager()
                .getQueue(leafQueueName);
        AutoCreatedLeafQueueConfig newTemplate = buildTemplate(capacities);
        return new QueueManagementChange.UpdateQueue(leafQueue, newTemplate);
      });
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 激活等待的叶子队列，按应用提交顺序激活，直到父队列容量用尽
   */
  private void activateLeafQueues(LeafQueueEntitlements leafQueueEntitlements, String nodeLabel,
      DeactivatedLeafQueuesByLabel deactivatedLeafQueues) throws SchedulerDynamicEditException {
    // 按提交时间对所有待处理应用排序
    List<FiCaSchedulerApp> pendingApps = getSortedPendingApplications();
    if (pendingApps.size() > 0) {
      // 计算还能激活多少个队列
      int maxLeafQueuesTobeActivated = deactivatedLeafQueues.
          getMaxLeavesToBeActivated(pendingApps.size());

      if (LOG.isDebugEnabled()) {
        LOG.debug("Parent queue = {}, Found {} leaf queues to be activated with {} aps",
            managedParentQueue.getQueuePath(), maxLeafQueuesTobeActivated, pendingApps.size());
      }

      // 获取按提交时间排序的需要激活的叶子队列列表
      Set<String> leafQueuesToBeActivated = getSortedLeafQueues(
          nodeLabel, pendingApps, maxLeafQueuesTobeActivated,
          deactivatedLeafQueues.getQueues());

      // 为选中的叶子队列更新容量，添加到变更结果中
      updateLeafQueueCapacitiesByLabel(nodeLabel, leafQueuesToBeActivated, leafQueueEntitlements);

      if (LOG.isDebugEnabled() && leafQueuesToBeActivated.size() > 0) {
        LOG.debug("Activated leaf queues : [{}]",
            getListContentsUpToLimit(leafQueuesToBeActivated));
      }
    }
  }

  private Object getListContentsUpToLimit(Set<String> leafQueuesToBeActivated) {
    return leafQueuesToBeActivated.size() < DEFAULT_QUEUE_PRINT_SIZE_LIMIT ?
        leafQueuesToBeActivated : leafQueuesToBeActivated.size();
  }

  private Object getMapUpToLimit(Map<String, QueueCapacities> deactivatedLeafQueues) {
    return deactivatedLeafQueues.size() > DEFAULT_QUEUE_PRINT_SIZE_LIMIT ?
        deactivatedLeafQueues.size() : deactivatedLeafQueues;
  }

  /**
   * 停用当前分区中不需要激活的叶子队列
   */
  private DeactivatedLeafQueuesByLabel deactivateLeafQueues(String nodeLabel,
      LeafQueueEntitlements leafQueueEntitlements) throws SchedulerDynamicEditException {
    // 获取父队列当前分区的绝对容量
    float parentAbsoluteCapacity =
        managedParentQueue.getQueueCapacities().getAbsoluteCapacity(nodeLabel);
    // 获取叶子模板当前分区的绝对容量
    float leafQueueTemplateAbsoluteCapacity =
        leafQueueTemplateCapacities.getAbsoluteCapacity(nodeLabel);
    // 遍历检查所有叶子队列，停用无待处理应用的队列
    Map<String, QueueCapacities> deactivatedLeafQueues =
        deactivateLeafQueuesIfInActive(managedParentQueue, nodeLabel, leafQueueEntitlements);

    if (LOG.isDebugEnabled() && deactivatedLeafQueues.size() > 0) {
      LOG.debug("Parent queue = {}, nodeLabel = {}, deactivated leaf queues = [{}] ",
          managedParentQueue.getQueuePath(), nodeLabel,
          getMapUpToLimit(deactivatedLeafQueues));
    }

    // 封装停用结果，包含剩余可用容量信息
    return new DeactivatedLeafQueuesByLabel(deactivatedLeafQueues,
        managedParentQueue.getQueuePath(),
        nodeLabel,
        parentQueueState.getAbsoluteActivatedChildQueueCapacity(nodeLabel),
        parentAbsoluteCapacity,
        leafQueueTemplateAbsoluteCapacity);
  }

  private void updateTemplateAbsoluteCapacities(QueueCapacities parentQueueCapacities,
                                                GuaranteedOrZeroCapacityOverTimePolicy policy) {
    writeLock.lock();
    try {
      // 根据父队列容量重新计算叶子模板的绝对容量
      CSQueueUtils.updateAbsoluteCapacitiesByNodeLabels(
          policy.leafQueueTemplate.getQueueCapacities(),
          parentQueueCapacities, policy.leafQueueTemplateNodeLabels,
          managedParentQueue.getQueueContext().getConfiguration().isLegacyQueueMode());
      policy.leafQueueTemplateCapacities =
          policy.leafQueueTemplate.getQueueCapacities();
    } finally {
      writeLock.unlock();
    }
  }

  public void updateTemplateAbsolute