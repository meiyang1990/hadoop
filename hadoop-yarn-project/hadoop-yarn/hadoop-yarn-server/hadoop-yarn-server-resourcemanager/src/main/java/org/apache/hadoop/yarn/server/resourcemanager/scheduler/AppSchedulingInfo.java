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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.RejectionReason;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.DiagnosticsCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ApplicationSchedulingConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.PendingAskUpdateResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SingleConstraintAppPlacementAllocator;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 应用程序调度信息管理类，负责跟踪单个应用的资源消耗、已运行/完成容器以及所有待分配资源请求
 */
@Private
@Unstable
public class AppSchedulingInfo {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(AppSchedulingInfo.class);

  private final ApplicationId applicationId;
  private final ApplicationAttemptId applicationAttemptId;
  private final AtomicLong containerIdCounter;
  private final String user;

  private Queue queue;
  private AbstractUsersManager abstractUsersManager;
  // 标记应用是否处于待调度状态，尚未分配到容器
  private volatile boolean pending = true;
  private ResourceUsage appResourceUsage;

  private AtomicBoolean userBlacklistChanged = new AtomicBoolean(false);
  // 系统黑名单：存储AM容器分配失败的节点/机架，目前仅对AM容器生效
  private final Set<String> placesBlacklistedBySystem = new HashSet<>();
  private Set<String> placesBlacklistedByApp = new HashSet<>();

  private Set<String> requestedPartitions = new HashSet<>();

  private final ConcurrentSkipListSet<SchedulerRequestKey>
      schedulerKeys = new ConcurrentSkipListSet<>();
  private final Map<SchedulerRequestKey, AppPlacementAllocator<SchedulerNode>>
      schedulerKeyToAppPlacementAllocator = new ConcurrentHashMap<>();

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  public final ContainerUpdateContext updateContext;
  private final Map<String, String> applicationSchedulingEnvs = new HashMap<>();
  private final RMContext rmContext;
  private final int retryAttempts;
  private boolean unmanagedAM;

  private final String defaultResourceRequestAppPlacementType;

  /**
   * 构造应用调度信息实例，初始化各类资源和配置
   */
  public AppSchedulingInfo(ApplicationAttemptId appAttemptId, String user,
      Queue queue, AbstractUsersManager abstractUsersManager, long epoch,
      ResourceUsage appResourceUsage,
      Map<String, String> applicationSchedulingEnvs, RMContext rmContext,
      boolean unmanagedAM) {
    this.applicationAttemptId = appAttemptId;
    this.applicationId = appAttemptId.getApplicationId();
    this.queue = queue;
    this.user = user;
    this.abstractUsersManager = abstractUsersManager;
    this.containerIdCounter = new AtomicLong(
        epoch << ResourceManager.EPOCH_BIT_SHIFT);
    this.appResourceUsage = appResourceUsage;
    this.applicationSchedulingEnvs.putAll(applicationSchedulingEnvs);
    this.rmContext = rmContext;
    this.retryAttempts = rmContext.getYarnConfiguration().getInt(
         YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS,
         YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS);
    this.unmanagedAM = unmanagedAM;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    updateContext = new ContainerUpdateContext(this);
    readLock = lock.readLock();
    writeLock = lock.writeLock();

    this.defaultResourceRequestAppPlacementType =
        getDefaultResourceRequestAppPlacementType();
  }

  /**
   * 获取默认的应用容器分配器类型，优先从应用环境变量读取，其次读取全局配置
   * @return 默认应用分配器类名
   */
  public String getDefaultResourceRequestAppPlacementType() {
    if (this.rmContext != null
        && this.rmContext.getYarnConfiguration() != null) {

      String appPlacementClass = applicationSchedulingEnvs.get(
          ApplicationSchedulingConfig.ENV_APPLICATION_PLACEMENT_TYPE_CLASS);
      if (null != appPlacementClass) {
        return appPlacementClass;
      } else {
        Configuration conf = rmContext.getYarnConfiguration();
        return conf.get(
            YarnConfiguration.APPLICATION_PLACEMENT_TYPE_CLASS);
      }
    }
    return null;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public String getUser() {
    return user;
  }

  public long getNewContainerId() {
    return this.containerIdCounter.incrementAndGet();
  }

  /**
   * 获取当前应用所属队列名称，加读锁保证线程安全
   * @return 队列名称
   */
  public String getQueueName() {
    this.readLock.lock();
    try {
      return queue.getQueueName();
    } finally {
      this.readLock.unlock();
    }
  }

  public boolean isPending() {
    return pending;
  }

  public void setUnmanagedAM(boolean unmanagedAM) {
    this.unmanagedAM = unmanagedAM;
  }

  public boolean isUnmanagedAM() {
    return unmanagedAM;
  }

  public Set<String> getRequestedPartitions() {
    return requestedPartitions;
  }

  /**
   * 清空应用所有待处理的资源请求
   */
  private void clearRequests() {
    schedulerKeys.clear();
    schedulerKeyToAppPlacementAllocator.clear();
    LOG.info("Application " + applicationId + " requests cleared");
  }

  public ContainerUpdateContext getUpdateContext() {
    return updateContext;
  }

  /**
   * 更新应用资源请求，处理新增和释放的资源请求
   * @param resourceRequests 待分配的资源请求列表
   * @param recoverPreemptedRequestForAContainer 是否恢复被抢占的资源请求
   * @return 是否有ANY位置的资源请求更新
   */
  public boolean updateResourceRequests(List<ResourceRequest> resourceRequests,
      boolean recoverPreemptedRequestForAContainer) {
    // 标记是否更新了ANY位置的资源请求
    boolean offswitchResourcesUpdated;

    writeLock.lock();
    try {
      // 根据请求更新应用分配器
      offswitchResourcesUpdated = internalAddResourceRequests(
          recoverPreemptedRequestForAContainer, resourceRequests);
    } finally {
      writeLock.unlock();
    }

    return offswitchResourcesUpdated;
  }

  /**
   * 更新应用资源请求，使用已经去重后的请求数据
   * @param dedupRequests 已经去重的资源请求，按调度请求键和资源名称分组
   * @param recoverPreemptedRequestForAContainer 是否恢复被抢占的资源请求
   * @return 是否有ANY位置的资源请求更新
   */
  public boolean updateResourceRequests(
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests,
      boolean recoverPreemptedRequestForAContainer) {
    // 标记是否更新了ANY位置的资源请求
    boolean offswitchResourcesUpdated;

    writeLock.lock();
    try {
      // 根据请求更新应用分配器
      offswitchResourcesUpdated = internalAddResourceRequests(
          recoverPreemptedRequestForAContainer, dedupRequests);
    } finally {
      writeLock.unlock();
    }

    return offswitchResourcesUpdated;
  }

  /**
   * 更新调度请求，处理新增的调度请求
   * @param schedulingRequests 待分配的调度请求列表
   * @param recoverPreemptedRequestForAContainer 是否恢复被抢占的调度请求
   * @return 是否有ANY位置的资源请求更新
   */
  public boolean updateSchedulingRequests(
      List<SchedulingRequest> schedulingRequests,
      boolean recoverPreemptedRequestForAContainer) {
    // 标记是否更新了ANY位置的资源请求
    boolean offswitchResourcesUpdated;

    writeLock.lock();
    try {
      // 根据请求更新应用分配器
      offswitchResourcesUpdated = addSchedulingRequests(
          recoverPreemptedRequestForAContainer, schedulingRequests);
    } finally {
      writeLock.unlock();
    }

    return offswitchResourcesUpdated;
  }

  /**
   * 移除指定调度请求对应的分配器
   * @param schedulerRequestKey 调度请求键
   */
  public void removeAppPlacement(SchedulerRequestKey schedulerRequestKey) {
    schedulerKeyToAppPlacementAllocator.remove(schedulerRequestKey);
  }

  /**
   * 添加调度请求，更新待分配资源统计
   * @param recoverPreemptedRequestForAContainer 是否恢复被抢占的请求
   * @param schedulingRequests 调度请求列表
   * @return 是否需要更新待分配资源统计
   */
  private boolean addSchedulingRequests(
      boolean recoverPreemptedRequestForAContainer,
      List<SchedulingRequest> schedulingRequests) {
    // 标记是否需要更新应用/队列的待分配资源
    boolean requireUpdatePendingResource = false;

    for (SchedulingRequest request : schedulingRequests) {
      SchedulerRequestKey schedulerRequestKey = SchedulerRequestKey.create(
          request);

      AppPlacementAllocator appPlacementAllocator =
          getAndAddAppPlacementAllocatorIfNotExist(schedulerRequestKey,
              SingleConstraintAppPlacementAllocator.class.getCanonicalName());

      // 更新分配器中的待分配请求
      PendingAskUpdateResult pendingAmountChanges =
          appPlacementAllocator.updatePendingAsk(schedulerRequestKey,
              request, recoverPreemptedRequestForAContainer);

      if (null != pendingAmountChanges) {
        updatePendingResources(pendingAmountChanges, schedulerRequestKey,
            queue.getMetrics());
        requireUpdatePendingResource = true;
      }
    }

    return requireUpdatePendingResource;
  }

  /**
   * 如果分配器不存在则创建并添加，必须在写锁保护下调用
   * @param schedulerRequestKey 调度请求键
   * @param placementTypeClass 分配器类型类名
   * @return 应用容器分配器实例
   */
  private AppPlacementAllocator<SchedulerNode> getAndAddAppPlacementAllocatorIfNotExist(
      SchedulerRequestKey schedulerRequestKey, String placementTypeClass) {
    AppPlacementAllocator<SchedulerNode> appPlacementAllocator;
    if ((appPlacementAllocator = schedulerKeyToAppPlacementAllocator.get(
        schedulerRequestKey)) == null) {
      appPlacementAllocator =
          ApplicationPlacementAllocatorFactory.getAppPlacementAllocator(
              placementTypeClass, this, schedulerRequestKey, rmContext);
      schedulerKeyToAppPlacementAllocator.put(schedulerRequestKey,
          appPlacementAllocator);
    }
    return appPlacementAllocator;
  }

  /**
   * 内部处理已经分组去重的资源请求，更新待分配资源统计
   * @param recoverPreemptedRequestForAContainer 是否恢复被抢占的请求
   * @param dedupRequests 分组去重后的资源请求
   * @return 是否更新了ANY位置的资源请求
   */
  private boolean internalAddResourceRequests(
      boolean recoverPreemptedRequestForAContainer,
      Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests) {
    boolean offswitchResourcesUpdated = false;
    for (Map.Entry<SchedulerRequestKey, Map<String, ResourceRequest>> entry :
    dedupRequests.entrySet()) {
      SchedulerRequestKey schedulerRequestKey = entry.getKey();
      AppPlacementAllocator<SchedulerNode> appPlacementAllocator =
          getAndAddAppPlacementAllocatorIfNotExist(schedulerRequestKey,
              defaultResourceRequestAppPlacementType);

      // 更新分配器中的待分配请求
      PendingAskUpdateResult pendingAmountChanges =
          appPlacementAllocator.updatePendingAsk(entry.getValue().values(),
              recoverPreemptedRequestForAContainer);

      if (null != pendingAmountChanges) {
        updatePendingResources(pendingAmountChanges, schedulerRequestKey,
            queue.getMetrics());
        offswitchResourcesUpdated = true;
      }
    }
    return offswitchResourcesUpdated;
  }

  /**
   * 内部处理资源请求列表，先分组去重再处理
   * @param recoverPreemptedRequestForAContainer 是否恢复被抢占的请求
   * @param resourceRequests 原始资源请求列表
   * @return 是否更新了ANY位置的资源请求
   */
  private boolean internalAddResourceRequests(boolean recoverPreemptedRequestForAContainer,
      List<ResourceRequest> resourceRequests) {
    if (null == resourceRequests || resourceRequests.isEmpty()) {
      return false;
    }

    // 用于对资源请求分组去重
    Map<SchedulerRequestKey, Map<String, ResourceRequest>> dedupRequests =
        new HashMap<>();

    // 按调度请求键和资源名称分组
    for (ResourceRequest request : resourceRequests) {
      SchedulerRequestKey schedulerKey = SchedulerRequestKey.create(request);
      if (!dedupRequests.containsKey(schedulerKey)) {
        dedupRequests.put(schedulerKey, new HashMap<>());
      }
      dedupRequests.get(schedulerKey).put(request.getResourceName(), request);
    }

    return internalAddResourceRequests(recoverPreemptedRequestForAContainer,
        dedupRequests);
  }

  /**
   * 更新队列、应用和指标中的待分配资源统计
   * @param updateResult 待分配请求更新结果，包含变更前后信息
   * @param schedulerKey 调度请求键
   * @param metrics 队列指标
   */
  private void updatePendingResources(PendingAskUpdateResult updateResult,
      SchedulerRequestKey schedulerKey, QueueMetrics metrics) {

    PendingAsk lastPendingAsk = updateResult.getLastPendingAsk();
    PendingAsk newPendingAsk = updateResult.getNewPendingAsk();
    String lastNodePartition = updateResult.getLastNodePartition();
    String newNodePartition = updateResult.getNewNodePartition();

    int lastRequestContainers =
        (lastPendingAsk != null) ? lastPendingAsk.getCount() : 0;
    if (newPendingAsk.getCount() <= 0) {
      if (lastRequestContainers >= 0) {
        schedulerKeys.remove(schedulerKey);
        schedulerKeyToAppPlacementAllocator.remove(schedulerKey);
      }
      LOG.info("checking for deactivate of application :"
          + this.applicationId);
      checkForDeactivation();
    } else {
      // 激活应用，同时更新指标激活状态
      if (lastRequestContainers <= 0) {
        schedulerKeys.add(schedulerKey);
        abstractUsersManager.activateApplication(user, applicationId);
      }
    }

    if (lastPendingAsk != null) {
      // 从指标、队列和应用中扣减变更前的资源
      metrics.decrPendingResources(lastNodePartition, user,
          lastPendingAsk.getCount(), lastPendingAsk.getPerAllocationResource());
      Resource decreasedResource = Resources.multiply(
          lastPendingAsk.getPerAllocationResource(), lastRequestContainers);
      queue.decPendingResource(lastNodePartition,