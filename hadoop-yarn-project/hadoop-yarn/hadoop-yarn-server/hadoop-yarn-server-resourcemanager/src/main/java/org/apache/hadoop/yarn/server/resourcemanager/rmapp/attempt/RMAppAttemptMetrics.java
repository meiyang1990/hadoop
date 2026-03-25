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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN RM应用尝试实例的指标统计类，负责收集和管理应用尝试的资源抢占、资源使用、数据局部性等运行指标。
 */
public class RMAppAttemptMetrics {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMAppAttemptMetrics.class);

  private ApplicationAttemptId attemptId = null;
  // 抢占资源信息
  private Resource resourcePreempted = Resource.newInstance(0, 0);
  // 应用剩余可用资源量
  private volatile Resource applicationHeadroom = Resource.newInstance(0, 0);
  private AtomicInteger numNonAMContainersPreempted = new AtomicInteger(0);
  private AtomicBoolean isPreempted = new AtomicBoolean(false);
  
  private ReadLock readLock;
  private WriteLock writeLock;
  // 资源使用累计统计（单位：资源秒）
  private Map<String, AtomicLong> resourceUsageMap = new ConcurrentHashMap<>();
  // 被抢占资源累计统计（单位：资源秒）
  private Map<String, AtomicLong> preemptedResourceMap = new ConcurrentHashMap<>();
  private RMContext rmContext;

  // 数据局部性统计：第一维是实际分配节点类型，第二维是请求节点类型，值为对应容器数量
  private int[][] localityStatistics =
      new int[NodeType.values().length][NodeType.values().length];
  private volatile int totalAllocatedContainers;

  /**
   * 构造应用尝试指标实例。
   * @param attemptId 应用尝试ID
   * @param rmContext RM上下文对象
   */
  public RMAppAttemptMetrics(ApplicationAttemptId attemptId,
      RMContext rmContext) {
    this.attemptId = attemptId;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    this.rmContext = rmContext;
  }

  /**
   * 更新资源抢占信息，统计被抢占的资源和容器数量。
   * @param resource 被抢占的资源
   * @param container 被抢占的容器
   */
  public void updatePreemptionInfo(Resource resource, RMContainer container) {
    // 获取写锁保护共享资源
    writeLock.lock();
    try {
      // 累加被抢占资源总量
      resourcePreempted = Resources.addTo(resourcePreempted, resource);
    } finally {
      // 确保释放写锁
      writeLock.unlock();
    }

    // 区分AM容器和普通容器分别统计
    if (!container.isAMContainer()) {
      // 被抢占的是非AM容器
      LOG.info(String.format(
        "Non-AM container preempted, current appAttemptId=%s, "
            + "containerId=%s, resource=%s", attemptId,
        container.getContainerId(), resource));
      numNonAMContainersPreempted.incrementAndGet();
    } else {
      // 被抢占的是AM容器
      LOG.info(String.format("AM container preempted, "
          + "current appAttemptId=%s, containerId=%s, resource=%s", attemptId,
        container.getContainerId(), resource));
      isPreempted.set(true);
    }
  }
  
  /**
   * 获取当前应用尝试被抢占的总资源量。
   * @return 被抢占资源对象
   */
  public Resource getResourcePreempted() {
    readLock.lock();
    try {
      return Resource.newInstance(resourcePreempted);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取被抢占内存总量。
   * @return 被抢占内存大小（MB）
   */
  public long getPreemptedMemory() {
    return preemptedResourceMap.get(ResourceInformation.MEMORY_MB.getName())
        .get();
  }

  /**
   * 获取被抢占虚拟CPU核数总量。
   * @return 被抢占虚拟CPU核数
   */
  public long getPreemptedVcore() {
    return preemptedResourceMap.get(ResourceInformation.VCORES.getName()).get();
  }

  /**
   * 获取所有被抢占资源的累计资源秒统计。
   * @return 按资源类型分组的累计资源秒映射
   */
  public Map<String, Long> getPreemptedResourceSecondsMap() {
    return convertAtomicLongMaptoLongMap(preemptedResourceMap);
  }

  /**
   * 获取被抢占的非AM容器数量。
   * @return 被抢占非AM容器数
   */
  public int getNumNonAMContainersPreempted() {
    return numNonAMContainersPreempted.get();
  }
  
  /**
   * 标记当前应用尝试已被抢占。
   */
  public void setIsPreempted() {
    this.isPreempted.set(true);
  }
  
  /**
   * 获取当前应用尝试是否已发生AM抢占。
   * @return true表示AM已被抢占，false表示未被抢占
   */
  public boolean getIsPreempted() {
    return this.isPreempted.get();
  }

  /**
   * 获取应用尝试的累计资源使用统计，包含已完成容器和当前运行容器。
   * @return 聚合后的资源使用信息
   */
  public AggregateAppResourceUsage getAggregateAppResourceUsage() {
    // 获取已完成容器的累计资源使用
    Map<String, Long> resourcesUsed =
        convertAtomicLongMaptoLongMap(resourceUsageMap);

    // 如果是当前活跃的应用尝试，需要累加正在运行容器的资源使用
    RMApp rmApp = rmContext.getRMApps().get(attemptId.getApplicationId());
    if (rmApp != null) {
      RMAppAttempt currentAttempt = rmApp.getCurrentAppAttempt();
      if (currentAttempt != null
          && currentAttempt.getAppAttemptId().equals(attemptId)) {
        // 从调度器获取当前运行容器的资源使用报告
        ApplicationResourceUsageReport appResUsageReport =
            rmContext.getScheduler().getAppResourceUsageReport(attemptId);
        if (appResUsageReport != null) {
          Map<String, Long> tmp = appResUsageReport.getResourceSecondsMap();
          // 累加运行容器资源到总统计中
          for (Map.Entry<String, Long> entry : tmp.entrySet()) {
            Long value = resourcesUsed.get(entry.getKey());
            if (value != null) {
              value += entry.getValue();
            } else {
              value = entry.getValue();
            }
            resourcesUsed.put(entry.getKey(), value);
          }
        }
      }
    }
    return new AggregateAppResourceUsage(resourcesUsed);
  }

  /**
   * 更新累计资源使用统计。
   * @param allocated 本次更新的资源量
   * @param deltaUsedMillis 新增的使用时长（毫秒）
   */
  public void updateAggregateAppResourceUsage(Resource allocated,
      long deltaUsedMillis) {
    updateUsageMap(allocated, deltaUsedMillis, resourceUsageMap);
  }

  /**
   * 更新累计被抢占资源统计。
   * @param allocated 本次更新的被抢占资源量
   * @param deltaUsedMillis 新增的被抢占时长（毫秒）
   */
  public void updateAggregatePreemptedAppResourceUsage(Resource allocated,
      long deltaUsedMillis) {
    updateUsageMap(allocated, deltaUsedMillis, preemptedResourceMap);
  }

  /**
   * 直接更新累计资源使用统计，覆盖原有值。
   * @param resourceSecondsMap 新的资源秒统计映射
   */
  public void updateAggregateAppResourceUsage(
      Map<String, Long> resourceSecondsMap) {
    updateUsageMap(resourceSecondsMap, resourceUsageMap);
  }

  /**
   * 直接更新累计被抢占资源统计，覆盖原有值。
   * @param preemptedResourceSecondsMap 新的被抢占资源秒统计映射
   */
  public void updateAggregatePreemptedAppResourceUsage(
      Map<String, Long> preemptedResourceSecondsMap) {
    updateUsageMap(preemptedResourceSecondsMap, preemptedResourceMap);
  }

  /**
   * 根据资源和使用时长更新目标统计映射，将毫秒转换为秒计算资源秒。
   * @param allocated 分配的资源
   * @param deltaUsedMillis 新增使用时长（毫秒）
   * @param targetMap 目标统计映射
   */
  private void updateUsageMap(Resource allocated, long deltaUsedMillis,
      Map<String, AtomicLong> targetMap) {
    // 遍历所有资源类型
    for (ResourceInformation entry : allocated.getResources()) {
      AtomicLong resourceUsed;
      // 如果不存在当前资源类型则初始化
      if (!targetMap.containsKey(entry.getName())) {
        resourceUsed = new AtomicLong(0);
        targetMap.put(entry.getName(), resourceUsed);

      }
      resourceUsed = targetMap.get(entry.getName());
      // 计算累计资源秒：资源量 × 毫秒数 / 毫秒每秒，累加进统计
      resourceUsed.addAndGet((entry.getValue() * deltaUsedMillis)
          / DateUtils.MILLIS_PER_SECOND);
    }
  }

  /**
   * 将源统计映射的值批量更新到目标映射中。
   * @param sourceMap 源统计映射
   * @param targetMap 目标统计映射
   */
  private void updateUsageMap(Map<String, Long> sourceMap,
      Map<String, AtomicLong> targetMap) {
    for (Map.Entry<String, Long> entry : sourceMap.entrySet()) {
      AtomicLong resourceUsed;
      // 如果不存在当前资源类型则初始化
      if (!targetMap.containsKey(entry.getKey())) {
        resourceUsed = new AtomicLong(0);
        targetMap.put(entry.getKey(), resourceUsed);

      }
      resourceUsed = targetMap.get(entry.getKey());
      // 直接设置为源值，覆盖原有统计
      resourceUsed.set(entry.getValue());
    }
  }

  /**
   * 将AtomicLong类型的映射转换为Long类型映射，便于输出。
   * @param source 源AtomicLong映射
   * @return 转换后的Long映射
   */
  private Map<String, Long> convertAtomicLongMaptoLongMap(
      Map<String, AtomicLong> source) {
    Map<String, Long> ret = new HashMap<>();
    for (Map.Entry<String, AtomicLong> entry : source.entrySet()) {
      ret.put(entry.getKey(), entry.getValue().get());
    }
    return ret;
  }

  /**
   * 增加容器分配的局部性统计计数。
   * @param containerType 实际分配容器所在节点类型
   * @param requestType 请求期望的节点类型
   */
  public void incNumAllocatedContainers(NodeType containerType,
      NodeType requestType) {
    localityStatistics[containerType.getIndex()][requestType.getIndex()]++;
    totalAllocatedContainers++;
  }

  /**
   * 获取完整的容器分配局部性统计数组。
   * @return 局部性统计二维数组
   */
  public int[][] getLocalityStatistics() {
    return this.localityStatistics;
  }

  /**
   * 获取当前应用尝试已分配的容器总数。
   * @return 已分配容器总数
   */
  public int getTotalAllocatedContainers() {
    return this.totalAllocatedContainers;
  }

  /**
   * 设置当前应用尝试已分配的容器总数。
   * @param totalAllocatedContainers 容器总数
   */
  public void setTotalAllocatedContainers(int totalAllocatedContainers) {
    this.totalAllocatedContainers = totalAllocatedContainers;
  }

  /**
   * 获取当前应用尝试的剩余可用资源量（头room）。
   * @return 剩余可用资源对象
   */
  public Resource getApplicationAttemptHeadroom() {
    return Resource.newInstance(applicationHeadroom);
  }

  /**
   * 设置当前应用尝试的剩余可用资源量。
   * @param headRoom 剩余可用资源对象
   */
  public void setApplicationAttemptHeadRoom(Resource headRoom) {
    this.applicationHeadroom = headRoom;
  }
}