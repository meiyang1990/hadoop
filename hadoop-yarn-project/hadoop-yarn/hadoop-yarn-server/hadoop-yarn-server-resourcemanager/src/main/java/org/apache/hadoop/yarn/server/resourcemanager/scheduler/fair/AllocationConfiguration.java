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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.QueueProperties;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 公平调度器的分配配置类，存储从分配文件解析得到的所有队列调度配置信息
 * 继承ReservationSchedulerConfiguration，支持预留资源调度配置
 */
public class AllocationConfiguration extends ReservationSchedulerConfiguration {
  // 预定义ACL：允许所有用户访问
  private static final AccessControlList EVERYBODY_ACL = new AccessControlList("*");
  // 预定义ACL：禁止所有用户访问
  private static final AccessControlList NOBODY_ACL = new AccessControlList(" ");
  // 每个队列的最小资源保证，key为队列名
  private final Map<String, Resource> minQueueResources;
  // 每个队列的最大资源限制，key为队列名
  @VisibleForTesting
  final Map<String, ConfigurableResource> maxQueueResources;
  // 每个队列所有子队列的累计最大资源限制，key为父队列名
  private final Map<String, ConfigurableResource> maxChildQueueResources;
  // 每个队列的共享权重，key为队列名
  private final Map<String, Float> queueWeights;

  // 每个队列的最大同时运行应用数；每个用户的最大同时运行应用数；未配置用户使用默认值userMaxJobsDefault
  @VisibleForTesting
  final Map<String, Integer> queueMaxApps;
  @VisibleForTesting
  final Map<String, Integer> userMaxApps;
  // 用户默认最大同时运行应用数
  private final int userMaxAppsDefault;
  // 队列默认最大同时运行应用数
  private final int queueMaxAppsDefault;
  // 队列默认最大资源限制
  private final ConfigurableResource queueMaxResourcesDefault;

  // 每个叶子队列可用于运行ApplicationMaster的最大资源占比
  final Map<String, Float> queueMaxAMShares;
  // 队列AM资源占比默认值
  private final float queueMaxAMShareDefault;

  // 每个队列的访问控制列表，仅存储非默认配置
  private final Map<String, Map<AccessType, AccessControlList>> queueAcls;

  // 每个队列的预留访问控制列表，仅存储非默认配置
  private final Map<String, Map<ReservationACL, AccessControlList>> resAcls;

  // 每个队列最小共享抢占超时时间（秒），超时未获得最小保证资源允许抢占其他任务
  private final Map<String, Long> minSharePreemptionTimeouts;

  // 每个队列公平共享抢占超时时间（秒），超时未获得公平共享阈值允许抢占其他任务
  private final Map<String, Long> fairSharePreemptionTimeouts;

  // 每个队列公平共享抢占阈值，只有当获得资源小于 公平共享*阈值 才允许抢占
  private final Map<String, Float> fairSharePreemptionThresholds;

  // 支持资源预留的队列集合
  private final Set<String> reservableQueues;

  // 每个队列配置的调度策略
  private final Map<String, SchedulingPolicy> schedulingPolicies;

  // 默认调度策略
  private final SchedulingPolicy defaultSchedulingPolicy;

  // 每个队列允许的单容器最大资源分配，key为队列名
  private final Map<String, Resource> queueMaxContainerAllocationMap;

  // 分配文件中配置的队列，按队列类型分类存储
  @VisibleForTesting
  Map<FSQueueType, Set<String>> configuredQueues;

  // 全局预留队列配置
  private ReservationQueueConfiguration globalReservationQueueConfig;

  // 不可抢占队列集合
  private final Set<String> nonPreemptableQueues;

  /**
   * 从解析后的队列属性构造完整的分配配置对象
   * @param queueProperties 从配置解析得到的队列属性集合
   * @param allocationFileParser 分配文件解析器，包含全局默认配置
   * @param globalReservationQueueConfig 全局预留队列配置
   * @throws AllocationConfigurationException 配置解析错误时抛出
   */
  public AllocationConfiguration(QueueProperties queueProperties,
      AllocationFileParser allocationFileParser,
      ReservationQueueConfiguration globalReservationQueueConfig)
      throws AllocationConfigurationException {
    this.minQueueResources = queueProperties.getMinQueueResources();
    this.maxQueueResources = queueProperties.getMaxQueueResources();
    this.maxChildQueueResources = queueProperties.getMaxChildQueueResources();
    this.queueMaxApps = queueProperties.getQueueMaxApps();
    this.userMaxApps = allocationFileParser.getUserMaxApps();
    this.queueMaxAMShares = queueProperties.getQueueMaxAMShares();
    this.queueWeights = queueProperties.getQueueWeights();
    this.userMaxAppsDefault = allocationFileParser.getUserMaxAppsDefault();
    this.queueMaxResourcesDefault =
            allocationFileParser.getQueueMaxResourcesDefault();
    this.queueMaxAppsDefault = allocationFileParser.getQueueMaxAppsDefault();
    this.queueMaxAMShareDefault =
        allocationFileParser.getQueueMaxAMShareDefault();
    this.defaultSchedulingPolicy =
        allocationFileParser.getDefaultSchedulingPolicy();
    this.schedulingPolicies = queueProperties.getQueuePolicies();
    this.minSharePreemptionTimeouts =
        queueProperties.getMinSharePreemptionTimeouts();
    this.fairSharePreemptionTimeouts =
        queueProperties.getFairSharePreemptionTimeouts();
    this.fairSharePreemptionThresholds =
        queueProperties.getFairSharePreemptionThresholds();
    this.queueAcls = queueProperties.getQueueAcls();
    this.resAcls = queueProperties.getReservationAcls();
    this.reservableQueues = queueProperties.getReservableQueues();
    this.globalReservationQueueConfig = globalReservationQueueConfig;
    this.configuredQueues = queueProperties.getConfiguredQueues();
    this.nonPreemptableQueues = queueProperties.getNonPreemptableQueues();
    this.queueMaxContainerAllocationMap =
        queueProperties.getMaxContainerAllocation();
  }

  /**
   * 构造仅包含默认值的基础调度配置，仅用于调度器初始化阶段
   * @param scheduler 公平调度器实例，用于初始化队列放置策略
   */
  public AllocationConfiguration(FairScheduler scheduler) {
    minQueueResources = new HashMap<>();
    maxChildQueueResources = new HashMap<>();
    maxQueueResources = new HashMap<>();
    queueWeights = new HashMap<>();
    queueMaxApps = new HashMap<>();
    userMaxApps = new HashMap<>();
    queueMaxAMShares = new HashMap<>();
    userMaxAppsDefault = Integer.MAX_VALUE;
    queueMaxAppsDefault = Integer.MAX_VALUE;
    queueMaxResourcesDefault = new ConfigurableResource(Resources.unbounded());
    queueMaxAMShareDefault = 0.5f;
    queueAcls = new HashMap<>();
    resAcls = new HashMap<>();
    minSharePreemptionTimeouts = new HashMap<>();
    fairSharePreemptionTimeouts = new HashMap<>();
    fairSharePreemptionThresholds = new HashMap<>();
    schedulingPolicies = new HashMap<>();
    defaultSchedulingPolicy = SchedulingPolicy.DEFAULT_POLICY;
    reservableQueues = new HashSet<>();
    configuredQueues = new HashMap<>();
    // 初始化各类型队列配置集合
    for (FSQueueType queueType : FSQueueType.values()) {
      configuredQueues.put(queueType, new HashSet<>());
    }
    // 初始化队列放置策略
    QueuePlacementPolicy.fromConfiguration(scheduler);
    nonPreemptableQueues = new HashSet<>();
    queueMaxContainerAllocationMap = new HashMap<>();
  }

  /**
   * 获取所有队列的ACL映射表
   * @return 不可修改的ACL映射表
   */
  public Map<String, Map<AccessType, AccessControlList>> getQueueAcls() {
    return Collections.unmodifiableMap(this.queueAcls);
  }

  @Override
  /**
   * 获取指定队列的预留ACL映射表
   */
  public Map<ReservationACL, AccessControlList> getReservationAcls(QueuePath
        queue) {
    return this.resAcls.get(queue.getFullPath());
  }

  /**
   * 获取指定队列的最小共享抢占超时时间（毫秒），未配置返回-1
   * @param queueName 队列名称
   * @return 超时时间，未配置返回-1
   */
  public long getMinSharePreemptionTimeout(String queueName) {
    Long minSharePreemptionTimeout = minSharePreemptionTimeouts.get(queueName);
    return (minSharePreemptionTimeout == null) ? -1 : minSharePreemptionTimeout;
  }

  /**
   * 获取指定队列的公平共享抢占超时时间（毫秒），未配置返回-1
   * @param queueName 队列名称
   * @return 超时时间，未配置返回-1
   */
  public long getFairSharePreemptionTimeout(String queueName) {
    Long fairSharePreemptionTimeout = fairSharePreemptionTimeouts.get(queueName);
    return (fairSharePreemptionTimeout == null) ?
        -1 : fairSharePreemptionTimeout;
  }

  /**
   * 获取指定队列的公平共享抢占阈值，未配置返回-1f
   * @param queueName 队列名称
   * @return 抢占阈值，未配置返回-1f
   */
  public float getFairSharePreemptionThreshold(String queueName) {
    Float fairSharePreemptionThreshold =
        fairSharePreemptionThresholds.get(queueName);
    return (fairSharePreemptionThreshold == null) ?
        -1f : fairSharePreemptionThreshold;
  }

  /**
   * 判断指定队列是否允许被抢占
   * @param queueName 队列名称
   * @return true表示允许抢占，false表示不可抢占
   */
  public boolean isPreemptable(String queueName) {
    return !nonPreemptableQueues.contains(queueName);
  }

  /**
   * 获取指定队列的调度权重，未配置返回默认值1.0f
   * @param queue 队列名称
   * @return 队列权重
   */
  private float getQueueWeight(String queue) {
    Float weight = queueWeights.get(queue);
    return (weight == null) ? 1.0f : weight;
  }

  /**
   * 获取指定用户的最大同时运行应用数，未配置返回默认值
   * @param user 用户名
   * @return 最大同时运行应用数
   */
  public int getUserMaxApps(String user) {
    Integer maxApps = userMaxApps.get(user);
    return (maxApps == null) ? userMaxAppsDefault : maxApps;
  }

  /**
   * 获取所有用户的最大应用数配置
   * @return 用户最大应用数映射表
   */
  public Map<String, Integer> getUserMaxApps() {
    return userMaxApps;
  }

  /**
   * 获取指定队列的最大同时运行应用数，未配置返回默认值
   * @param queue 队列名称
   * @return 最大同时运行应用数
   */
  @VisibleForTesting
  int getQueueMaxApps(String queue) {
    Integer maxApps = queueMaxApps.get(queue);
    return (maxApps == null) ? queueMaxAppsDefault : maxApps;
  }

  /**
   * 获取队列最大同时运行应用数默认值
   * @return 默认值
   */
  public int getQueueMaxAppsDefault() {
    return queueMaxAppsDefault;
  }

  /**
   * 获取用户最大同时运行应用数默认值
   * @return 默认值
   */
  public int getUserMaxAppsDefault() {
    return userMaxAppsDefault;
  }

  /**
   * 获取指定队列的AM最大资源占比，未配置返回默认值
   * @param queue 队列名称
   * @return AM最大资源占比
   */
  @VisibleForTesting
  float getQueueMaxAMShare(String queue) {
    Float maxAMShare = queueMaxAMShares.get(queue);
    return (maxAMShare == null) ? queueMaxAMShareDefault : maxAMShare;
  }

  /**
   * 获取队列AM最大资源占比默认值
   * @return 默认值
   */
  public float getQueueMaxAMShareDefault() {
    return queueMaxAMShareDefault;
  }

  /**
   * 获取指定队列的最小资源保证，未配置返回Resources.none()
   * @param queue 队列名称
   * @return 最小保证资源
   */
  @VisibleForTesting
  Resource getMinResources(String queue) {
    Resource minQueueResource = minQueueResources.get(queue);
    return (minQueueResource == null) ? Resources.none() : minQueueResource;
  }

  /**
   * 获取指定队列的最大资源限制，未配置返回默认值
   * @param queue 队列名称
   * @return 最大资源限制
   */
  @VisibleForTesting
  ConfigurableResource getMaxResources(String queue) {
    ConfigurableResource maxQueueResource = maxQueueResources.get(queue);
    if (maxQueueResource == null) {
      maxQueueResource = queueMaxResourcesDefault;
    }
    return maxQueueResource;
  }

  /**
   * 获取指定队列的单容器最大资源分配，未配置返回无限资源
   * @param queue 队列名称
   * @return 单容器最大资源
   */
  @VisibleForTesting
  Resource getQueueMaxContainerAllocation(String queue) {
    Resource resource = queueMaxContainerAllocationMap.get(queue);
    return resource == null ? Resources.unbounded() : resource;
  }

  /**
   * 获取指定队列所有子队列的累计最大资源限制
   * @param queue 父队列名称
   * @return 子队列累计最大资源，未配置返回null
   */
  @VisibleForTesting
  ConfigurableResource getMaxChildResources(String queue) {
    return maxChildQueueResources.get(queue);
  }

  /**
   * 获取指定队列配置的调度策略，未配置返回默认调度策略
   * @param queueName 队列名称
   * @return 调度策略实例
   */
  @VisibleForTesting
  SchedulingPolicy getSchedulingPolicy(String queueName) {
    SchedulingPolicy policy = schedulingPolicies.get(queueName);
    return (policy == null) ? defaultSchedulingPolicy : policy;
  }

  /**
   * 获取默认调度策略
   * @return 默认调度策略实例
   */
  public SchedulingPolicy getDefaultSchedulingPolicy() {
    return defaultSchedulingPolicy;
  }

  /**
   * 获取按类型分类的已配置队列集合
   * @return 已配置队列映射表
   */
  public Map<FSQueueType, Set<String>> getConfiguredQueues() {
    return configuredQueues;
  }

  @Override
  /**
   * 判断指定队列是否支持资源预留
   */
  public boolean isReservable(QueuePath queue) {
    return reservableQueues.contains(queue.getFullPath());
  }

  @Override
  /**
   * 获取指定队列的预留窗口大小
   */
  public long getReservationWindow(QueuePath queue) {
    return globalReservationQueueConfig.getReservationWindowMsec();
  }

  @Override
  /**
   * 获取指定队列的平均容量百分比
   */
  public float getAverageCapacity(QueuePath queue) {
    return globalReservationQueueConfig.getAvgOverTimeMultiplier() * 100;
  }

  @Override
  /**
   * 获取指定队列的瞬时最大容量百分比
   */
  public float getInstantaneousMaxCapacity(QueuePath queue) {
    return globalReservationQueueConfig.getMaxOverTimeMultiplier() * 100;
  }

  @Override
  /**
   * 获取指定队列的预留准入策略名称
   */
  public String getReservationAdmissionPolicy(QueuePath queue) {
    return globalReservationQueueConfig.getReservationAdmissionPolicy();
  }

  @Override
  /**
   * 获取指定队列的预留代理名称
   */
  public String getReservationAgent(QueuePath queue) {
    return globalReservationQueueConfig.getReservationAgent();
  }

  @Override
  /**
   * 判断是否需要将预留显示为独立队列
   */
  public boolean getShowReservationAsQueues(QueuePath queue) {
    return