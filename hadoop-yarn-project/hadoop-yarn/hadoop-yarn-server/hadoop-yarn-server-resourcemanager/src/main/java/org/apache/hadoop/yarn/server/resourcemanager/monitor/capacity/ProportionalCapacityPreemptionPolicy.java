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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptableQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

/**
 * 按比例容量抢占策略，为容量调度器设计实现的调度编辑策略。
 * 每次执行时计算各队列理想资源分配，判断是否需要抢占，按权重分配超容量资源，
 * 选择需要抢占的容器，等待超时后强制杀死容器，通过抢占事件通知调度器执行。
 */
public class ProportionalCapacityPreemptionPolicy
    implements SchedulingEditPolicy, CapacitySchedulerPreemptionContext {

  /**
   * 队列内抢占排序策略，支持管理员配置不同的抢占优先级顺序。
   */
  @Unstable
  public enum IntraQueuePreemptionOrderPolicy {
    PRIORITY_FIRST, USERLIMIT_FIRST;
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ProportionalCapacityPreemptionPolicy.class);

  private final Clock clock;

  // 可配置参数
  private double maxIgnoredOverCapacity;
  private long maxWaitTime;
  private long monitoringInterval;
  private float percentageClusterPreemptionAllowed;
  private double naturalTerminationFactor;
  private boolean observeOnly;
  private boolean lazyPreempionEnabled;

  private float maxAllowableLimitForIntraQueuePreemption;
  private float minimumThresholdForIntraQueuePreemption;
  private IntraQueuePreemptionOrderPolicy intraQueuePreemptionOrderPolicy;

  private boolean crossQueuePreemptionConservativeDRF;
  private boolean inQueuePreemptionConservativeDRF;

  // 当前配置对象
  private CapacitySchedulerConfiguration csConfig;

  // 指向RM其他组件的引用
  private RMContext rmContext;
  private ResourceCalculator rc;
  private CapacityScheduler scheduler;
  private RMNodeLabelsManager nlm;

  // 内部决策属性：记录待抢占容器和添加时间
  private final Map<RMContainer,Long> preemptionCandidates =
    new HashMap<>();
  // 按队列分区存储临时队列信息
  private Map<String, Map<String, TempQueuePerPartition>> queueToPartitions =
      new HashMap<>();
  // 按分区存储未满足需求的队列列表
  private Map<String, LinkedHashSet<String>> partitionToUnderServedQueues =
      new HashMap<String, LinkedHashSet<String>>();
  // 抢占候选容器选择策略列表
  private List<PreemptionCandidatesSelector> candidatesSelectionPolicies;
  // 所有节点分区标签集合
  private Set<String> allPartitions;
  // 所有叶子队列名称集合
  private Set<String> leafQueueNames;
  // 按选择策略分组的待抢占容器集合
  Map<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
      Set<RMContainer>>> pcsMap;

  // 从调度器同步来的可抢占实体
  private Map<String, PreemptableQueue> preemptableQueues;
  // 可杀死容器ID集合
  private Set<ContainerId> killableContainers;

  public ProportionalCapacityPreemptionPolicy() {
    clock = SystemClock.getInstance();
    allPartitions = Collections.emptySet();
    leafQueueNames = Collections.emptySet();
    preemptableQueues = Collections.emptyMap();
  }

  @VisibleForTesting
  public ProportionalCapacityPreemptionPolicy(RMContext context,
      CapacityScheduler scheduler, Clock clock) {
    init(context.getYarnConfiguration(), context, scheduler);
    this.clock = clock;
    allPartitions = Collections.emptySet();
    leafQueueNames = Collections.emptySet();
    preemptableQueues = Collections.emptyMap();
  }

  /**
   * 初始化抢占策略，验证调度器类型，保存组件引用。
   * @param config 配置对象
   * @param context RM上下文
   * @param sched 资源调度器
   */
  public void init(Configuration config, RMContext context,
      ResourceScheduler sched) {
    LOG.info("Preemption monitor:" + this.getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    rc = scheduler.getResourceCalculator();
    nlm = scheduler.getRMContext().getNodeLabelManager();
    updateConfigIfNeeded();
  }

  /**
   * 检测配置变更，更新抢占策略的所有配置参数，初始化选择策略。
   */
  private void updateConfigIfNeeded() {
    CapacitySchedulerConfiguration config = scheduler.getConfiguration();
    if (config == csConfig) {
      return;
    }

    maxIgnoredOverCapacity = config.getDouble(
        CapacitySchedulerConfiguration.PREEMPTION_MAX_IGNORED_OVER_CAPACITY,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_MAX_IGNORED_OVER_CAPACITY);

    naturalTerminationFactor = config.getDouble(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_NATURAL_TERMINATION_FACTOR);

    maxWaitTime = config.getLong(
        CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_WAIT_TIME_BEFORE_KILL);

    monitoringInterval = config.getLong(
        CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_MONITORING_INTERVAL);

    percentageClusterPreemptionAllowed = config.getFloat(
        CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        CapacitySchedulerConfiguration.DEFAULT_TOTAL_PREEMPTION_PER_ROUND);

    observeOnly = config.getBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_OBSERVE_ONLY,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_OBSERVE_ONLY);

    lazyPreempionEnabled = config.getBoolean(
        CapacitySchedulerConfiguration.LAZY_PREEMPTION_ENABLED,
        CapacitySchedulerConfiguration.DEFAULT_LAZY_PREEMPTION_ENABLED);

    maxAllowableLimitForIntraQueuePreemption = config.getFloat(
        CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT,
        CapacitySchedulerConfiguration.
        DEFAULT_INTRAQUEUE_PREEMPTION_MAX_ALLOWABLE_LIMIT);

    minimumThresholdForIntraQueuePreemption = config.getFloat(
        CapacitySchedulerConfiguration.
        INTRAQUEUE_PREEMPTION_MINIMUM_THRESHOLD,
        CapacitySchedulerConfiguration.
        DEFAULT_INTRAQUEUE_PREEMPTION_MINIMUM_THRESHOLD);

    intraQueuePreemptionOrderPolicy = IntraQueuePreemptionOrderPolicy
        .valueOf(config
            .get(
                CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ORDER_POLICY,
                CapacitySchedulerConfiguration.DEFAULT_INTRAQUEUE_PREEMPTION_ORDER_POLICY)
            .toUpperCase());

    crossQueuePreemptionConservativeDRF =  config.getBoolean(
        CapacitySchedulerConfiguration.
        CROSS_QUEUE_PREEMPTION_CONSERVATIVE_DRF,
        CapacitySchedulerConfiguration.
        DEFAULT_CROSS_QUEUE_PREEMPTION_CONSERVATIVE_DRF);

    inQueuePreemptionConservativeDRF =  config.getBoolean(
        CapacitySchedulerConfiguration.
        IN_QUEUE_PREEMPTION_CONSERVATIVE_DRF,
        CapacitySchedulerConfiguration.
        DEFAULT_IN_QUEUE_PREEMPTION_CONSERVATIVE_DRF);

    candidatesSelectionPolicies = new ArrayList<>();

    // 根据配置决定是否添加队列优先级抢占策略
    boolean isQueuePriorityPreemptionEnabled =
        config.getPUOrderingPolicyUnderUtilizedPreemptionEnabled();
    if (isQueuePriorityPreemptionEnabled) {
      candidatesSelectionPolicies.add(
          new QueuePriorityContainerCandidateSelector(this));
    }

    // 根据配置决定是否添加预留容器候选选择策略
    boolean selectCandidatesForResevedContainers = config.getBoolean(
        CapacitySchedulerConfiguration.
        PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS,
        CapacitySchedulerConfiguration.
        DEFAULT_PREEMPTION_SELECT_CANDIDATES_FOR_RESERVED_CONTAINERS);
    if (selectCandidatesForResevedContainers) {
      candidatesSelectionPolicies
          .add(new ReservedContainerCandidatesSelector(this));
    }

    boolean additionalPreemptionBasedOnReservedResource = config.getBoolean(
        CapacitySchedulerConfiguration.ADDITIONAL_RESOURCE_BALANCE_BASED_ON_RESERVED_CONTAINERS,
        CapacitySchedulerConfiguration.DEFAULT_ADDITIONAL_RESOURCE_BALANCE_BASED_ON_RESERVED_CONTAINERS);

    // 初始化默认FIFO候选选择策略
    candidatesSelectionPolicies.add(new FifoCandidatesSelector(this,
        additionalPreemptionBasedOnReservedResource, false));

    // 根据配置决定是否添加队列平衡抢占策略
    boolean isPreemptionToBalanceRequired = config.getBoolean(
        CapacitySchedulerConfiguration.PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED,
        CapacitySchedulerConfiguration.DEFAULT_PREEMPTION_TO_BALANCE_QUEUES_BEYOND_GUARANTEED);
    long maximumKillWaitTimeForPreemptionToQueueBalance = config.getLong(
        CapacitySchedulerConfiguration.MAX_WAIT_BEFORE_KILL_FOR_QUEUE_BALANCE_PREEMPTION,
        CapacitySchedulerConfiguration.DEFAULT_MAX_WAIT_BEFORE_KILL_FOR_QUEUE_BALANCE_PREEMPTION);
    if (isPreemptionToBalanceRequired) {
      PreemptionCandidatesSelector selector = new FifoCandidatesSelector(this,
          false, true);
      selector.setMaximumKillWaitTime(maximumKillWaitTimeForPreemptionToQueueBalance);
      candidatesSelectionPolicies.add(selector);
    }

    // 根据配置决定是否添加队列内抢占策略
    boolean isIntraQueuePreemptionEnabled = config.getBoolean(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED,
        CapacitySchedulerConfiguration.DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED);
    if (isIntraQueuePreemptionEnabled) {
      candidatesSelectionPolicies.add(new IntraQueueCandidatesSelector(this));
    }

    LOG.info("Capacity Scheduler configuration changed, updated preemption " +
        "properties to:\n" +
        "max_ignored_over_capacity = " + maxIgnoredOverCapacity + "\n" +
        "natural_termination_factor = " + naturalTerminationFactor + "\n" +
        "max_wait_before_kill = " + maxWaitTime + "\n" +
        "monitoring_interval = " + monitoringInterval + "\n" +
        "total_preemption_per_round = " + percentageClusterPreemptionAllowed +
          "\n" +
        "observe_only = " + observeOnly + "\n" +
        "lazy-preemption-enabled = " + lazyPreempionEnabled + "\n" +
        "intra-queue-preemption.enabled = " + isIntraQueuePreemptionEnabled +
          "\n" +
        "intra-queue-preemption.max-allowable-limit = " +
          maxAllowableLimitForIntraQueuePreemption + "\n" +
        "intra-queue-preemption.minimum-threshold = " +
          minimumThresholdForIntraQueuePreemption + "\n" +
        "intra-queue-preemption.preemption-order-policy = " +
          intraQueuePreemptionOrderPolicy + "\n" +
        "priority-utilization.underutilized-preemption.enabled = " +
          isQueuePriorityPreemptionEnabled + "\n" +
        "select_based_on_reserved_containers = " +
          selectCandidatesForResevedContainers + "\n" +
        "additional_res_balance_based_on_reserved_containers = " +
          additionalPreemptionBasedOnReservedResource + "\n" +
        "Preemption-to-balance-queue-enabled = " +
          isPreemptionToBalanceRequired + "\n" +
        "cross-queue-preemption.conservative-drf = " +
          crossQueuePreemptionConservativeDRF + "\n" +
        "in-queue-preemption.conservative-drf = " +
          inQueuePreemptionConservativeDRF);

    csConfig = config;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return rc;
  }

  @Override
  public synchronized void editSchedule() {
    updateConfigIfNeeded();

    long startTs = clock.getTime();

    CSQueue root = scheduler.getRootQueue();
    Resource clusterResources = Resources.clone(scheduler.getClusterResource());
    containerBasedPreemptOrKill(root, clusterResources);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Total time used=" + (clock.getTime() - startTs) + " ms.");
    }
  }

  /**
   * 根据等待超时处理选中的待抢占容器，超时则杀死容器，否则标记为可抢占。
   * @param toPreemptPerSelector 按选择策略分组的待抢占容器
   * @param currentTime 当前时间戳
   */
  private void preemptOrkillSelectedContainerAfterWait(
      Map<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
          Set<RMContainer>>> toPreemptPerSelector, long currentTime) {
    int toPreemptCount = 0;
    for (Map<ApplicationAttemptId, Set<RMContainer>> containers :
        toPreemptPerSelector.values()) {
      toPreemptCount += containers.size();
    }
    LOG.debug(
        "Starting to preempt containers for selectedCandidates and size:{}",
        toPreemptCount);

    // 遍历每个选择策略的待抢占容器
    for (Map.Entry<PreemptionCandidatesSelector, Map<ApplicationAttemptId,
        Set<RMContainer>>> pc : toPreemptPerSelector
        .entrySet()) {
      Map<ApplicationAttemptId, Set<RMContainer>> cMap = pc.getValue();
      if (cMap.size() > 0) {
        for (Map.Entry<ApplicationAttemptId,
            Set<RMContainer>> e : cMap.entrySet()) {
          ApplicationAttemptId appAttemptId = e.getKey();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Send to scheduler: in app=" + appAttemptId
                + " #containers-to-be-preemptionCandidates=" + e.getValue().size());
          }
          for (RMContainer container : e.getValue()) {
            // 检查是否超时：超过当前选择策略的最大等待时间则杀死容器
            if (preemptionCandidates.get(container) != null
                && preemptionCandidates.get(container)
                + pc.getKey().getMaximumKillWaitTimeMs() <= currentTime) {
              // 发送杀死容器事件
              rmContext.getDispatcher().getEventHandler().handle(
                  new ContainerPreemptEvent(appAttemptId, container,
                      SchedulerEventType.MARK_CONTAINER_FOR_KILL