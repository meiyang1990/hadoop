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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;


import org.apache.hadoop.classification.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;


import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .QueueManagementChangeEvent;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 容量调度器队列管理动态编辑策略，用于支持开启自动子队列创建的托管父队列动态调整
 */
public class QueueManagementDynamicEditPolicy implements SchedulingEditPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(QueueManagementDynamicEditPolicy.class);

  private Clock clock;

  // 指向RM其他核心组件的引用
  private RMContext rmContext;
  private ResourceCalculator rc;
  private CapacityScheduler scheduler;
  private RMNodeLabelsManager nlm;

  private long monitoringInterval;

  // 存储所有开启自动创建子队列的托管父队列名称
  private Set<String> managedParentQueues = new HashSet<>();

  /**
   * 空构造函数，由CapacitySchedulerConfiguration反射实例化
   */
  public QueueManagementDynamicEditPolicy() {
    clock = SystemClock.getInstance();
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  public QueueManagementDynamicEditPolicy(RMContext context,
      CapacityScheduler scheduler) {
    init(context.getYarnConfiguration(), context, scheduler);
  }

  @SuppressWarnings("unchecked")
  @VisibleForTesting
  public QueueManagementDynamicEditPolicy(RMContext context,
      CapacityScheduler scheduler, Clock clock) {
    init(context.getYarnConfiguration(), context, scheduler);
    this.clock = clock;
  }

  @Override
  public void init(final Configuration config, final RMContext context,
      final ResourceScheduler sched) {
    LOG.info("Queue Management Policy monitor: {}" + this.
        getClass().getCanonicalName());
    assert null == scheduler : "Unexpected duplicate call to init";
    // 检查调度器是否为CapacityScheduler，不兼容其他调度器
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    clock = scheduler.getClock();

    rc = scheduler.getResourceCalculator();
    nlm = scheduler.getRMContext().getNodeLabelManager();

    CapacitySchedulerConfiguration csConfig = scheduler.getConfiguration();

    // 从配置加载队列管理监控间隔时间
    monitoringInterval = csConfig.getLong(
        CapacitySchedulerConfiguration.QUEUE_MANAGEMENT_MONITORING_INTERVAL,
        CapacitySchedulerConfiguration.
            DEFAULT_QUEUE_MANAGEMENT_MONITORING_INTERVAL);

    // 初始化托管父队列列表
    initQueues();
  }

  /**
   * 重新初始化队列（调度器重新初始化时调用）
   * @param config 配置对象
   * @param context ResourceManager上下文
   * @param sched 调度器实例
   */
  public void reinitialize(final Configuration config, final RMContext context,
      final ResourceScheduler sched) {
    //TODO - Wire with scheduler reinitialize and remove initQueues below?
    initQueues();
  }

  /**
   * 扫描调度器所有队列，收集所有托管父队列
   */
  private void initQueues() {
    managedParentQueues.clear();
    for (Map.Entry<String, CSQueue> queues : scheduler
        .getCapacitySchedulerQueueManager()
        .getQueues().entrySet()) {

      String queueName = queues.getKey();
      CSQueue queue = queues.getValue();

      if ( queue instanceof ManagedParentQueue) {
        managedParentQueues.add(queueName);
      }
    }
  }

  @Override
  public void editSchedule() {
    long startTs = clock.getTime();

    // 重新扫描更新托管父队列列表
    initQueues();
    // 处理自动创建的叶子队列，生成并应用变更
    manageAutoCreatedLeafQueues();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Total time used=" + (clock.getTime() - startTs) + " ms.");
    }
  }

  @VisibleForTesting
  List<QueueManagementChange> manageAutoCreatedLeafQueues()
  {

    List<QueueManagementChange> queueManagementChanges = new ArrayList<>();

    // 只有存在待处理的托管父队列才执行处理
    if (managedParentQueues.size() > 0) {
      for (String parentQueueName : managedParentQueues) {
        // 获取父队列实例
        ManagedParentQueue parentQueue =
            (ManagedParentQueue) scheduler.getCapacitySchedulerQueueManager().
                getQueue(parentQueueName);

        // 计算该父队列需要执行的队列变更，添加到总变更列表
        queueManagementChanges.addAll(
            computeQueueManagementChanges
            (parentQueue));
      }
    }
    return queueManagementChanges;
  }


  @VisibleForTesting
  List<QueueManagementChange> computeQueueManagementChanges
      (ManagedParentQueue parentQueue) {

    // 默认返回空变更列表
    List<QueueManagementChange> queueManagementChanges =
        Collections.emptyList();
    // 仅当允许超出保障容量自动创建队列时才执行变更计算
    if (!parentQueue.shouldFailAutoCreationWhenGuaranteedCapacityExceeded()) {

      // 获取父队列配置的自动队列管理策略实例
      AutoCreatedQueueManagementPolicy policyClazz =
          parentQueue.getAutoCreatedQueueManagementPolicy();
      long startTime = 0;
      try {
        startTime = clock.getTime();

        // 调用策略计算需要执行的队列变更
        queueManagementChanges = policyClazz.computeQueueManagementChanges();

        // 如果有变更，发送异步事件让调度器更新
        if (queueManagementChanges.size() > 0) {
          QueueManagementChangeEvent queueManagementChangeEvent =
              new QueueManagementChangeEvent(parentQueue,
                  queueManagementChanges);
          scheduler.getRMContext().getDispatcher().getEventHandler().handle(
              queueManagementChangeEvent);
        }

        // 调试日志记录处理耗时和变更信息
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} uses {} millisecond" + " to run",
              policyClazz.getClass().getName(), clock.getTime() - startTime);
          if (queueManagementChanges.size() > 0) {
            LOG.debug(" Updated queue management changes for parent queue" + " "
                    + "{}: [{}]", parentQueue.getQueuePath(),
                queueManagementChanges.size() < 25 ?
                    queueManagementChanges.toString() :
                    queueManagementChanges.size());
          }
        }
      } catch (YarnException e) {
        // 捕获计算异常，记录错误日志不中断整个流程
        LOG.error(
            "Could not compute child queue management updates for parent "
                + "queue "
                + parentQueue.getQueuePath(), e);
      }
    } else{
      // 配置禁止超出容量创建，跳过该父队列处理
      LOG.debug("Skipping queue management updates for parent queue {} "
          + "since configuration for auto creating queues beyond "
          + "parent's guaranteed capacity is disabled",
          parentQueue.getQueuePath());
    }
    return queueManagementChanges;
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return "QueueManagementDynamicEditPolicy";
  }

  public ResourceCalculator getResourceCalculator() {
    return rc;
  }

  public RMContext getRmContext() {
    return rmContext;
  }

  public ResourceCalculator getRC() {
    return rc;
  }

  public CapacityScheduler getScheduler() {
    return scheduler;
  }

  public Set<String> getManagedParentQueues() {
    return managedParentQueues;
  }
}