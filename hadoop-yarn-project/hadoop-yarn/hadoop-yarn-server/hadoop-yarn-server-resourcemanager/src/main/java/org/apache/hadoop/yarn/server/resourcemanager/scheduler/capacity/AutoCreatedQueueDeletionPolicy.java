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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AutoCreatedQueueDeletionEvent;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 自动创建队列V2的自动删除策略，仅适用于基于权重的自动创建队列。
 * 定期扫描符合删除条件的空闲自动队列，分两轮标记后触发删除。
 */
public class AutoCreatedQueueDeletionPolicy implements SchedulingEditPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(AutoCreatedQueueDeletionPolicy.class);

  private Clock clock;

  // 指向RM其他组件的引用
  private RMContext rmContext;
  private ResourceCalculator rc;
  private CapacityScheduler scheduler;

  // 队列删除检查间隔（毫秒）
  private long monitoringInterval;

  // 标记待删除队列：每个检查周期中，新增符合删除条件的队列加入该集合
  private Set<String> markedForDeletion = new HashSet<>();
  // 确认待删除队列：若下个检查周期中，队列仍符合删除条件且已被标记，则移入该集合等待删除
  private Set<String> sentForDeletion = new HashSet<>();

  @Override
  public void init(final Configuration config, final RMContext context,
                   final ResourceScheduler sched) {
    LOG.info("Auto Deletion Policy monitor: {}" + this.
        getClass().getCanonicalName());
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException("Class " +
          sched.getClass().getCanonicalName() + " not instance of " +
          CapacityScheduler.class.getCanonicalName());
    }
    rmContext = context;
    scheduler = (CapacityScheduler) sched;
    clock = scheduler.getClock();

    rc = scheduler.getResourceCalculator();

    CapacitySchedulerConfiguration csConfig = scheduler.getConfiguration();

    // 默认监控间隔等于队列自动删除过期时间
    monitoringInterval =
        csConfig.getLong(CapacitySchedulerConfiguration.
                AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME,
            CapacitySchedulerConfiguration.
                DEFAULT_AUTO_CREATE_CHILD_QUEUE_EXPIRED_TIME) * 1000;

    prepareForAutoDeletion();
  }

  /**
   * 扫描所有队列，筛选符合自动删除条件的队列并更新标记状态。
   * 已经连续两次扫描符合条件的队列会被移入待删除集合。
   */
  public void prepareForAutoDeletion() {
    Set<String> newMarks = new HashSet<>();
    // 遍历所有队列检查删除资格
    for (Map.Entry<String, CSQueue> queueEntry :
        scheduler.getCapacitySchedulerQueueManager().getQueues().entrySet()) {
      String queuePath = queueEntry.getKey();
      CSQueue queue = queueEntry.getValue();
      if (queue instanceof AbstractCSQueue &&
          ((AbstractCSQueue) queue).isEligibleForAutoDeletion()) {
        // 已经标记过的队列，确认后移入待删除集合
        if (markedForDeletion.contains(queuePath)) {
          sentForDeletion.add(queuePath);
          markedForDeletion.remove(queuePath);
        } else {
          // 首次符合条件，加入标记集合
          newMarks.add(queuePath);
        }
      }
    }
    markedForDeletion.clear();
    markedForDeletion.addAll(newMarks);
  }

  @Override
  public void editSchedule() {
    long startTs = clock.getTime();

    prepareForAutoDeletion();
    triggerAutoDeletionForExpiredQueues();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Total time used=" + (clock.getTime() - startTs) + " ms.");
    }
  }

  /**
   * 对所有确认待删除的队列触发删除操作。
   */
  public void triggerAutoDeletionForExpiredQueues() {
    // 遍历待删除队列执行删除
    for (String queueName : sentForDeletion) {
      CSQueue checkQueue =
          scheduler.getCapacitySchedulerQueueManager().
              getQueue(queueName);
      deleteAutoCreatedQueue(checkQueue);
    }
    sentForDeletion.clear();
  }

  private void deleteAutoCreatedQueue(CSQueue queue) {
    if (queue != null) {
      // 创建队列删除事件，分发到事件处理器执行实际删除
      AutoCreatedQueueDeletionEvent autoCreatedQueueDeletionEvent =
          new AutoCreatedQueueDeletionEvent(queue);
      LOG.info("Queue:" + queue.getQueuePath() +
          " will trigger deletion event to CS.");
      scheduler.getRMContext().getDispatcher().getEventHandler().handle(
          autoCreatedQueueDeletionEvent);
    }
  }

  @Override
  public long getMonitoringInterval() {
    return monitoringInterval;
  }

  @Override
  public String getPolicyName() {
    return AutoCreatedQueueDeletionPolicy.class.getCanonicalName();
  }

  @VisibleForTesting
  public Set<String> getMarkedForDeletion() {
    return markedForDeletion;
  }

  @VisibleForTesting
  public Set<String> getSentForDeletion() {
    return sentForDeletion;
  }
}