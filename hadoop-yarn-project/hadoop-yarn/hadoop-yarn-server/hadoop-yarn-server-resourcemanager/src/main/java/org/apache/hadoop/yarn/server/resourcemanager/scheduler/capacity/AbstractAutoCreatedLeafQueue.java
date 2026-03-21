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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common
    .QueueEntitlement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;

/**
 * 文件说明：容量调度器中动态自动创建叶子队列的抽象基类，由AbstractManagedParentQueue管理自动创建队列生命周期
 * 抽象类，用于实现由AbstractManagedParentQueue管理的动态自动创建队列
 */
public class AbstractAutoCreatedLeafQueue extends AbstractLeafQueue {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractAutoCreatedLeafQueue.class);

  // 管理此自动创建队列的父队列
  protected AbstractManagedParentQueue parent;

  /**
   * 构造自动创建叶子队列实例
   * @param queueContext 容量调度器队列上下文
   * @param queueName 队列名称
   * @param parent 管理此队列的父队列
   * @param old 旧队列对象（用于队列重建时复用状态）
   * @throws IOException 构造过程中IO异常
   */
  public AbstractAutoCreatedLeafQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, AbstractManagedParentQueue parent, CSQueue old)
      throws IOException {
    super(queueContext, queueName, parent, old);
    this.parent = parent;
  }

  /**
   * 设置队列容量配额，默认使用无节点标签场景
   *
   * @param entitlement 队列新配额（包含容量、最大容量等信息）
   * @throws SchedulerDynamicEditException 配额设置失败时抛出
   */
  public void setEntitlement(QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {
     setEntitlement(NO_LABEL, entitlement);
  }

  @Override
  protected Resource getMinimumAbsoluteResource(QueuePath queuePath,
      String label) {
    // 从父队列的自动创建队列模板配置中获取最小资源
    return super.getMinimumAbsoluteResource(QueuePrefixes
        .getAutoCreatedQueueObjectTemplateConfPrefix(this.getParent().getQueuePathObject()),
        label);
  }

  @Override
  protected Resource getMaximumAbsoluteResource(QueuePath queuePath,
      String label) {
    // 从父队列的自动创建队列模板配置中获取最大资源
    return super.getMaximumAbsoluteResource(QueuePrefixes
        .getAutoCreatedQueueObjectTemplateConfPrefix(this.getParent().getQueuePathObject()),
        label);
  }

  @Override
  protected boolean checkConfigTypeIsAbsoluteResource(QueuePath queuePath,
      String label) {
    // 从父队列的自动创建队列模板配置中检查是否为绝对资源配置
    return super.checkConfigTypeIsAbsoluteResource(QueuePrefixes
        .getAutoCreatedQueueObjectTemplateConfPrefix(this.getParent().getQueuePathObject()),
        label);
  }

  /**
   * 设置指定节点标签的队列容量配额，更新绝对容量
   *
   * @param nodeLabel 节点标签
   * @param entitlement 队列新配额（包含容量、最大容量等信息）
   * @throws SchedulerDynamicEditException 配额设置失败时抛出
   */
  public void setEntitlement(String nodeLabel, QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {
    // 获取写锁保证并发安全
    writeLock.lock();
    try {
      // 取出请求容量
      float capacity = entitlement.getCapacity();
      // 检查容量合法性，必须在[0,1]范围内
      if (capacity < 0 || capacity > 1.0f) {
        throw new SchedulerDynamicEditException(
            "Capacity demand is not in the [0,1] range: " + capacity);
      }
      // 设置当前队列相对父队列的容量
      setCapacity(nodeLabel, capacity);
      // 计算并设置当前队列的绝对容量（父队列绝对容量 * 当前队列相对容量）
      setAbsoluteCapacity(nodeLabel,
          this.getParent().getQueueCapacities().
              getAbsoluteCapacity(nodeLabel)
              * getQueueCapacities().getCapacity(nodeLabel));
      // note: we currently set maxCapacity to capacity
      // this might be revised later
      // 设置队列最大容量
      setMaxCapacity(nodeLabel, entitlement.getMaxCapacity());

      // 设置配置最小容量向量（基于百分比）
      setConfiguredMinCapacityVector(nodeLabel,
          QueueCapacityVector.of(queueCapacities.getCapacity(nodeLabel) * 100,
              QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE));
      // 设置配置最大容量向量（基于百分比）
      setConfiguredMaxCapacityVector(nodeLabel,
          QueueCapacityVector.of(queueCapacities.getMaximumCapacity(nodeLabel) * 100,
              QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE));

      LOG.debug("successfully changed to {} for queue {}", capacity, this
            .getQueuePath());

      // 更新队列统计信息（已用容量等）
      CSQueueUtils.updateQueueStatistics(resourceCalculator,
          queueContext.getClusterResource(),
          this, labelManager, nodeLabel);
    } finally {
      // 释放写锁
      writeLock.unlock();
    }
  }
}