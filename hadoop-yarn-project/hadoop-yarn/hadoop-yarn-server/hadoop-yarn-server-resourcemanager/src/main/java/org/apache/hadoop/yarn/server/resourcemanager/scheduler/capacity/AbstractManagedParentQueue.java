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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common
    .QueueEntitlement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * 文件说明：容量调度器中自动创建子队列的父队列抽象基类，继承自AbstractParentQueue
 * 
 * 用于管理动态自动创建的叶子队列，对用户而言对外表现类似叶子队列，实际本身是父队列类型，
 * 负责管理动态生成的子叶子队列的生命周期与配置管理。
 */
public abstract class AbstractManagedParentQueue extends AbstractParentQueue {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractManagedParentQueue.class);

  // 自动创建叶子队列的配置模板，用于新建队列时继承配置
  protected AutoCreatedLeafQueueConfig leafQueueTemplate;
  // 自动创建队列的管理策略，定义队列创建/销毁的规则
  protected AutoCreatedQueueManagementPolicy queueManagementPolicy = null;

  /**
   * 构造函数，创建自动管理子队列的父队列实例
   * @param queueContext 容量调度器队列上下文
   * @param queueName 队列名称
   * @param parent 父队列
   * @param old 旧队列对象（用于重新初始化）
   * @throws IOException 初始化IO异常
   */
  public AbstractManagedParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    super(queueContext, queueName, parent, old);
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
      throws IOException {
    // 获取写锁
    writeLock.lock();
    try {
      // 重新应用队列配置
      setupQueueConfigs(clusterResource);

    } finally {
      // 释放写锁
      writeLock.unlock();
    }
  }

  /**
   * 添加子队列到当前父队列
   * @param childQueue 待添加的子队列引用
   * @throws SchedulerDynamicEditException 添加失败抛出异常
   * @throws IOException IO异常
   */
  public void addChildQueue(CSQueue childQueue)
      throws SchedulerDynamicEditException, IOException {
    writeLock.lock();
    try {
      // 校验待添加队列容量必须为0（动态创建队列容量由父队列统一分配）
      if (childQueue.getCapacity() > 0) {
        throw new SchedulerDynamicEditException(
            "Queue " + childQueue + " being added has non zero capacity.");
      }
      // 添加到子队列集合
      boolean added = this.childQueues.add(childQueue);
      if (LOG.isDebugEnabled()) {
        LOG.debug("updateChildQueues (action: add queue): " + added + " "
            + getChildQueuesToPrint());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 移除指定子队列
   * @param childQueue 待移除的子队列引用
   * @throws SchedulerDynamicEditException 移除失败抛出异常
   */
  public void removeChildQueue(CSQueue childQueue)
      throws SchedulerDynamicEditException {
    writeLock.lock();
    try {
      // 校验待删除队列容量必须为0，避免移除正在占用资源的队列
      if (childQueue.getCapacity() > 0) {
        throw new SchedulerDynamicEditException(
            "Queue " + childQueue + " being removed has non zero capacity.");
      }
      // 遍历子队列查找并移除
      Iterator<CSQueue> qiter = childQueues.iterator();
      while (qiter.hasNext()) {
        CSQueue cs = qiter.next();
        if (cs.equals(childQueue)) {
          qiter.remove();
          LOG.debug("Removed child queue: {}", cs.getQueuePath());
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 根据队列名称移除指定子队列
   * @param childQueueName 待移除子队列名称
   * @return 被移除的子队列对象
   * @throws SchedulerDynamicEditException 移除失败抛出异常
   */
  public CSQueue removeChildQueue(String childQueueName)
      throws SchedulerDynamicEditException {
    CSQueue childQueue;
    writeLock.lock();
    try {
      // 从队列管理器查找目标队列
      childQueue = queueContext.getQueueManager().getQueue(childQueueName);
      if (childQueue != null) {
        // 调用移除逻辑
        removeChildQueue(childQueue);
      } else {
        throw new SchedulerDynamicEditException("Cannot find queue to delete "
            + ": " + childQueueName);
      }
    } finally {
      writeLock.unlock();
    }
    return childQueue;
  }

  /**
   * 计算所有子队列容量总和
   * @return 子队列容量总和
   */
  protected float sumOfChildCapacities() {
    writeLock.lock();
    try {
      float ret = 0;
      for (CSQueue l : childQueues) {
        ret += l.getCapacity();
      }
      return ret;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 计算所有子队列绝对容量总和
   * @return 子队列绝对容量总和
   */
  protected float sumOfChildAbsCapacities() {
    writeLock.lock();
    try {
      float ret = 0;
      for (CSQueue l : childQueues) {
        ret += l.getAbsoluteCapacity();
      }
      return ret;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 获取自动创建叶子队列的配置模板
   * @return 叶子队列配置模板
   */
  public AutoCreatedLeafQueueConfig getLeafQueueTemplate() {
    return leafQueueTemplate;
  }

  /**
   * 获取自动创建队列的管理策略实例
   * @return 自动队列管理策略
   */
  public AutoCreatedQueueManagementPolicy
  getAutoCreatedQueueManagementPolicy() {
    return queueManagementPolicy;
  }

  /**
   * 从父队列配置中初始化叶子队列配置
   * @param configPrefix 配置前缀
   * @return 提取后的叶子队列配置对象
   */
  protected CapacitySchedulerConfiguration initializeLeafQueueConfigs(String
      configPrefix) {

    // 创建空的容量调度器配置对象
    CapacitySchedulerConfiguration leafQueueConfigs = new
        CapacitySchedulerConfiguration(new Configuration(false), false);

    // 从调度器全局配置中提取指定前缀的所有配置项
    Map<String, String> templateConfigs = queueContext
        .getConfiguration().getConfigurationProperties()
        .getPropertiesWithPrefix(configPrefix, true);

    // 将提取的配置项复制到新配置对象
    for (Map.Entry<String, String> confKeyValuePair : templateConfigs.entrySet()) {
      leafQueueConfigs.set(confKeyValuePair.getKey(), confKeyValuePair.getValue());
    }

    return leafQueueConfigs;
  }

  /**
   * 校验自动创建叶子队列的权限变更是否合法
   * @param leafQueue 待变更的自动创建叶子队列
   * @param entitlement 新的队列权限配置
   * @throws SchedulerDynamicEditException 校验不通过抛出异常
   */
  protected void validateQueueEntitlementChange(AbstractAutoCreatedLeafQueue
      leafQueue, QueueEntitlement entitlement)
      throws SchedulerDynamicEditException {

    // 计算当前所有子队列容量总和
    float sumChilds = sumOfChildCapacities();
    // 计算变更后的总容量：原总和 - 旧容量 + 新容量
    float newChildCap =
        sumChilds - leafQueue.getCapacity() + entitlement.getCapacity();

    // 校验变更后总容量不超过100%（加上容差处理浮点误差），且不小于0
    if (!(newChildCap >= 0 && newChildCap < 1.0f + CSQueueUtils.EPSILON)) {
      throw new SchedulerDynamicEditException(
          "Sum of child queues should exceed 100% for auto creating parent "
              + "queue : " + getQueueName());
    }
  }
}