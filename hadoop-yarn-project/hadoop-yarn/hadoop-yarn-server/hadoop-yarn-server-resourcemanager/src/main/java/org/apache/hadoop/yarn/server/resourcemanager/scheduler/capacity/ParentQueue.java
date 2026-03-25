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

import java.io.IOException;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * 容量调度器中的父队列实现类，支持动态创建子队列，管理子队列资源分配
 */
@Private
@Evolving
public class ParentQueue extends AbstractParentQueue {

  private static final Logger LOG =
      LoggerFactory.getLogger(ParentQueue.class);

  /**
   * 构造ParentQueue实例
   * @param queueContext 容量调度器队列上下文
   * @param queueName 队列名称
   * @param parent 父队列
   * @param old 原有队列（用于重建队列场景）
   * @throws IOException 配置加载异常
   */
  public ParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(queueContext, queueName, parent, old, false);
  }

  /**
   * 构造ParentQueue实例，支持标记是否为动态创建队列
   * @param queueContext 容量调度器队列上下文
   * @param queueName 队列名称
   * @param parent 父队列
   * @param old 原有队列（用于重建队列场景）
   * @param isDynamic 是否为动态创建队列
   * @throws IOException 配置加载异常
   */
  public ParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old, boolean isDynamic)
      throws IOException {
    super(queueContext, queueName, parent, old, isDynamic);
    super.setupQueueConfigs(queueContext.getClusterResource());
  }

  /**
   * 添加动态创建的父队列子节点
   * @param queuePath 新队列完整路径
   * @return 新创建的父队列
   * @throws SchedulerDynamicEditException 动态编辑队列异常
   */
  public ParentQueue addDynamicParentQueue(String queuePath)
      throws SchedulerDynamicEditException {
    return (ParentQueue) addDynamicChildQueue(queuePath, false);
  }

  /**
   * 添加动态创建的叶子队列子节点
   * @param queuePath 新队列完整路径
   * @return 新创建的叶子队列
   * @throws SchedulerDynamicEditException 动态编辑队列异常
   */
  public LeafQueue addDynamicLeafQueue(String queuePath)
      throws SchedulerDynamicEditException {
    return (LeafQueue) addDynamicChildQueue(queuePath, true);
  }

  /**
   * 动态添加子队列核心方法，支持创建父队列或叶子队列
   * @param childQueuePath 新子队列完整路径
   * @param isLeaf 是否为叶子队列
   * @return 新创建的子队列
   * @throws SchedulerDynamicEditException 动态编辑队列异常
   */
  private CSQueue addDynamicChildQueue(String childQueuePath, boolean isLeaf)
      throws SchedulerDynamicEditException {
    // 获取写锁保证并发安全
    writeLock.lock();
    try {
      // 检查队列是否已存在，存在则直接返回已有队列
      CSQueue queue =
          queueContext.getQueueManager().getQueueByFullName(
              childQueuePath);
      if (queue != null) {
        LOG.warn(
            "This should not happen, trying to create queue=" + childQueuePath
                + ", however the queue already exists");
        return queue;
      }

      // 检查是否超过最大子队列数量限制
      int maxQueues = queueContext.getConfiguration().
          getAutoCreatedQueuesV2MaxChildQueuesLimit(getQueuePathObject());
      if (childQueues.size() >= maxQueues) {
        throw new SchedulerDynamicEditException(
            "Cannot auto create queue " + childQueuePath + ". Max Child "
                + "Queue limit exceeded which is configured as: " + maxQueues
                + " and number of child queues is: " + childQueues.size());
      }

      // 检查是否允许动态创建队列：仅当前所有子队列都基于权重分配容量时才允许
      boolean weightsAreUsed = false;
      try {
        weightsAreUsed = getCapacityConfigurationTypeForQueues(childQueues)
            == QueueCapacityType.WEIGHT;
      } catch (IOException e) {
        LOG.warn("Caught Exception during auto queue creation", e);
      }
      if (!weightsAreUsed && queueContext.getConfiguration().isLegacyQueueMode()) {
        throw new SchedulerDynamicEditException(
            "Trying to create new queue=" + childQueuePath
                + " but not all the queues under parent=" + this.getQueuePath()
                + " are using weight-based capacity. Failed to created queue");
      }

      // 创建新队列实例并添加到子队列列表
      CSQueue newQueue = createNewQueue(childQueuePath, isLeaf);
      this.childQueues.add(newQueue);
      // 更新队列最后提交时间戳
      updateLastSubmittedTimeStamp();

      // 更新集群资源，重新计算所有子队列的有效最小/最大资源
      this.updateClusterResource(queueContext.getClusterResource(),
          new ResourceLimits(queueContext.getClusterResource()));

      return newQueue;
    } finally {
      // 释放写锁
      writeLock.unlock();
    }
  }
}