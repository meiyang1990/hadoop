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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;

import java.io.IOException;

/**
 * 容量调度器队列放置规则的工具类，提供队列映射验证、放置上下文构建等公共能力。
 */
public final class QueuePlacementRuleUtils {

  /** 当前用户占位符，用于动态映射到对应用户队列 */
  public static final String CURRENT_USER_MAPPING = "%user";

  /** 主用户组占位符，用于动态映射到对应用户组队列 */
  public static final String PRIMARY_GROUP_MAPPING = "%primary_group";

  /** 次要用户组占位符，用于动态映射到对应用户组队列 */
  public static final String SECONDARY_GROUP_MAPPING = "%secondary_group";

  private QueuePlacementRuleUtils() {
  }

  /**
   * 验证父队列下的叶子队列映射配置是否合法。
   * @param parentQueue 待验证的父队列对象
   * @param parentQueueName 配置中指定的父队列名称
   * @param leafQueuePath 配置中指定的叶子队列完整路径
   * @throws IOException 验证不通过时抛出异常
   */
  public static void validateQueueMappingUnderParentQueue(
            CSQueue parentQueue, String parentQueueName,
            String leafQueuePath) throws IOException {
    // 父队列不存在，抛出异常
    if (parentQueue == null) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue [" + leafQueuePath
              + "] and invalid parent queue [" + parentQueueName + "]");
    // 父队列不是ManagedParentQueue，不支持自动创建叶子队列，抛出异常
    } else if (!(parentQueue instanceof ManagedParentQueue)) {
      throw new IOException("mapping contains leaf queue [" + leafQueuePath
          + "] and invalid parent queue which "
          + "does not have auto creation of leaf queues enabled ["
          + parentQueueName + "]");
    // 配置的父队列名称与实际队列不匹配（短名称和全路径都不匹配），抛出异常
    } else if (!parentQueue.getQueueShortName().equals(parentQueueName)
        && !parentQueue.getQueuePath().equals(parentQueueName)) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue [" + leafQueuePath
              + "] and invalid parent queue "
              + "which does not match existing leaf queue's parent : ["
              + parentQueueName + "] does not match [ " + parentQueue
              .getQueueShortName() + "]");
    }
  }

  /**
   * 验证并获取自动创建队列的映射配置。
   * @param queueManager 容量调度器队列管理器
   * @param mapping 待验证的队列映射
   * @return 验证通过的队列映射，无合法父队列时返回null
   * @throws IOException 验证不通过时抛出异常
   */
  public static QueueMapping validateAndGetAutoCreatedQueueMapping(
      CapacitySchedulerQueueManager queueManager, QueueMapping mapping)
      throws IOException {
    if (mapping.hasParentQueue()) {
      // 指定了父队列，需要验证父队列存在且为ManagedParentQueue（支持自动创建叶子队列）
      validateQueueMappingUnderParentQueue(queueManager.getQueue(
          mapping.getParentQueue()), mapping.getParentQueue(),
          mapping.getFullPath());
      return mapping;
    }

    return null;
  }

  /**
   * 验证并获取通用队列映射配置。
   * @param queueManager 容量调度器队列管理器
   * @param queue 待验证的目标队列
   * @param mapping 待验证的队列映射
   * @return 验证通过的队列映射
   * @throws IOException 验证不通过时抛出异常
   */
  public static QueueMapping validateAndGetQueueMapping(
      CapacitySchedulerQueueManager queueManager, CSQueue queue,
      QueueMapping mapping) throws IOException {
    // 目标队列不是叶子队列，抛出异常
    if (!(queue instanceof AbstractLeafQueue)) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue : " +
          mapping.getFullPath());
    }

    // 如果是自动创建的叶子队列且父队列是ManagedParentQueue，需要额外验证父队列配置
    if (queue instanceof AutoCreatedLeafQueue && queue
        .getParent() instanceof ManagedParentQueue) {

      QueueMapping newMapping = validateAndGetAutoCreatedQueueMapping(
          queueManager, mapping);
      if (newMapping == null) {
        throw new IOException(
            "mapping contains invalid or non-leaf queue " +
            mapping.getFullPath());
      }
      return newMapping;
    }
    return mapping;
  }

  /**
   * 判断队列映射是否为静态映射（不包含任何动态占位符）。
   * @param mapping 待判断的队列映射
   * @return 是否为静态映射
   */
  public static boolean isStaticQueueMapping(QueueMapping mapping) {
    return !mapping.getQueue().contains(CURRENT_USER_MAPPING) && !mapping
        .getQueue().contains(PRIMARY_GROUP_MAPPING)
        && !mapping.getQueue().contains(SECONDARY_GROUP_MAPPING);
  }

  /**
   * 根据队列映射构建应用放置上下文。
   * @param mapping 队列映射配置
   * @param queueManager 容量调度器队列管理器
   * @return 应用放置上下文
   * @throws IOException 验证不通过时抛出异常
   */
  public static ApplicationPlacementContext getPlacementContext(
      QueueMapping mapping, CapacitySchedulerQueueManager queueManager)
      throws IOException {
    return getPlacementContext(mapping, mapping.getQueue(), queueManager);
  }

  /**
   * 根据队列映射和叶子队列名称构建应用放置上下文，检查队列歧义性。
   * @param mapping 队列映射配置
   * @param leafQueueName 目标叶子队列名称
   * @param queueManager 容量调度器队列管理器
   * @return 应用放置上下文
   * @throws IOException 存在歧义队列引用时抛出异常
   */
  public static ApplicationPlacementContext getPlacementContext(
      QueueMapping mapping, String leafQueueName,
      CapacitySchedulerQueueManager queueManager) throws IOException {

    // 未指定父队列且队列名称不唯一（存在歧义），抛出异常
    if (!mapping.hasParentQueue() && queueManager.isAmbiguous(leafQueueName)) {
      throw new IOException("mapping contains ambiguous leaf queue reference " +
          leafQueueName);
    }

    // 指定了父队列，创建带父队列的放置上下文
    if (!org.apache.commons.lang3.StringUtils.isEmpty(mapping.getParentQueue())) {
      return new ApplicationPlacementContext(leafQueueName,
          mapping.getParentQueue());
    } else{
      // 未指定父队列，创建仅含叶子队列名称的放置上下文
      return new ApplicationPlacementContext(leafQueueName);
    }
  }
}