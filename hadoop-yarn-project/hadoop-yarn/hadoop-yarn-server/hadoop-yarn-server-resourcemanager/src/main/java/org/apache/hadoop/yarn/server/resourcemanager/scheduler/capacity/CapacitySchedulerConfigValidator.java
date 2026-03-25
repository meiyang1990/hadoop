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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 容量调度器配置验证工具类，提供容量调度器配置刷新前的完整性、合法性检查能力
 */
public final class CapacitySchedulerConfigValidator {
  private static final Logger LOG = LoggerFactory.getLogger(
          CapacitySchedulerConfigValidator.class);

  private CapacitySchedulerConfigValidator() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * 整体验证容量调度器新配置的合法性，通过启动一个新的调度器实例加载配置完成验证
   * @param oldConfParam 旧配置
   * @param newConf 待验证的新配置
   * @param rmContext RM上下文
   * @return 验证通过返回true
   * @throws IOException 验证失败抛出IO异常
   */
  public static boolean validateCSConfiguration(
          final Configuration oldConfParam, final Configuration newConf,
          final RMContext rmContext) throws IOException {
    // 深度拷贝旧配置，避免修改原配置
    Configuration oldConf = new Configuration(oldConfParam);
    // 标记配置为验证模式，指标统计不做实际更新
    QueueMetrics.setConfigurationValidation(oldConf, true);
    QueueMetrics.setConfigurationValidation(newConf, true);

    // 获取当前运行的容量调度器实例
    CapacityScheduler liveScheduler = (CapacityScheduler) rmContext.getScheduler();
    // 创建新的调度器实例用于验证配置
    CapacityScheduler newCs = new CapacityScheduler();
    try {
      //TODO: extract all the validation steps and replace reinitialize with
      //the specific validation steps
      newCs.setConf(oldConf);
      newCs.setRMContext(rmContext);
      newCs.init(oldConf);
      // 复用当前集群节点信息
      newCs.addNodes(liveScheduler.getAllNodes());
      // 重新初始化加载新配置，若配置非法会抛出异常
      newCs.reinitialize(newConf, rmContext, true);
      return true;
    } finally {
      // 停止验证用的调度器实例，释放资源
      newCs.stop();
    }
  }

  /**
   * 验证应用放置规则配置，检查是否存在重复规则
   * @param placementRuleStrs 待验证的放置规则列表
   * @return 去重后的规则集合
   * @throws IOException 存在重复规则抛出异常
   */
  public static Set<String> validatePlacementRules(
          Collection<String> placementRuleStrs) throws IOException {
    Set<String> distinguishRuleSet = new LinkedHashSet<>();
    // 检查是否存在重复放置规则
    for (String pls : placementRuleStrs) {
      if (!distinguishRuleSet.add(pls)) {
        throw new IOException("Invalid PlacementRule inputs which "
                + "contains duplicate rule strings");
      }
    }
    return distinguishRuleSet;
  }

  /**
   * 验证内存资源分配配置的合法性
   * @param conf 调度器配置
   */
  public static void validateMemoryAllocation(Configuration conf) {
    int minMem = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    // 最小值需大于0，且不大于最大值
    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
              + " allocation configuration"
              + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
              + "=" + minMem
              + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
              + "=" + maxMem + ", min and max should be greater than 0"
              + ", max should be no smaller than min.");
    }
  }

  /**
   * 验证CPU核数分配配置的合法性
   * @param conf 调度器配置
   */
  public static void validateVCores(Configuration conf) {
    int minVcores = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores = conf.getInt(
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    // 最小值需大于0，且不大于最大值
    if (minVcores <= 0 || minVcores > maxVcores) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
              + " allocation configuration"
              + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
              + "=" + minVcores
              + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
              + "=" + maxVcores + ", min and max should be greater than 0"
              + ", max should be no smaller than min.");
    }
  }

  /**
   * 验证队列层次结构变更的合法性，检查删除、类型转换、层次移动等操作是否符合规则
   * 规则：非停止状态队列不允许删除；队列不允许跨层级移动；只有停止状态叶子队列才能转换为父队列
   * @param queues 原有队列存储
   * @param newQueues 新配置解析后的队列存储
   * @param newConf 容量调度器新配置
   * @throws IOException 验证不通过抛出IO异常
   */
  public static void validateQueueHierarchy(
      CSQueueStore queues,
      CSQueueStore newQueues,
      CapacitySchedulerConfiguration newConf) throws IOException {
    // 遍历所有原有静态队列，检查合法性
    for (CSQueue oldQueue : queues.getQueues()) {
      // 跳过自动创建的叶子队列
      if (AbstractAutoCreatedLeafQueue.class.isAssignableFrom(oldQueue.getClass())) {
        continue;
      }

      final String queuePath = oldQueue.getQueuePath();
      final String configPrefix = QueuePrefixes.getQueuePrefix(
          oldQueue.getQueuePathObject());
      // 从新配置中获取队列状态
      final QueueState newQueueState = createQueueState(newConf.get(configPrefix + "state"),
          queuePath);
      final CSQueue newQueue = newQueues.get(queuePath);

      if (null == newQueue) {
        // 原有队列不存在于新配置中，即将被删除
        if (isEitherQueueStopped(oldQueue.getState(), newQueueState)) {
          LOG.info("Deleting Queue {}, as it is not present in the modified capacity " +
              "configuration xml", queuePath);
        } else {
          // 非动态队列且未停止，不允许删除
          if (!isDynamicQueue(oldQueue)) {
            throw new IOException(oldQueue.getQueuePath() + " cannot be"
                + " deleted from the capacity scheduler configuration, as the"
                + " queue is not yet in stopped state. Current State : "
                + oldQueue.getState());
          }
        }
      } else {
        // 队列存在于新配置，验证各项规则
        validateSameQueuePath(oldQueue, newQueue);
        validateParentQueueConversion(oldQueue, newQueue);
        validateLeafQueueConversion(oldQueue, newQueue);
      }
    }
  }

  /**
   * 验证队列路径未发生变更，不允许队列跨层级移动
   * @param oldQueue 原有队列
   * @param newQueue 新配置中的队列
   * @throws IOException 路径变更抛出异常
   */
  private static void validateSameQueuePath(CSQueue oldQueue, CSQueue newQueue) throws IOException {
    if (!oldQueue.getQueuePath().equals(newQueue.getQueuePath())) {
      // Queues cannot be moved from one hierarchy to another
      throw new IOException(
          oldQueue.getQueuePath() + " is moved from:" + oldQueue.getQueuePath() + " to:"
              + newQueue.getQueuePath()
              + " after refresh, which is not allowed.");
    }
  }

  /**
   * 验证父队列类型转换的合法性
   * @param oldQueue 原有队列
   * @param newQueue 新配置中的队列
   * @throws IOException 非法转换抛出异常
   */
  private static void validateParentQueueConversion(CSQueue oldQueue,
                                                    CSQueue newQueue) throws IOException {
    if (oldQueue instanceof AbstractParentQueue) {
      // 不允许普通父队列转换为自动创建子队列的父队列，会导致原有预配置子队列不兼容
      if (!(oldQueue instanceof ManagedParentQueue) && newQueue instanceof ManagedParentQueue) {
        throw new IOException(
            "Can not convert parent queue: " + oldQueue.getQueuePath()
                + " to auto create enabled parent queue since "
                + "it could have other pre-configured queues which is not "
                + "supported");
      }

      // 不允许自动创建父队列转换为普通父队列/叶子队列
      if (oldQueue instanceof ManagedParentQueue
          && !(newQueue instanceof ManagedParentQueue)) {
        throw new IOException(
            "Cannot convert auto create enabled parent queue: "
                + oldQueue.getQueuePath() + " to leaf queue. Please check "
                + " parent queue's configuration "
                + CapacitySchedulerConfiguration.AUTO_CREATE_CHILD_QUEUE_ENABLED
                + " is set to true");
      }

      // 允许父队列转换为叶子队列，记录日志
      if (newQueue instanceof AbstractLeafQueue) {
        LOG.info("Converting the parent queue: {} to leaf queue.", oldQueue.getQueuePath());
      }
    }
  }

  /**
   * 验证叶子队列转换为父队列的合法性，只有停止状态才能转换
   * @param oldQueue 原有队列
   * @param newQueue 新配置中的队列
   * @throws IOException 非法转换抛出异常
   */
  private static void validateLeafQueueConversion(CSQueue oldQueue,
                                                  CSQueue newQueue) throws IOException {
    if (oldQueue instanceof AbstractLeafQueue && newQueue instanceof AbstractParentQueue) {
      if (isEitherQueueStopped(oldQueue.getState(), newQueue.getState())) {
        LOG.info("Converting the leaf queue: {} to parent queue.", oldQueue.getQueuePath());
      } else {
        // 非停止状态不允许转换
        throw new IOException(
            "Can not convert the leaf queue: " + oldQueue.getQueuePath()
                + " to parent queue since "
                + "it is not yet in stopped state. Current State : "
                + oldQueue.getState());
      }
    }
  }

  /**
   * 解析队列状态字符串，转换为QueueState枚举
   * @param state 状态字符串
   * @param queuePath 队列路径，用于日志输出
   * @return 解析后的队列状态，解析失败返回null
   */
  private static QueueState createQueueState(String state, String queuePath) {
    if (state != null) {
      try {
        return QueueState.valueOf(state);
      } catch (Exception ex) {
        LOG.warn("Not a valid queue state for queue: {}, state: {}", queuePath, state);
      }
    }
    return null;
  }

  /**
   * 判断队列是否为动态创建队列
   * @param csQueue 目标队列
   * @return 是动态队列返回true，否则返回false
   */
  private static boolean isDynamicQueue(CSQueue csQueue) {
    return ((AbstractCSQueue)csQueue).isDynamicQueue();
  }

  /**
   * 判断任意一个队列状态是否为停止状态
   * @param a 原有状态
   * @param b 新状态
   * @return 任意一个为停止状态返回true
   */
  private static boolean isEitherQueueStopped(QueueState a, QueueState b) {
    return a == QueueState.STOPPED || b == QueueState.STOPPED;
  }
}