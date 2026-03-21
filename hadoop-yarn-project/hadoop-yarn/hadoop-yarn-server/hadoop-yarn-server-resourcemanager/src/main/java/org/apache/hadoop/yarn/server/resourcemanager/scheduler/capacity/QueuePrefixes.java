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

import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ACCESSIBLE_NODE_LABELS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;

/**
 * 容量调度器队列配置前缀工具类，提供各类队列相关配置路径前缀的生成方法
 */
public final class QueuePrefixes {

  private QueuePrefixes() {
  }

  /**
   * 获取指定队列的基础配置前缀
   * @param queuePath 队列路径对象
   * @return 队列配置前缀字符串
   */
  public static String getQueuePrefix(QueuePath queuePath) {
    return PREFIX + queuePath.getFullPath() + DOT;
  }

  /**
   * 获取指定队列指定节点标签的配置前缀
   * @param queuePath 队列路径对象
   * @param label 节点标签名称
   * @return 节点标签配置前缀字符串
   */
  public static String getNodeLabelPrefix(QueuePath queuePath, String label) {
    if (label.equals(CommonNodeLabelsManager.NO_LABEL)) {
      return getQueuePrefix(queuePath);
    }
    return getQueuePrefix(queuePath) + ACCESSIBLE_NODE_LABELS + DOT + label + DOT;
  }

  /**
   * Get the auto created leaf queue's template configuration prefix.
   * Leaf queue's template capacities are configured at the parent queue.
   *
   * @param queuePath parent queue's path
   * @return Config prefix for leaf queue template configurations
   */
  public static String getAutoCreatedQueueTemplateConfPrefix(QueuePath queuePath) {
    return queuePath.getFullPath() + DOT + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
  }

  /**
   * 获取自动创建叶子队列模板配置前缀的队列路径对象
   * @param queuePath 父队列路径对象
   * @return 自动创建队列模板配置前缀对应的QueuePath对象
   */
  public static QueuePath getAutoCreatedQueueObjectTemplateConfPrefix(QueuePath queuePath) {
    return new QueuePath(getAutoCreatedQueueTemplateConfPrefix(queuePath));
  }
}