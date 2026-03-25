// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.queuemanagement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueManagementChange;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * 存储自动创建叶队列的容量 entitlement 配置，
 * 用于管理动态队列的资源权限变更，支撑队列管理操作。
 */
public class LeafQueueEntitlements {
  // 按队列路径存储各叶队列的容量配置
  private final Map<String, QueueCapacities> entitlements = new HashMap<>();

  /**
   * 根据叶队列对象获取其容量配置。
   * @param leafQueue 自动创建的叶队列对象
   * @return 该队列的容量配置
   */
  public QueueCapacities getCapacityOfQueue(AutoCreatedLeafQueue leafQueue) {
    return getCapacityOfQueueByPath(leafQueue.getQueuePath());
  }

  /**
   * 根据队列路径获取容量配置，不存在则初始化空配置。
   * @param leafQueuePath 叶队列完整路径
   * @return 该队列的容量配置
   */
  public QueueCapacities getCapacityOfQueueByPath(String leafQueuePath) {
    if (!entitlements.containsKey(leafQueuePath)) {
      entitlements.put(leafQueuePath, new QueueCapacities(false));
    }
    return entitlements.get(leafQueuePath);
  }

  /**
   * 获取所有叶队列的容量配置集合。
   * @return 队列路径到容量配置的映射
   */
  public Map<String, QueueCapacities> getEntitlements() {
    return entitlements;
  }

  /**
   * 将所有队列容量配置转换为队列管理变更列表。
   * @param func 转换函数，输入队列路径和容量配置，输出队列管理变更对象
   * @return 队列管理变更列表
   */
  public List<QueueManagementChange> mapToQueueManagementChanges(
      BiFunction<String, QueueCapacities, QueueManagementChange> func) {
    return entitlements.entrySet().stream().map(e -> func.apply(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }
}