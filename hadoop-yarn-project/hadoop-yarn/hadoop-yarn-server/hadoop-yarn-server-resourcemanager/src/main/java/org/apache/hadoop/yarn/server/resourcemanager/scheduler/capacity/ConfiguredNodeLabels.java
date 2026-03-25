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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 容量调度器中从配置文件提取的所有队列节点标签容器。
 * 凡是配置了accessible-node-labels前缀属性的队列，都会在此记录其允许使用的节点标签。
 * 示例配置: yarn.scheduler.capacity.root.accessible-node-labels.test-label.capacity
 */
public class ConfiguredNodeLabels {
  // 按队列路径存储该队列配置的可访问节点标签
  private final Map<String, Set<String>> configuredNodeLabelsByQueue;
  // 空标签集合，代表队列未配置任何标签时使用默认值（仅包含空标签）
  private static final Set<String> NO_LABEL =
      ImmutableSet.of(RMNodeLabelsManager.NO_LABEL);

  /**
   * 构造空的配置节点标签容器.
   */
  public ConfiguredNodeLabels() {
    configuredNodeLabelsByQueue = new HashMap<>();
  }

  /**
   * 从容量调度器配置中加载队列节点标签配置.
   * @param conf 容量调度器配置对象
   */
  public ConfiguredNodeLabels(
      CapacitySchedulerConfiguration conf) {
    this.configuredNodeLabelsByQueue = conf.getConfiguredNodeLabelsByQueue();
  }

  /**
   * 获取指定队列配置的可访问节点标签集合。如果队列未配置任何标签，
   * 则返回仅包含空标签的不可变集合作为默认值。
   * @param queuePath 队列全路径
   * @return 该队列配置的可访问节点标签，无配置则返回仅含空标签的集合
   */
  public Set<String> getLabelsByQueue(String queuePath) {
    Set<String> labels = configuredNodeLabelsByQueue.get(queuePath);

    if (labels == null) {
      return NO_LABEL;
    }

    return ImmutableSet.copyOf(labels);
  }

  /**
   * 为指定队列设置可访问节点标签.
   * @param queuePath 队列全路径
   * @param nodeLabels 需要设置的配置节点标签集合
   */
  public void setLabelsByQueue(
      String queuePath, Collection<String> nodeLabels) {
    configuredNodeLabelsByQueue.put(queuePath, new HashSet<>(nodeLabels));
  }

  /**
   * 获取所有队列配置中出现过的全部节点标签集合.
   * @return 所有队列配置聚合后的全部节点标签，无配置则返回仅含空标签的集合
   */
  public Set<String> getAllConfiguredLabels() {
    Set<String> nodeLabels = configuredNodeLabelsByQueue.values().stream()
        .flatMap(Set::stream).collect(Collectors.toSet());

    if (nodeLabels.size() == 0) {
      nodeLabels = NO_LABEL;
    }

    return nodeLabels;
  }
}