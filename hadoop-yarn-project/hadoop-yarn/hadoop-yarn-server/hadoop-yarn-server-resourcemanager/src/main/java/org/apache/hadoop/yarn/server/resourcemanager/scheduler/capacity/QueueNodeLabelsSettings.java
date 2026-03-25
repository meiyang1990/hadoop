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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import java.io.IOException;
import java.util.Set;

/**
 * 容量调度器队列节点标签设置管理类，根据配置和队列属性计算可访问节点标签、配置节点标签和默认节点标签表达式。
 */
public class QueueNodeLabelsSettings {
  private final CSQueue parent;
  private final QueuePath queuePath;
  private Set<String> accessibleLabels;
  private Set<String> configuredNodeLabels;
  private String defaultLabelExpression;

  /**
   * 构造队列节点标签设置，初始化并验证所有标签配置。
   * @param configuration 容量调度器配置
   * @param parent 父队列，根队列为null
   * @param queuePath 当前队列路径
   * @param configuredNodeLabels 预解析的节点标签配置
   * @throws IOException 标签配置验证失败时抛出
   */
  public QueueNodeLabelsSettings(CapacitySchedulerConfiguration configuration,
      CSQueue parent,
      QueuePath queuePath,
      ConfiguredNodeLabels configuredNodeLabels) throws IOException {
    this.parent = parent;
    this.queuePath = queuePath;
    initializeNodeLabels(configuration, configuredNodeLabels);
  }

  /**
   * 按顺序初始化所有标签配置并完成验证。
   * @param configuration 容量调度器配置
   * @param configuredNodeLabels 预解析的节点标签配置
   * @throws IOException 标签配置验证失败时抛出
   */
  private void initializeNodeLabels(CapacitySchedulerConfiguration configuration,
      ConfiguredNodeLabels configuredNodeLabels)
      throws IOException {
    initializeAccessibleLabels(configuration);
    initializeDefaultLabelExpression(configuration);
    initializeConfiguredNodeLabels(configuration, configuredNodeLabels);
    validateNodeLabels();
  }

  /**
   * 初始化当前队列可访问节点标签，未配置则从父队列继承。
   * @param configuration 容量调度器配置
   */
  private void initializeAccessibleLabels(CapacitySchedulerConfiguration configuration) {
    this.accessibleLabels = configuration.getAccessibleNodeLabels(queuePath);
    // 未配置可访问标签，从父队列继承
    if (this.accessibleLabels == null && parent != null) {
      this.accessibleLabels = parent.getAccessibleNodeLabels();
    }
  }

  /**
   * 初始化当前队列默认节点标签表达式，满足条件则从父队列继承。
   * @param configuration 容量调度器配置
   */
  private void initializeDefaultLabelExpression(CapacitySchedulerConfiguration configuration) {
    this.defaultLabelExpression = configuration.getDefaultNodeLabelExpression(
        queuePath);
    // 当前队列已配置可访问标签、未单独配置默认表达式、且当前可访问标签包含父队列所有可访问标签时，继承父队列默认表达式
    if (this.accessibleLabels != null && parent != null
        && this.defaultLabelExpression == null &&
        this.accessibleLabels.containsAll(parent.getAccessibleNodeLabels())) {
      this.defaultLabelExpression = parent.getDefaultNodeLabelExpression();
    }
  }

  /**
   * 初始化当前队列已配置节点标签，优先使用预解析结果，否则回退到从配置读取。
   * @param configuration 容量调度器配置
   * @param configuredNodeLabelsParam 预解析的节点标签配置
   */
  private void initializeConfiguredNodeLabels(CapacitySchedulerConfiguration configuration,
      ConfiguredNodeLabels configuredNodeLabelsParam) {
    if (configuredNodeLabelsParam != null) {
      if (queuePath.isRoot()) {
        // 根队列获取所有已配置标签
        this.configuredNodeLabels = configuredNodeLabelsParam.getAllConfiguredLabels();
      } else {
        // 子队列获取自身路径对应的已配置标签
        this.configuredNodeLabels = configuredNodeLabelsParam.getLabelsByQueue(
            queuePath.getFullPath());
      }
    } else {
      // 无预解析结果，回退到直接从配置读取
      this.configuredNodeLabels = configuration.getConfiguredNodeLabels(queuePath);
    }
  }

  /**
   * 验证队列节点标签配置合法性，要求子队列标签必须是父队列标签的子集。
   * @throws IOException 验证不通过时抛出异常
   */
  private void validateNodeLabels() throws IOException {
    // 仅对非根队列进行验证
    if (!queuePath.isRoot()) {
      if (parent.getAccessibleNodeLabels() != null && !parent
          .getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
        // 父队列未开放所有标签，子队列不能设置为开放所有标签
        if (this.getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
          throw new IOException("Parent's accessible queue is not ANY(*), "
              + "but child's accessible queue is " + RMNodeLabelsManager.ANY);
        } else {
          // 检查子队列所有标签都属于父队列可访问标签集合
          Set<String> diff = Sets.difference(this.getAccessibleNodeLabels(),
              parent.getAccessibleNodeLabels());
          if (!diff.isEmpty()) {
            throw new IOException(String.format(
                "Some labels of child queue is not a subset of parent queue, these labels=[%s]",
                StringUtils.join(diff, ",")));
          }
        }
      }
    }
  }

  /**
   * 检查当前队列是否有权限访问指定节点分区（标签）。
   * @param nodePartition 待检查的节点分区标签
   * @return true 允许访问，false 不允许访问
   */
  public boolean isAccessibleToPartition(String nodePartition) {
    // 队列可访问任意节点标签，直接允许访问
    if (accessibleLabels != null && accessibleLabels.contains(RMNodeLabelsManager.ANY)) {
      return true;
    }
    // 任何队列都可以访问无标签的节点，直接允许访问
    if (nodePartition == null || nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      return true;
    }
    // 当前队列可访问标签包含该分区，允许访问
    if (accessibleLabels != null && accessibleLabels.contains(nodePartition)) {
      return true;
    }
    // 不满足任何允许条件，拒绝访问
    return false;
  }

  public Set<String> getAccessibleNodeLabels() {
    return accessibleLabels;
  }

  public Set<String> getConfiguredNodeLabels() {
    return configuredNodeLabels;
  }

  public String getDefaultLabelExpression() {
    return defaultLabelExpression;
  }
}