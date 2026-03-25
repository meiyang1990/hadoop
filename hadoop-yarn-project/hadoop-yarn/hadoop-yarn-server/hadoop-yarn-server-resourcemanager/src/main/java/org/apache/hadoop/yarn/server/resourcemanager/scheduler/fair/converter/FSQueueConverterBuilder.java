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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

/**
 * Fair调度器队列配置转换为容量调度器配置的构建器，
 * 用于构造FSQueueConverter实例，遵循Builder设计模式
 */
@SuppressWarnings({"checkstyle:visibilitymodifier", "checkstyle:hiddenfield"})
public final class FSQueueConverterBuilder {
  FSConfigToCSConfigRuleHandler ruleHandler;
  CapacitySchedulerConfiguration capacitySchedulerConfig;
  boolean preemptionEnabled;
  boolean sizeBasedWeight;
  Resource clusterResource;
  float queueMaxAMShareDefault;
  int queueMaxAppsDefault;
  ConversionOptions conversionOptions;
  boolean drfUsed;
  boolean usePercentages;

  private FSQueueConverterBuilder() {
  }

  /**
   * 创建构建器实例
   * @return 构建器实例
   */
  public static FSQueueConverterBuilder create() {
    return new FSQueueConverterBuilder();
  }

  /**
   * 设置规则处理器
   * @param ruleHandler 规则处理器
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withRuleHandler(
      FSConfigToCSConfigRuleHandler ruleHandler) {
    this.ruleHandler = ruleHandler;
    return this;
  }

  /**
   * 设置目标容量调度器配置对象
   * @param capacitySchedulerConfig 容量调度器配置
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withCapacitySchedulerConfig(
      CapacitySchedulerConfiguration capacitySchedulerConfig) {
    this.capacitySchedulerConfig = capacitySchedulerConfig;
    return this;
  }

  /**
   * 设置抢占是否启用
   * @param preemptionEnabled 是否启用抢占
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withPreemptionEnabled(
      boolean preemptionEnabled) {
    this.preemptionEnabled = preemptionEnabled;
    return this;
  }

  /**
   * 设置是否基于队列大小计算权重
   * @param sizeBasedWeight 是否基于大小计算权重
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withSizeBasedWeight(
      boolean sizeBasedWeight) {
    this.sizeBasedWeight = sizeBasedWeight;
    return this;
  }

  /**
   * 设置集群总资源
   * @param resource 集群总资源
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withClusterResource(
      Resource resource) {
    this.clusterResource = resource;
    return this;
  }

  /**
   * 设置队列ApplicationMaster最大资源份额默认值
   * @param queueMaxAMShareDefault AM最大份额默认值
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withQueueMaxAMShareDefault(
      float queueMaxAMShareDefault) {
    this.queueMaxAMShareDefault = queueMaxAMShareDefault;
    return this;
  }

  /**
   * 设置队列最大运行应用数默认值
   * @param queueMaxAppsDefault 最大应用数默认值
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withQueueMaxAppsDefault(
      int queueMaxAppsDefault) {
    this.queueMaxAppsDefault = queueMaxAppsDefault;
    return this;
  }

  /**
   * 设置转换选项
   * @param conversionOptions 转换选项
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withConversionOptions(
      ConversionOptions conversionOptions) {
    this.conversionOptions = conversionOptions;
    return this;
  }

  /**
   * 设置是否使用DRF调度策略
   * @param drfUsed 是否使用DRF
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withDrfUsed(boolean drfUsed) {
    this.drfUsed = drfUsed;
    return this;
  }

  /**
   * 设置是否使用百分比配置
   * @param usePercentages 是否使用百分比
   * @return 当前构建器
   */
  public FSQueueConverterBuilder withPercentages(boolean usePercentages) {
    this.usePercentages = usePercentages;
    return this;
  }

  /**
   * 构造FSQueueConverter实例
   * @return 配置完成的转换实例
   */
  public FSQueueConverter build() {
    return new FSQueueConverter(this);
  }
}