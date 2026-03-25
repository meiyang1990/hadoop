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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;

/**
 * 自动创建叶子队列的配置容器，存储自动创建叶子队列的容量和相关配置信息
 */
public class AutoCreatedLeafQueueConfig {

  /**
   * 队列容量信息，包含配置值和计算后的绝对容量，供自动队列创建策略使用
   */
  private QueueCapacities queueCapacities;

  /**
   * 叶子队列的调度配置信息
   */
  private CapacitySchedulerConfiguration leafQueueConfigs;

  /**
   * 队列资源配额信息
   */
  private final QueueResourceQuotas resourceQuotas;

  /**
   * 通过Builder构造配置对象
   * @param builder 构造器
   */
  public AutoCreatedLeafQueueConfig(Builder builder) {
    this.queueCapacities = builder.queueCapacities;
    this.leafQueueConfigs = builder.leafQueueConfigs;
    this.resourceQuotas = builder.queueResourceQuotas;
  }

  /**
   * AutoCreatedLeafQueueConfig 构造器，用于分步构建配置对象
   */
  public static class Builder {

    private QueueCapacities queueCapacities;
    private CapacitySchedulerConfiguration leafQueueConfigs;
    private QueueResourceQuotas queueResourceQuotas;

    /**
     * 设置队列容量信息
     * @param capacities 队列容量
     * @return 当前构造器实例
     */
    public Builder capacities(QueueCapacities capacities) {
      this.queueCapacities = capacities;
      return this;
    }

    /**
     * 设置队列调度配置
     * @param conf 容量调度器配置
     * @return 当前构造器实例
     */
    public Builder configuration(CapacitySchedulerConfiguration conf) {
      this.leafQueueConfigs = conf;
      return this;
    }

    /**
     * 构建自动创建叶子队列配置对象
     * @return 构造完成的配置对象
     */
    public AutoCreatedLeafQueueConfig build() {
      return new AutoCreatedLeafQueueConfig(this);
    }

    /**
     * 设置队列资源配额
     * @param queueResourceQuotas 资源配额对象
     * @return 当前构造器实例
     */
    public Builder resourceQuotas(QueueResourceQuotas queueResourceQuotas) {
      this.queueResourceQuotas = queueResourceQuotas;
      return this;
    }
  }

  /**
   * 获取队列容量信息
   * @return 队列容量对象
   */
  public QueueCapacities getQueueCapacities() {
    return queueCapacities;
  }

  /**
   * 获取叶子队列调度配置
   * @return 容量调度器配置
   */
  public CapacitySchedulerConfiguration getLeafQueueConfigs() {
    return leafQueueConfigs;
  }

  /**
   * 获取队列资源配额
   * @return 资源配额对象
   */
  public QueueResourceQuotas getResourceQuotas() {
    return resourceQuotas;
  }

  @Override
  public String toString() {
    return "AutoCreatedLeafQueueConfig{" + "queueCapacities=" + queueCapacities
        + ", leafQueueConfigs=" + leafQueueConfigs + '}';
  }
}