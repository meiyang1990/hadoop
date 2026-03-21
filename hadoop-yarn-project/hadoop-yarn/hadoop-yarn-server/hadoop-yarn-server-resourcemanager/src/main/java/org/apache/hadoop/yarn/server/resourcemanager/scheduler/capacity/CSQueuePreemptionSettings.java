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

import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 容量调度队列抢占配置容器，存储单个队列及其层级的抢占相关配置。
 * 负责从配置中计算当前队列的跨队列抢占和队列内抢占最终状态，配置继承自父队列层级。
 */
public class CSQueuePreemptionSettings {
  private final boolean preemptionDisabled;
  // Indicates if the in-queue preemption setting is ever disabled within the
  // hierarchy of this queue.
  private final boolean intraQueuePreemptionDisabledInHierarchy;

  /**
   * 从队列和调度配置构建抢占配置，初始化抢占状态。
   * @param queue 当前队列
   * @param configuration 容量调度配置
   */
  public CSQueuePreemptionSettings(
      CSQueue queue,
      CapacitySchedulerConfiguration configuration) {
    this.preemptionDisabled = isQueueHierarchyPreemptionDisabled(queue, configuration);
    this.intraQueuePreemptionDisabledInHierarchy =
        isIntraQueueHierarchyPreemptionDisabled(queue, configuration);
  }

  /**
   * 检查队列层级中是否禁用了跨队列抢占，配置从父队列继承，本级可覆盖。
   * 全局抢占关闭时，所有队列默认禁用抢占。
   * @param q 待检查队列
   * @param configuration 容量调度配置
   * @return true表示队列跨队列抢占被禁用，false否则
   */
  private boolean isQueueHierarchyPreemptionDisabled(CSQueue q,
      CapacitySchedulerConfiguration configuration) {
    // 获取全局抢占总开关状态
    boolean systemWidePreemption =
        configuration
            .getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);
    CSQueue parentQ = q.getParent();

    // 全局抢占关闭，直接返回禁用
    if (!systemWidePreemption) return true;

    // 根队列，使用配置默认值false，无父队列继承
    if (parentQ == null) {
      return configuration.getPreemptionDisabled(q.getQueuePathObject(), false);
    }

    // 非根队列，默认值继承父队列禁用状态，本级配置覆盖父级
    return configuration.getPreemptionDisabled(q.getQueuePathObject(),
        parentQ.getPreemptionDisabled());
  }

  /**
   * 检查队列层级中是否禁用了队列内抢占，配置从父队列继承，本级可覆盖。
   * 全局队列内抢占关闭时，所有队列默认禁用队列内抢占。
   * @param q 待检查队列
   * @param configuration 容量调度配置
   * @return true表示队列内抢占被禁用，false否则
   */
  private boolean isIntraQueueHierarchyPreemptionDisabled(CSQueue q,
      CapacitySchedulerConfiguration configuration) {
    // 获取全局队列内抢占总开关状态
    boolean systemWideIntraQueuePreemption =
        configuration.getBoolean(
            CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED,
            CapacitySchedulerConfiguration
                .DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED);
    // 全局队列内抢占关闭，直接返回禁用
    if (!systemWideIntraQueuePreemption) return true;

    // 根队列，使用配置默认值false，无父队列继承
    CSQueue parentQ = q.getParent();
    if (parentQ == null) {
      return configuration
          .getIntraQueuePreemptionDisabled(q.getQueuePathObject(), false);
    }

    // 非根队列，默认值继承父队列层级禁用状态，本级配置覆盖父级
    return configuration.getIntraQueuePreemptionDisabled(q.getQueuePathObject(),
        parentQ.getIntraQueuePreemptionDisabledInHierarchy());
  }

  /**
   * 获取当前队列是否禁用队列内抢占。
   * @return true表示禁用，false表示启用
   */
  public boolean isIntraQueuePreemptionDisabled() {
    return intraQueuePreemptionDisabledInHierarchy || preemptionDisabled;
  }

  /**
   * 获取当前队列层级是否存在队列内抢占禁用配置。
   * @return true表示队列层级中已禁用队列内抢占
   */
  public boolean isIntraQueuePreemptionDisabledInHierarchy() {
    return intraQueuePreemptionDisabledInHierarchy;
  }

  /**
   * 获取当前队列是否禁用跨队列抢占。
   * @return true表示禁用，false表示启用
   */
  public boolean isPreemptionDisabled() {
    return preemptionDisabled;
  }
}