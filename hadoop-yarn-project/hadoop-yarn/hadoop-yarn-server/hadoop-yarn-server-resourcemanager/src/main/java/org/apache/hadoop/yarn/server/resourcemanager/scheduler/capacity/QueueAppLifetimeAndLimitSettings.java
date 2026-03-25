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

import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * 容量调度器队列的应用生命周期和最大并行应用数配置容器，负责从配置中继承并计算队列最终的配置值
 * 基于队列层级结构实现配置继承，支持子队列覆盖或继承父队列配置
 **/
public class QueueAppLifetimeAndLimitSettings {
  // -1 表示禁用应用生命周期限制
  private final long maxApplicationLifetime;
  private final long defaultApplicationLifetime;

  // 标记默认生命周期是否在当前队列或其层级祖先中被显式配置过
  private boolean defaultAppLifetimeWasSpecifiedInConfig = false;

  // 队列最大并行应用数
  private int maxParallelApps;

  /**
   * 构造函数，基于配置和队列路径计算最终的应用生命周期与并行应用限制配置
   * @param configuration 容量调度器配置
   * @param q 当前队列
   * @param queuePath 队列层级路径
   */
  public QueueAppLifetimeAndLimitSettings(CapacitySchedulerConfiguration configuration,
      AbstractCSQueue q, QueuePath queuePath) {
    // 读取队列最大并行应用数配置
    this.maxParallelApps = configuration.getMaxParallelAppsForQueue(queuePath);
    // 计算继承后的最大应用生命周期
    this.maxApplicationLifetime = getInheritedMaxAppLifetime(q, configuration);
    // 计算继承后的默认应用生命周期
    this.defaultApplicationLifetime = setupInheritedDefaultAppLifetime(q, queuePath, configuration,
        maxApplicationLifetime);
  }

  /**
   * 基于队列层级继承计算最终的最大应用生命周期
   * @param q 当前队列
   * @param conf 容量调度器配置
   * @return 最终最大应用生命周期值
   */
  private long getInheritedMaxAppLifetime(CSQueue q, CapacitySchedulerConfiguration conf) {
    CSQueue parentQ = q.getParent();
    long maxAppLifetime = conf.getMaximumLifetimePerQueue(q.getQueuePathObject());

    // 根队列直接返回自身配置
    if (q.getQueuePathObject().isRoot()) {
      return maxAppLifetime;
    }

    // 非根队列：当前队列配置>=0则使用当前配置，否则继承父队列配置
    // 0表示禁用最大生命周期限制，会覆盖父队列配置；负值表示继承父队列
    long parentsMaxAppLifetime = parentQ.getMaximumApplicationLifetime();
    return (maxAppLifetime >= 0) ? maxAppLifetime : parentsMaxAppLifetime;
  }

  /**
   * 基于队列层级继承计算最终的默认应用生命周期，同时验证配置合法性
   * @param q 当前队列
   * @param queuePath 队列层级路径
   * @param conf 容量调度器配置
   * @param myMaxAppLifetime 当前队列已确定的最大应用生命周期
   * @return 最终默认应用生命周期值
   */
  private long setupInheritedDefaultAppLifetime(CSQueue q,
      QueuePath queuePath, CapacitySchedulerConfiguration conf, long myMaxAppLifetime) {
    CSQueue parentQ = q.getParent();
    long defaultAppLifetime = conf.getDefaultLifetimePerQueue(queuePath);
    // 标记默认生命周期是否在当前层级或祖先层级被显式配置
    defaultAppLifetimeWasSpecifiedInConfig =
        (defaultAppLifetime >= 0
            || (!queuePath.isRoot() &&
            parentQ.getDefaultAppLifetimeWasSpecifiedInConfig()));

    // 根队列直接返回自身配置
    if (queuePath.isRoot()) {
      return defaultAppLifetime;
    }

    // 获取父队列的默认应用生命周期
    long parentsDefaultAppLifetime = parentQ.getDefaultApplicationLifetime();

    // 当前队列未配置默认生命周期，需要根据继承规则计算
    if (defaultAppLifetime < 0) {
      // 祖先层级已配置过默认生命周期，取父默认值和当前最大生命周期的较小值
      if (defaultAppLifetimeWasSpecifiedInConfig) {
        defaultAppLifetime =
            Math.min(parentsDefaultAppLifetime, myMaxAppLifetime);
      } else {
        // 整个层级都没有配置过默认生命周期，使用当前队列的最大生命周期作为默认值
        defaultAppLifetime = myMaxAppLifetime;
      }
    } // 当前队列已配置(>=0)，直接使用配置值

    // 校验默认生命周期不能大于最大生命周期，配置不合法抛出异常
    if (myMaxAppLifetime > 0 && defaultAppLifetime > myMaxAppLifetime) {
      throw new YarnRuntimeException(
          "Default lifetime " + defaultAppLifetime
              + " can't exceed maximum lifetime " + myMaxAppLifetime);
    }

    // 默认生命周期小于等于0时，使用当前队列最大生命周期作为默认值
    if (defaultAppLifetime <= 0) {
      defaultAppLifetime = myMaxAppLifetime;
    }
    return defaultAppLifetime;
  }

  /**
   * 获取队列最大并行应用数
   * @return 最大并行应用数
   */
  public int getMaxParallelApps() {
    return maxParallelApps;
  }

  /**
   * 设置队列最大并行应用数
   * @param maxParallelApps 最大并行应用数
   */
  public void setMaxParallelApps(int maxParallelApps) {
    this.maxParallelApps = maxParallelApps;
  }

  /**
   * 获取队列最大应用生命周期
   * @return 最大应用生命周期（毫秒）
   */
  public long getMaxApplicationLifetime() {
    return maxApplicationLifetime;
  }

  /**
   * 获取队列默认应用生命周期
   * @return 默认应用生命周期（毫秒）
   */
  public long getDefaultApplicationLifetime() {
    return defaultApplicationLifetime;
  }

  /**
   * 查询默认生命周期是否在队列层级中被显式配置过
   * @return true表示已配置，false表示全层级都未配置
   */
  public boolean isDefaultAppLifetimeWasSpecifiedInConfig() {
    return defaultAppLifetimeWasSpecifiedInConfig;
  }
}