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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.yarn.api.records.Resource;

/**
 * 公平调度器上下文容器，用于在公平调度器各个类之间传递核心调度信息。
 * 相当于一个保存调度器关键状态的结构化容器，方便各个模块共享调度核心信息。
 */
public class FSContext {
  // 抢占相关信息
  private boolean preemptionEnabled = false;
  private float preemptionUtilizationThreshold;
  private FSStarvedApps starvedApps;
  private final FairScheduler scheduler;

  /**
   * 构造上下文对象，关联所属的公平调度器实例。
   * @param scheduler 所属公平调度器
   */
  FSContext(FairScheduler scheduler) {
    this.scheduler = scheduler;
  }

  /**
   * 获取抢占功能是否启用。
   * @return 抢占启用状态
   */
  boolean isPreemptionEnabled() {
    return preemptionEnabled;
  }

  /**
   * 启用抢占功能，延迟初始化饥饿应用队列。
   */
  void setPreemptionEnabled() {
    this.preemptionEnabled = true;
    if (starvedApps == null) {
      starvedApps = new FSStarvedApps();
    }
  }

  /**
   * 获取饥饿应用管理器，用于管理等待资源抢占的应用。
   * @return 饥饿应用管理器实例
   */
  FSStarvedApps getStarvedApps() {
    return starvedApps;
  }

  /**
   * 获取抢占资源利用率阈值，集群利用率低于该阈值才会触发抢占。
   * @return 抢占资源利用率阈值
   */
  float getPreemptionUtilizationThreshold() {
    return preemptionUtilizationThreshold;
  }

  /**
   * 设置抢占资源利用率阈值。
   * @param preemptionUtilizationThreshold 抢占资源利用率阈值
   */
  void setPreemptionUtilizationThreshold(
      float preemptionUtilizationThreshold) {
    this.preemptionUtilizationThreshold = preemptionUtilizationThreshold;
  }

  /**
   * 获取集群总资源量，从关联的公平调度器获取。
   * @return 集群总资源
   */
  public Resource getClusterResource() {
    return scheduler.getClusterResource();
  }
}