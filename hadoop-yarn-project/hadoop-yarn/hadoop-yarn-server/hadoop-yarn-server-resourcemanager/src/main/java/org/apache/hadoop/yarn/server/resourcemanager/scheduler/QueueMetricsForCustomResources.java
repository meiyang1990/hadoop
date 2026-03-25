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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.metrics.CustomResourceMetrics;
import org.apache.hadoop.yarn.metrics.CustomResourceMetricValue;

import java.util.Map;

/**
 * 队列自定义资源指标管理器，负责统计和维护YARN队列中各种自定义资源的指标数据
 * 包括待分配资源、预留资源、抢占资源的统计，供监控系统和调度器使用
 */
public class QueueMetricsForCustomResources extends CustomResourceMetrics {
  // 累计抢占资源时间统计，单位：秒
  private final CustomResourceMetricValue aggregatePreemptedSeconds =
      new CustomResourceMetricValue();
  // 累计抢占资源量统计
  private final CustomResourceMetricValue aggregatePreempted =
      new CustomResourceMetricValue();
  // 待分配资源量统计（等待调度的资源）
  private final CustomResourceMetricValue pending =
      new CustomResourceMetricValue();
  // 已预留资源量统计（为容器预留但未分配的资源）
  private final CustomResourceMetricValue reserved =
      new CustomResourceMetricValue();

  /**
   * 增加预留资源量
   * @param res 要增加的资源
   */
  public void increaseReserved(Resource res) {
    reserved.increase(res);
  }

  /**
   * 减少预留资源量
   * @param res 要减少的资源
   */
  public void decreaseReserved(Resource res) {
    reserved.decrease(res);
  }

  /**
   * 增加待分配资源量，按容器数量倍乘计算
   * @param res 单个容器的资源
   * @param containers 容器数量
   */
  public void increasePending(Resource res, int containers) {
    pending.increaseWithMultiplier(res, containers);
  }

  /**
   * 减少待分配资源量
   * @param res 要减少的资源
   */
  public void decreasePending(Resource res) {
    pending.decrease(res);
  }

  /**
   * 减少待分配资源量，按容器数量倍乘计算
   * @param res 单个容器的资源
   * @param containers 容器数量
   */
  public void decreasePending(Resource res, int containers) {
    pending.decreaseWithMultiplier(res, containers);
  }

  /**
   * 获取所有自定义资源的待分配指标值
   * @return 资源名称到指标值的映射
   */
  public Map<String, Long> getPendingValues() {
    return pending.getValues();
  }

  /**
   * 获取所有自定义资源的预留指标值
   * @return 资源名称到指标值的映射
   */
  public Map<String, Long> getReservedValues() {
    return reserved.getValues();
  }

  /**
   * 增加累计抢占资源时间，按抢占时长倍乘计算
   * @param res 被抢占的资源
   * @param seconds 抢占时长
   */
  public void increaseAggregatedPreemptedSeconds(Resource res, long seconds) {
    aggregatePreemptedSeconds.increaseWithMultiplier(res, seconds);
  }

  /**
   * 增加累计抢占资源量
   * @param res 被抢占的资源
   */
  public void increaseAggregatedPreempted(Resource res) {
    aggregatePreempted.increase(res);
  }

  /**
   * 获取累计抢占资源时间指标对象
   * @return 累计抢占时间指标
   */
  CustomResourceMetricValue getAggregatePreemptedSeconds() {
    return aggregatePreemptedSeconds;
  }
}