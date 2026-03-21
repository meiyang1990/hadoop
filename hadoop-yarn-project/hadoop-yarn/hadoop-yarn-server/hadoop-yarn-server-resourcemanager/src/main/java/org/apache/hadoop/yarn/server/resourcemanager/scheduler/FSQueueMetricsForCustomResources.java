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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.metrics.CustomResourceMetricValue;

import java.util.Map;

/**
 * 公平调度队列自定义资源指标容器，承载自定义资源各类调度指标的统计数据，
 * 提供各类指标的增删改查访问入口，用于YARN监控系统采集自定义资源的调度状态。
 */
public class FSQueueMetricsForCustomResources {
  // 当前瞬时公平份额指标
  private final CustomResourceMetricValue
      fairShare = new CustomResourceMetricValue();
  // 稳定公平份额指标
  private final CustomResourceMetricValue steadyFairShare =
      new CustomResourceMetricValue();
  // 最小资源份额指标
  private final CustomResourceMetricValue
      minShare = new CustomResourceMetricValue();
  // 最大资源份额指标
  private final CustomResourceMetricValue
      maxShare = new CustomResourceMetricValue();
  // ApplicationMaster最大资源份额指标
  private final CustomResourceMetricValue
      maxAMShare = new CustomResourceMetricValue();
  // ApplicationMaster已使用资源指标
  private final CustomResourceMetricValue amResourceUsage =
      new CustomResourceMetricValue();

  /**
   * 获取当前瞬时公平份额指标对象。
   * @return 瞬时公平份额指标
   */
  public CustomResourceMetricValue getFairShare() {
    return fairShare;
  }

  /**
   * 设置当前瞬时公平份额。
   * @param res 资源对象，包含各自定义资源的公平份额值
   */
  public void setFairShare(Resource res) {
    fairShare.set(res);
  }

  /**
   * 获取当前瞬时公平份额各资源的指标值。
   * @return 资源名称到份额值的映射
   */
  public Map<String, Long> getFairShareValues() {
    return fairShare.getValues();
  }

  /**
   * 获取稳定公平份额指标对象。
   * @return 稳定公平份额指标
   */
  public CustomResourceMetricValue getSteadyFairShare() {
    return steadyFairShare;
  }

  /**
   * 设置稳定公平份额。
   * @param res 资源对象，包含各自定义资源的稳定公平份额值
   */
  public void setSteadyFairShare(Resource res) {
    steadyFairShare.set(res);
  }

  /**
   * 获取稳定公平份额各资源的指标值。
   * @return 资源名称到份额值的映射
   */
  public Map<String, Long> getSteadyFairShareValues() {
    return steadyFairShare.getValues();
  }

  /**
   * 获取最小资源份额指标对象。
   * @return 最小资源份额指标
   */
  public CustomResourceMetricValue getMinShare() {
    return minShare;
  }

  /**
   * 设置最小资源份额。
   * @param res 资源对象，包含各自定义资源的最小份额值
   */
  public void setMinShare(Resource res) {
    minShare.set(res);
  }

  /**
   * 获取最小资源份额各资源的指标值。
   * @return 资源名称到份额值的映射
   */
  public Map<String, Long> getMinShareValues() {
    return minShare.getValues();
  }

  /**
   * 获取最大资源份额指标对象。
   * @return 最大资源份额指标
   */
  public CustomResourceMetricValue getMaxShare() {
    return maxShare;
  }

  /**
   * 设置最大资源份额。
   * @param res 资源对象，包含各自定义资源的最大份额值
   */
  public void setMaxShare(Resource res) {
    maxShare.set(res);
  }

  /**
   * 获取最大资源份额各资源的指标值。
   * @return 资源名称到份额值的映射
   */
  public Map<String, Long> getMaxShareValues() {
    return maxShare.getValues();
  }

  /**
   * 获取ApplicationMaster最大资源份额指标对象。
   * @return AM最大资源份额指标
   */
  public CustomResourceMetricValue getMaxAMShare() {
    return maxAMShare;
  }

  /**
   * 设置ApplicationMaster最大资源份额。
   * @param res 资源对象，包含各自定义资源的AM最大份额值
   */
  public void setMaxAMShare(Resource res) {
    maxAMShare.set(res);
  }

  /**
   * 获取ApplicationMaster最大资源份额各资源的指标值。
   * @return 资源名称到份额值的映射
   */
  public Map<String, Long> getMaxAMShareValues() {
    return maxAMShare.getValues();
  }

  /**
   * 获取ApplicationMaster已使用资源指标对象。
   * @return AM已使用资源指标
   */
  public CustomResourceMetricValue getAMResourceUsage() {
    return amResourceUsage;
  }

  /**
   * 设置ApplicationMaster已使用资源量。
   * @param res 资源对象，包含各自定义资源的AM已使用量
   */
  public void setAMResourceUsage(Resource res) {
    amResourceUsage.set(res);
  }

  /**
   * 获取ApplicationMaster已使用资源各资源的指标值。
   * @return 资源名称到已使用值的映射
   */
  public Map<String, Long> getAMResourceUsageValues() {
    return amResourceUsage.getValues();
  }
}