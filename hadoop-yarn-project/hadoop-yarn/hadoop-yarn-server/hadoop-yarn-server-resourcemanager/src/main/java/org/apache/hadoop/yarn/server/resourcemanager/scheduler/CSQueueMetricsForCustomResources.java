// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE
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
 * 容量调度器自定义资源队列指标入口，负责管理自定义资源相关容量指标的存储与获取
 * 提供保障容量、最大容量等自定义资源指标的增删改查能力
 */
public class CSQueueMetricsForCustomResources
    extends QueueMetricsForCustomResources {
  // 自定义资源保障容量指标存储
  private final CustomResourceMetricValue guaranteedCapacity =
      new CustomResourceMetricValue();
  // 自定义资源最大容量指标存储
  private final CustomResourceMetricValue maxCapacity =
      new CustomResourceMetricValue();

  /**
   * 设置自定义资源保障容量指标值
   * @param res 待设置的资源值
   */
  public void setGuaranteedCapacity(Resource res) {
    guaranteedCapacity.set(res);
  }

  /**
   * 设置自定义资源最大容量指标值
   * @param res 待设置的资源值
   */
  public void setMaxCapacity(Resource res) {
    maxCapacity.set(res);
  }

  /**
   * 获取所有自定义资源保障容量指标值
   * @return 资源名称->指标值的映射
   */
  public Map<String, Long> getGuaranteedCapacity() {
    return guaranteedCapacity.getValues();
  }

  /**
   * 获取所有自定义资源最大容量指标值
   * @return 资源名称->指标值的映射
   */
  public Map<String, Long> getMaxCapacity() {
    return maxCapacity.getValues();
  }
}