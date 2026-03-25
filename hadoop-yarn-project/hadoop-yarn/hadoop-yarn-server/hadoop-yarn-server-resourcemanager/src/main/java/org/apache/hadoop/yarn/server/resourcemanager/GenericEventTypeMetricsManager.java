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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.metrics.GenericEventTypeMetrics;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * 通用事件类型指标管理器，为YARN ResourceManager中的调度器创建并注册事件类型统计指标
 * 用于对不同类型事件的发生次数进行统计监控
 */
public final class GenericEventTypeMetricsManager {

  private GenericEventTypeMetricsManager() {
    // 工具类禁止实例化
  }

  /**
   * 为指定分发器创建并注册通用事件类型指标
   * @param dispatcherName 分发器名称，用于指标描述
   * @param eventTypeClass 事件类型枚举类
   * @param <T> 事件枚举类型
   * @return 构建完成并已注册的通用事件类型指标实例
   */
  public static <T extends Enum<T>> GenericEventTypeMetrics
      create(String dispatcherName, Class<T> eventTypeClass) {
    // 创建指标元信息，包含指标名称和描述
    MetricsInfo metricsInfo = info("GenericEventTypeMetrics for " + eventTypeClass.getName(),
        "Metrics for " + dispatcherName);
    // 使用Builder模式构建指标实例并注册到默认指标系统
    return new GenericEventTypeMetrics.EventTypeMetricsBuilder<T>()
        .setMs(DefaultMetricsSystem.instance())
        .setInfo(metricsInfo)
        .setEnumClass(eventTypeClass)
        .setEnums(eventTypeClass.getEnumConstants())
        .build().registerMetrics();
  }
}