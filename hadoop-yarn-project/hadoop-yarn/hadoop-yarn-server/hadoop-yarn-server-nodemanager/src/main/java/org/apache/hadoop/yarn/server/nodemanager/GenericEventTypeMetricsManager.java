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
package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.metrics.GenericEventTypeMetrics;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * 通用事件类型指标管理器，为NodeManager中的事件分发器创建并注册按事件类型分类的指标
 * 用于统计不同类型事件的处理情况，辅助监控事件调度性能
 */
public final class GenericEventTypeMetricsManager {

  private GenericEventTypeMetricsManager() {
      // 工具类不允许实例化
  }

  /**
   * 为指定事件分发器创建并注册通用事件类型指标
   * @param dispatcherName 事件分发器名称，用于指标描述
   * @param eventTypeClass 事件类型枚举类，用于提取所有事件类型生成对应指标
   * @return 构建完成并已注册的通用事件类型指标实例
   * @param <T> 事件类型枚举类型
   */
  // Construct a GenericEventTypeMetrics for dispatcher
  @SuppressWarnings("unchecked")
  public static <T extends Enum<T>> GenericEventType
      create(String dispatcherName, Class<T> eventTypeClass) {
    return new GenericEventTypeMetrics.EventTypeMetricsBuilder<T>()
        // 设置指标系统实例
        .setMs(DefaultMetricsSystem.instance())
        // 设置指标元信息，包含名称和描述
        .setInfo(info("GenericEventTypeMetrics for " + eventTypeClass.getName(),
            "Metrics for " + dispatcherName))
        // 设置事件类型枚举类
        .setEnumClass(eventTypeClass)
        // 设置所有枚举常量，为每个事件类型生成对应指标
        .setEnums(eventTypeClass.getEnumConstants())
        // 构建指标并完成注册到指标系统
        .build().registerMetrics();
  }
}