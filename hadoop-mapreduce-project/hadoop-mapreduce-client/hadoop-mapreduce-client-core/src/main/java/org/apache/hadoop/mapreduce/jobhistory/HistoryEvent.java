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

package org.apache.hadoop.mapreduce.jobhistory;

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * MapReduce作业历史事件的顶层接口，定义所有作业历史事件需要实现的通用契约。
 * 实现类封装Avro生成的事件数据对象，提供统一的事件访问和转换接口，
 * 用于作业历史日志的序列化存储与时间线服务指标转换。
 * 核心职责：统一事件类型定义、封装底层Avro数据、支持转换为YARN时间线服务格式。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface HistoryEvent {

  /**
   * 获取当前事件的具体类型。
   * @return 事件类型枚举实例
   */
  EventType getEventType();

  /**
   * 获取当前实例封装的Avro数据对象。
   * @return 封装的Avro序列化数据对象
   */
  Object getDatum();

  /**
   * 设置当前实例封装的Avro数据对象。
   * @param datum 需要封装的Avro序列化数据对象
   */
  void setDatum(Object datum);

  /**
   * 将当前作业历史事件转换为YARN时间线服务可识别的TimelineEvent对象。
   * 用于将MapReduce作业事件同步到YARN时间线服务进行统一存储与查询。
   *
   * @return 转换后的时间线事件对象
   */
  TimelineEvent toTimelineEvent();

  /**
   * 获取当前事件关联的时间线指标集合。
   * 用于提取事件携带的计数器指标数据，上报给YARN时间线服务。
   *
   * @return 时间线指标集合，若无指标则返回null
   */
  Set<TimelineMetric> getTimelineMetrics();
}