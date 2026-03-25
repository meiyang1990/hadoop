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

package org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.entity;

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;

import java.util.Map;
import java.util.TreeMap;

/**
 *  TimelineEntityDocument 文档中用于存储 TimelineMetric 的子文档，适配文档存储的存储结构。
 */
public class TimelineMetricSubDoc {

  // 被包装的原始时间线指标对象
  private final TimelineMetric timelineMetric;
  // 单值指标的时间戳
  private long singleDataTimestamp;
  // 单值指标的数值，默认初始化为0
  private Number singleDataValue = 0;

  /**
   * 无参构造函数，初始化空的时间线指标。
   */
  public TimelineMetricSubDoc() {
    this.timelineMetric = new TimelineMetric();
  }

  /**
   * 基于已有时间线指标构造子文档，若为单值类型则提取单值信息。
   * @param timelineMetric 原始时间线指标对象
   */
  public TimelineMetricSubDoc(TimelineMetric timelineMetric) {
    this.timelineMetric = timelineMetric;
    // 如果是单值类型且存在数值，提取时间戳和数值单独存储
    if (timelineMetric.getType() == TimelineMetric.Type.SINGLE_VALUE &&
        timelineMetric.getValues().size() > 0) {
      this.singleDataTimestamp = timelineMetric.getSingleDataTimestamp();
      this.singleDataValue = timelineMetric.getSingleDataValue();
    }
  }

  /**
   * 获取指标的实时聚合操作类型。
   *
   * @return 实时聚合操作类型
   */
  public TimelineMetricOperation getRealtimeAggregationOp() {
    return timelineMetric.getRealtimeAggregationOp();
  }

  /**
   * 设置指标的实时聚合操作类型。
   *
   * @param op 待设置的实时聚合操作类型
   */
  public void setRealtimeAggregationOp(
      final TimelineMetricOperation op) {
    timelineMetric.setRealtimeAggregationOp(op);
  }

  /**
   * 获取指标ID。
   * @return 指标ID
   */
  public String getId() {
    return timelineMetric.getId();
  }

  /**
   * 设置指标ID。
   * @param metricId 待设置的指标ID
   */
  public void setId(String metricId) {
    timelineMetric.setId(metricId);
  }

  /**
   * 设置单值指标的时间戳。
   * @param singleDataTimestamp 待设置的时间戳
   */
  public void setSingleDataTimestamp(long singleDataTimestamp) {
    this.singleDataTimestamp = singleDataTimestamp;
  }

  /**
   * 获取单值指标的时间戳。
   *
   * @return 单值指标的时间戳，非单值类型返回0
   */
  public long getSingleDataTimestamp() {
    if (timelineMetric.getType() == TimelineMetric.Type.SINGLE_VALUE) {
      return singleDataTimestamp;
    }
    return 0;
  }

  /**
   * 获取单值指标的数值。
   *
   * @return 单值指标的数值，非单值类型返回null
   */
  public Number getSingleDataValue() {
    if (timelineMetric.getType() == TimelineMetric.Type.SINGLE_VALUE) {
      return singleDataValue;
    }
    return null;
  }

  /**
   * 设置单值指标的数值。
   * @param singleDataValue 待设置的数值
   */
  public void setSingleDataValue(Number singleDataValue) {
    this.singleDataValue = singleDataValue;
  }

  /**
   * 获取多时间点指标的数值集合。
   * @return 时间戳到数值的映射
   */
  public Map<Long, Number> getValues() {
    return timelineMetric.getValues();
  }

  /**
   * 设置多时间点指标的数值集合。
   * @param vals 待设置的时间戳到数值的映射
   */
  public void setValues(Map<Long, Number> vals) {
    timelineMetric.setValues(vals);
  }

  // JAXB序列化所需的获取方法，返回TreeMap类型的数值集合
  public TreeMap<Long, Number> getValuesJAXB() {
    return timelineMetric.getValuesJAXB();
  }

  /**
   * 获取指标类型（单值/多值）。
   * @return 指标类型
   */
  public TimelineMetric.Type getType() {
    return timelineMetric.getType();
  }

  /**
   * 设置指标类型。
   * @param metricType 待设置的指标类型
   */
  public void setType(TimelineMetric.Type metricType) {
    timelineMetric.setType(metricType);
  }

  /**
   * 校验当前子文档是否有效（ID不为空即为有效）。
   * @return 有效返回true，否则返回false
   */
  public boolean isValid() {
    return (timelineMetric.getId() != null);
  }

  @Override
  public int hashCode() {
    // 基于指标ID和指标类型计算哈希值
    int result = timelineMetric.getId().hashCode();
    result = 31 * result + timelineMetric.getType().hashCode();
    return result;
  }

  // 仅基于ID和类型判断相等性
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TimelineMetricSubDoc)) {
      return false;
    }
    TimelineMetricSubDoc otherTimelineMetric = (TimelineMetricSubDoc) obj;
    if (!this.timelineMetric.getId().equals(otherTimelineMetric.getId())) {
      return false;
    }
    return this.timelineMetric.getType() == otherTimelineMetric.getType();
  }

  /**
   * 获取内部包装的原始时间线指标对象。
   * @return 原始时间线指标对象
   */
  public TimelineMetric fetchTimelineMetric() {
    return timelineMetric;
  }
}