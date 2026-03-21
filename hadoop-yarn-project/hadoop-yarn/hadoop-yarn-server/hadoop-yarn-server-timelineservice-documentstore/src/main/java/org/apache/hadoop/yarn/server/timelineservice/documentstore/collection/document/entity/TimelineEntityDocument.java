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

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.TimelineContext;
import org.apache.hadoop.yarn.server.timelineservice.documentstore.collection.document.TimelineDocument;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 文档存储时间线服务中，表示时间线实体的文档模型，包含一个概念实体及其关联事件的全部元信息。
 * 时间线实体可以是应用、尝试、容器或用户自定义对象。
 */
public class TimelineEntityDocument implements
    TimelineDocument<TimelineEntityDocument> {

  // 原始时间线实体对象
  private final TimelineEntity timelineEntity;
  // 时间线上下文，包含流、用户等层级标识信息
  private TimelineContext context;
  // 工作流版本
  private String flowVersion;
  // 子应用用户名
  private String subApplicationUser;
  // 按指标ID分组存储的指标子文档集合
  private final Map<String, Set<TimelineMetricSubDoc>>
      metrics = new HashMap<>();
  // 按事件ID分组存储的事件子文档集合
  private final Map<String, Set<TimelineEventSubDoc>>
      events = new HashMap<>();

  /**
   * 构造空时间线实体文档。
   */
  public TimelineEntityDocument() {
    timelineEntity = new TimelineEntity();
  }

  /**
   * 基于原始时间线实体构造文档对象，自动转换指标和事件为文档子结构。
   * @param timelineEntity 原始时间线实体
   */
  public TimelineEntityDocument(TimelineEntity timelineEntity) {
    this.timelineEntity = timelineEntity;
    transformEvents(timelineEntity.getEvents());
    timelineMetrics(timelineEntity.getMetrics());
  }

  /**
   * 将原始指标集合转换为文档存储用的指标子文档结构，按指标ID分组存储。
   * @param timelineMetrics 原始指标集合
   */
  private void timelineMetrics(Set<TimelineMetric> timelineMetrics) {
    for (TimelineMetric timelineMetric : timelineMetrics) {
      if (this.metrics.containsKey(timelineMetric.getId())) {
        this.metrics.get(timelineMetric.getId()).add(
            new TimelineMetricSubDoc(timelineMetric));
      } else {
        Set<TimelineMetricSubDoc> metricSet = new HashSet<>();
        metricSet.add(new TimelineMetricSubDoc(timelineMetric));
        this.metrics.put(timelineMetric.getId(), metricSet);
      }
    }
  }

  /**
   * 将原始事件集合转换为文档存储用的事件子文档结构，按事件ID分组存储。
   * @param timelineEvents 原始事件集合
   */
  private void transformEvents(Set<TimelineEvent> timelineEvents) {
    for (TimelineEvent timelineEvent : timelineEvents) {
      if (this.events.containsKey(timelineEvent.getId())) {
        this.events.get(timelineEvent.getId())
            .add(new TimelineEventSubDoc(timelineEvent));
      } else {
        Set<TimelineEventSubDoc> eventSet = new HashSet<>();
        eventSet.add(new TimelineEventSubDoc(timelineEvent));
        this.events.put(timelineEvent.getId(), eventSet);
      }
    }
  }

  /**
   * 将传入的文档合并到当前文档，用于upsert操作，合并所有字段信息。
   *
   * @param newTimelineDocument 需要合并的新文档
   */
  @Override
  public void merge(TimelineEntityDocument newTimelineDocument) {
    if(newTimelineDocument.getCreatedTime() > 0) {
      timelineEntity.setCreatedTime(newTimelineDocument.getCreatedTime());
    }
    setMetrics(newTimelineDocument.getMetrics());
    setEvents(newTimelineDocument.getEvents());
    timelineEntity.getInfo().putAll(newTimelineDocument.getInfo());
    timelineEntity.getConfigs().putAll(newTimelineDocument.getConfigs());
    timelineEntity.getIsRelatedToEntities().putAll(newTimelineDocument
        .getIsRelatedToEntities());
    timelineEntity.getRelatesToEntities().putAll(newTimelineDocument
        .getRelatesToEntities());
  }

  @Override
  public String getId() {
    return timelineEntity.getId();
  }

  public void setId(String key) {
    timelineEntity.setId(key);
  }

  public String getType() {
    return timelineEntity.getType();
  }

  public void setType(String type) {
    timelineEntity.setType(type);
  }

  public Map<String, Object> getInfo() {
    // 添加实体ID到信息中供查询使用
    timelineEntity.getInfo().put(TimelineReaderUtils.FROMID_KEY, getId());
    return timelineEntity.getInfo();
  }

  public void setInfo(Map<String, Object> info) {
    timelineEntity.setInfo(info);
  }

  public Map<String, Set<TimelineMetricSubDoc>> getMetrics() {
    return metrics;
  }

  /**
   * 合并传入的指标集合到当前文档，同时更新原始时间线实体。
   * @param metrics 需要合并的指标集合
   */
  public void setMetrics(Map<String, Set<TimelineMetricSubDoc>> metrics) {
    for (Map.Entry<String, Set<TimelineMetricSubDoc>> metricEntry :
        metrics.entrySet()) {
      final String metricId = metricEntry.getKey();
      final Set<TimelineMetricSubDoc> metricValue = metricEntry.getValue();

      for(TimelineMetricSubDoc metricSubDoc : metricValue) {
        timelineEntity.addMetric(metricSubDoc.fetchTimelineMetric());
      }
      if (this.metrics.containsKey(metricId)) {
        this.metrics.get(metricId).addAll(metricValue);
      } else {
        this.metrics.put(metricId, new HashSet<>(metricValue));
      }
    }
  }

  public Map<String, Set<TimelineEventSubDoc>> getEvents() {
    return events;
  }

  /**
   * 合并传入的事件集合到当前文档，同时更新原始时间线实体。
   * @param events 需要合并的事件集合
   */
  public void setEvents(Map<String, Set<TimelineEventSubDoc>> events) {
    for (Map.Entry<String, Set<TimelineEventSubDoc>> eventEntry :
        events.entrySet()) {
      final String eventId = eventEntry.getKey();
      final Set<TimelineEventSubDoc> eventValue = eventEntry.getValue();

      for(TimelineEventSubDoc eventSubDoc : eventValue) {
        timelineEntity.addEvent(eventSubDoc.fetchTimelineEvent());
      }
      if (this.events.containsKey(eventId)) {
        this.events.get(eventId).addAll(events.get(eventId));
      } else {
        this.events.put(eventId, new HashSet<>(eventValue));
      }
    }
  }

  public Map<String, String> getConfigs() {
    return timelineEntity.getConfigs();
  }

  public void setConfigs(Map<String, String> configs) {
    timelineEntity.setConfigs(configs);
  }

  public Map<String, Set<String>> getIsRelatedToEntities() {
    return timelineEntity.getIsRelatedToEntities();
  }

  public void setIsRelatedToEntities(Map<String, Set<String>>
      isRelatedToEntities) {
    timelineEntity.setIsRelatedToEntities(isRelatedToEntities);
  }

  public Map<String, Set<String>> getRelatesToEntities() {
    return timelineEntity.getRelatesToEntities();
  }

  public void setRelatesToEntities(Map<String, Set<String>> relatesToEntities) {
    timelineEntity.setRelatesToEntities(relatesToEntities);
  }

  public String getFlowVersion() {
    return flowVersion;
  }


  public void setFlowVersion(String flowVersion) {
    this.flowVersion = flowVersion;
  }

  public void setIdentifier(TimelineEntity.Identifier identifier) {
    timelineEntity.setIdentifier(identifier);
  }

  public void setIdPrefix(long idPrefix) {
    timelineEntity.setIdPrefix(idPrefix);
  }

  public String getSubApplicationUser() {
    return subApplicationUser;
  }

  public void setSubApplicationUser(String subApplicationUser) {
    this.subApplicationUser = subApplicationUser;
  }

  public long getCreatedTime() {
    if (timelineEntity.getCreatedTime() == null) {
      return 0;
    }
    return timelineEntity.getCreatedTime();
  }

  public void setCreatedTime(long createdTime) {
    timelineEntity.setCreatedTime(createdTime);
  }

  public TimelineContext getContext() {
    return context;
  }

  public void setContext(TimelineContext context) {
    this.context = context;
  }

  /**
   * 获取封装的原始时间线实体对象。
   * @return 原始时间线实体
   */
  public TimelineEntity fetchTimelineEntity() {
    return timelineEntity;
  }
}