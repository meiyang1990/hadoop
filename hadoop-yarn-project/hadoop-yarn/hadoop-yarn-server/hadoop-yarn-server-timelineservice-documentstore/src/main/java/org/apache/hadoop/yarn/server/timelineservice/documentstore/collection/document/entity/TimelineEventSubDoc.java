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

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;

import java.util.Map;

/**
 * 文档存储时间线服务中，用于表示{@link TimelineEntityDocument}中事件信息的子文档。
 * 封装时间线事件，适配文档存储的文档结构模型。
 */
public class TimelineEventSubDoc {

  /** 持有的实际时间线事件对象 */
  private final TimelineEvent timelineEvent;

  /** 无参构造方法，初始化空的时间线事件 */
  public TimelineEventSubDoc() {
    timelineEvent = new TimelineEvent();
  }

  /**
   * 构造方法，基于已有时间线事件创建子文档
   * @param timelineEvent 原始时间线事件对象
   */
  public TimelineEventSubDoc(TimelineEvent timelineEvent) {
    this.timelineEvent = timelineEvent;
  }

  /** 获取事件ID */
  public String getId() {
    return timelineEvent.getId();
  }

  /** 设置事件ID */
  public void setId(String eventId) {
    timelineEvent.setId(eventId);
  }

  /** 检查当前事件是否合法 */
  public boolean isValid() {
    return timelineEvent.isValid();
  }
  
  /** 获取事件时间戳 */
  public long getTimestamp() {
    return timelineEvent.getTimestamp();
  }

  /** 设置事件时间戳 */
  public void setTimestamp(long ts) {
    timelineEvent.setTimestamp(ts);
  }

  /** 获取事件扩展信息键值对 */
  public Map<String, Object> getInfo() {
    return timelineEvent.getInfo();
  }

  /** 设置事件扩展信息，将输入Map转换为HashMap适配存储 */
  public void setInfo(Map<String, Object> info) {
    timelineEvent.setInfo(TimelineServiceHelper.mapCastToHashMap(info));
  }

  @Override
  public int hashCode() {
    return 31 * timelineEvent.getId().hashCode();
  }

  // Only check if id is equal
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TimelineEventSubDoc)) {
      return false;
    }
    TimelineEventSubDoc otherTimelineEvent = (TimelineEventSubDoc) obj;
    return this.timelineEvent.getId().equals(otherTimelineEvent.getId());
  }

  /** 获取封装的原始时间线事件对象 */
  public TimelineEvent fetchTimelineEvent() {
    return timelineEvent;
  }
}