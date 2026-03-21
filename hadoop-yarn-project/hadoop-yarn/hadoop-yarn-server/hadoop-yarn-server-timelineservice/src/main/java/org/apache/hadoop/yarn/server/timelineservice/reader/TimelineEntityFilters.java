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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;

/**
 * 时间线实体查询过滤器封装类，用于在查询时间线实体时根据条件过滤结果，限制返回实体数量。
 * 封装了多种查询过滤条件，包括结果数量限制、创建时间范围、关系过滤、键值属性过滤、指标过滤、事件过滤等。
 * <br>
 * 过滤条件包含以下类型：<br>
 * <ul>
 * <li><b>limit</b> - 返回实体数量上限。如果为null或小于0，默认值为{@link #DEFAULT_LIMIT}。最大值可为{@link Long#MAX_VALUE}。</li>
 * <li><b>createdTimeBegin</b> - 匹配实体的创建时间不能早于此时间戳。如果为null或小于等于0，默认值为0。</li>
 * <li><b>createdTimeEnd</b> - 匹配实体的创建时间不能晚于此时间戳。如果为null或小于等于0，默认值为{@link Long#MAX_VALUE}。</li>
 * <li><b>relatesTo</b> - 匹配实体需要满足与指定实体的关联关系条件。每个过滤项包含实体类型作为键，实体ID集合作为值，搭配等于/不等于比较符，通过{@link TimelineFilterList}组织支持逻辑与/或组合。如果为null或空，则不进行关系过滤。</li>
 * <li><b>isRelatedTo</b> - 匹配实体需要满足被指定实体关联的关系条件。结构与relatesTo类似，如果为null或空，则不进行关系过滤。</li>
 * <li><b>infoFilters</b> - 匹配实体需要满足信息属性的键值匹配条件，每个过滤项包含键、值和等于/不等于比较符，通过{@link TimelineFilterList}组织支持逻辑与/或组合。如果为null或空，则不应用该过滤。</li>
 * <li><b>configFilters</b> - 匹配实体需要满足配置属性的键值匹配条件，每个过滤项包含配置名键、配置值值和等于/不等于比较符，通过{@link TimelineFilterList}组织支持逻辑与/或组合。如果为null或空，则不应用该过滤。</li>
 * <li><b>metricFilters</b> - 匹配实体需要满足指标数值比较条件，每个过滤项包含指标键、数值和比较运算符（大于、小于等于等），通过{@link TimelineFilterList}组织支持逻辑与/或组合。如果为null或空，则不应用该过滤。</li>
 * <li><b>eventFilters</b> - 匹配实体需要满足事件存在性条件，每个过滤项包含事件ID和存在/不存在比较符，通过{@link TimelineFilterList}组织支持逻辑与/或组合。如果为null或空，则不应用该过滤。</li>
 * <li><b>fromId</b> - 分页查询起始实体ID，返回结果包含该实体，该值来自上一次查询响应中的FROM_ID。如果未指定则从头开始查询。</li>
 * </ul>
 */
@Private
@Unstable
public final class TimelineEntityFilters {
  private final long limit;
  private long createdTimeBegin;
  private long createdTimeEnd;
  private final TimelineFilterList relatesTo;
  private final TimelineFilterList isRelatedTo;
  private final TimelineFilterList infoFilters;
  private final TimelineFilterList configFilters;
  private final TimelineFilterList metricFilters;
  private final TimelineFilterList eventFilters;
  private final String fromId;
  private static final long DEFAULT_BEGIN_TIME = 0L;
  private static final long DEFAULT_END_TIME = Long.MAX_VALUE;


  /**
   * getEntities API默认返回实体数量上限。
   */
  public static final long DEFAULT_LIMIT = 100;

  /**
   * 构造TimelineEntityFilters对象，使用传入参数设置过滤条件，参数非法时使用默认值。
   */
  private TimelineEntityFilters(
      Long entityLimit, Long timeBegin, Long timeEnd,
      TimelineFilterList entityRelatesTo,
      TimelineFilterList entityIsRelatedTo,
      TimelineFilterList entityInfoFilters,
      TimelineFilterList entityConfigFilters,
      TimelineFilterList  entityMetricFilters,
      TimelineFilterList entityEventFilters, String fromId) {
    // 实体数量限界检查，非法值使用默认值
    if (entityLimit == null || entityLimit < 0) {
      this.limit = DEFAULT_LIMIT;
    } else {
      this.limit = entityLimit;
    }
    // 创建起始时间检查，非法值使用默认值
    if (timeBegin == null || timeBegin < 0) {
      this.createdTimeBegin = DEFAULT_BEGIN_TIME;
    } else {
      this.createdTimeBegin = timeBegin;
    }
    // 创建结束时间检查，非法值使用默认值
    if (timeEnd == null || timeEnd < 0) {
      this.createdTimeEnd = DEFAULT_END_TIME;
    } else {
      this.createdTimeEnd = timeEnd;
    }
    this.relatesTo = entityRelatesTo;
    this.isRelatedTo = entityIsRelatedTo;
    this.infoFilters = entityInfoFilters;
    this.configFilters = entityConfigFilters;
    this.metricFilters = entityMetricFilters;
    this.eventFilters = entityEventFilters;
    this.fromId = fromId;
  }

  public long getLimit() {
    return limit;
  }

  public long getCreatedTimeBegin() {
    return createdTimeBegin;
  }

  public long getCreatedTimeEnd() {
    return createdTimeEnd;
  }

  public TimelineFilterList getRelatesTo() {
    return relatesTo;
  }

  public TimelineFilterList getIsRelatedTo() {
    return isRelatedTo;
  }

  public TimelineFilterList getInfoFilters() {
    return infoFilters;
  }

  public TimelineFilterList getConfigFilters() {
    return configFilters;
  }

  public TimelineFilterList getMetricFilters() {
    return metricFilters;
  }

  public TimelineFilterList getEventFilters() {
    return eventFilters;
  }

  public String getFromId() {
    return fromId;
  }

  /**
   * TimelineEntityFilters的Builder构造类，用于逐步构建过滤器对象。
   */
  public static class Builder {
    private Long entityLimit;
    private Long createdTimeBegin;
    private Long createdTimeEnd;
    private TimelineFilterList relatesToFilters;
    private TimelineFilterList isRelatedToFilters;
    private TimelineFilterList entityInfoFilters;
    private TimelineFilterList entityConfigFilters;
    private TimelineFilterList entityMetricFilters;
    private TimelineFilterList entityEventFilters;
    private String entityFromId;

    /**
     * 设置返回实体数量上限。
     */
    public Builder entityLimit(Long limit) {
      this.entityLimit = limit;
      return this;
    }

    /**
     * 设置创建时间起始边界。
     */
    public Builder createdTimeBegin(Long timeBegin) {
      this.createdTimeBegin = timeBegin;
      return this;
    }

    /**
     * 设置创建时间结束边界。
     */
    public Builder createTimeEnd(Long timeEnd) {
      this.createdTimeEnd = timeEnd;
      return this;
    }

    /**
     * 设置relatesTo关联过滤条件列表。
     */
    public Builder relatesTo(TimelineFilterList relatesTo) {
      this.relatesToFilters = relatesTo;
      return this;
    }

    /**
     * 设置isRelatedTo被关联过滤条件列表。
     */
    public Builder isRelatedTo(TimelineFilterList isRelatedTo) {
      this.isRelatedToFilters = isRelatedTo;
      return this;
    }

    /**
     * 设置信息属性过滤条件列表。
     */
    public Builder infoFilters(TimelineFilterList infoFilters) {
      this.entityInfoFilters = infoFilters;
      return this;
    }

    /**
     * 设置配置属性过滤条件列表。
     */
    public Builder configFilters(TimelineFilterList configFilters) {
      this.entityConfigFilters = configFilters;
      return this;
    }

    /**
     * 设置指标数值过滤条件列表。
     */
    public Builder metricFilters(TimelineFilterList metricFilters) {
      this.entityMetricFilters = metricFilters;
      return this;
    }

    /**
     * 设置事件存在性过滤条件列表。
     */
    public Builder eventFilters(TimelineFilterList eventFilters) {
      this.entityEventFilters = eventFilters;
      return this;
    }

    /**
     * 设置分页查询起始实体ID。
     */
    public Builder fromId(String fromId) {
      this.entityFromId = fromId;
      return this;
    }

    /**
     * 构造TimelineEntityFilters对象。
     * @return 构建完成的过滤对象
     */
    public TimelineEntityFilters build() {
      return new TimelineEntityFilters(entityLimit, createdTimeBegin,
          createdTimeEnd, relatesToFilters, isRelatedToFilters,
          entityInfoFilters, entityConfigFilters, entityMetricFilters,
          entityEventFilters, entityFromId);
    }
  }
}