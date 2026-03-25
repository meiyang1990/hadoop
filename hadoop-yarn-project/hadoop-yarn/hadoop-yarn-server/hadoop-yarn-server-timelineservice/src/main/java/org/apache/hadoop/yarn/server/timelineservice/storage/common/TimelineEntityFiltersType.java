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

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter.TimelineFilterType;

/**
 * 时间线实体过滤器类型枚举，定义不同实体属性允许使用的过滤类型，用于时间线数据存储查询时过滤合法性校验。
 */
enum TimelineEntityFiltersType {
  /** 配置信息过滤 */
  CONFIG {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.KEY_VALUE;
    }
  },
  /** 实体信息过滤 */
  INFO {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.KEY_VALUE;
    }
  },
  /** 指标数据过滤 */
  METRIC {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.COMPARE;
    }
  },
  /** 事件过滤 */
  EVENT {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.EXISTS;
    }
  },
  /** 被关联关系过滤 */
  IS_RELATED_TO {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.KEY_VALUES;
    }
  },
  /** 关联关系过滤 */
  RELATES_TO {
    boolean isValidFilter(TimelineFilterType filterType) {
      return filterType == TimelineFilterType.LIST ||
          filterType == TimelineFilterType.KEY_VALUES;
    }
  };

  /**
   * 检查给定的过滤类型对当前实体属性是否合法。
   *
   * @param filterType 待检查的过滤类型
   * @return 合法返回true，否则返回false
   */
  abstract boolean isValidFilter(TimelineFilterType filterType);
}