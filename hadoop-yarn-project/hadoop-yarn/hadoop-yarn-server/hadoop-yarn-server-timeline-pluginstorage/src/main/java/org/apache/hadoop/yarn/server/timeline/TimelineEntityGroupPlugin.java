// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;

import java.util.Collection;
import java.util.Set;
import java.util.SortedSet;

// 这个文件已经全部加上中文注释
// 时间线条目组插件抽象类，将查询请求映射到缓存ID。缓存ID是需要查询的数据集标识符
/**
 * 时间线实体分组插件抽象基类，将用户查询请求映射到对应的缓存分组ID，
 * 缓存分组ID标识需要扫描查询的目标数据集，用于服务时间线数据查询请求。
 */
public abstract class TimelineEntityGroupPlugin {

  /**
   * 根据过滤条件获取需要扫描的时间线实体分组ID集合，用于条件查询场景。
   *
   * @param entityType 待查询的实体类型
   * @param primaryFilter 应用的主过滤条件
   * @param secondaryFilters 应用的二级过滤条件集合
   * @return 需要扫描的时间线实体分组ID集合
   */
  public abstract Set<TimelineEntityGroupId> getTimelineEntityGroupId(
      String entityType, NameValuePair primaryFilter,
      Collection<NameValuePair> secondaryFilters);

  /**
   * 根据单个实体获取需要扫描的时间线实体分组ID集合，用于单实体查询场景。
   *
   * @param entityId 待查询的实体ID
   * @param entityType 待查询的实体类型
   * @return 需要扫描的时间线实体分组ID集合
   */
  public abstract Set<TimelineEntityGroupId> getTimelineEntityGroupId(
      String entityId,
      String entityType);


  /**
   * 根据多个实体ID和事件类型获取需要扫描的时间线实体分组ID集合，用于多实体批量查询场景。
   *
   * @param entityType 待查询的实体类型
   * @param entityIds 待查询的实体ID有序集合
   * @param eventTypes 待查询的事件类型集合
   * @return 需要扫描的时间线实体分组ID集合
   */
  public abstract Set<TimelineEntityGroupId> getTimelineEntityGroupId(
      String entityType, SortedSet<String> entityIds,
      Set<String> eventTypes);


}