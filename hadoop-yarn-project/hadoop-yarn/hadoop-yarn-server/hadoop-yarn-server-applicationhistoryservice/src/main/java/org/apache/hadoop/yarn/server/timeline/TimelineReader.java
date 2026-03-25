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

package org.apache.hadoop.yarn.server.timeline;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager.CheckAcl;

/**
 * 文件说明：时间线数据读取接口，定义了YARN应用时间线服务读取实体、事件、域信息的统一接口
 * 该接口由时间线存储层实现，为上层查询提供统一的查询入口，支持多维度条件查询时间线数据
 * 
 * This interface is for retrieving timeline information.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface TimelineReader {

  /**
   * 查询实体时可指定返回的字段枚举，用于控制返回数据范围，减少不必要的数据传输
   * Possible fields to retrieve for {@link #getEntities} and {@link #getEntity}
   * .
   */
  enum Field {
    EVENTS,
    RELATED_ENTITIES,
    PRIMARY_FILTERS,
    OTHER_INFO,
    LAST_EVENT_ONLY
  }

  /**
   * 查询结果默认数量限制，避免单次返回过多数据导致性能问题
   * Default limit for {@link #getEntities} and {@link #getEntityTimelines}.
   */
  final long DEFAULT_LIMIT = 100;

  /**
   * 按条件分页查询时间线实体列表，结果按实体起始时间降序排序
   * 支持按时间窗口、主键过滤、副键过滤多条件组合查询，可指定返回字段范围
   * 
   * @param entityType
   *          要查询的实体类型（必填）
   * @param limit
   *          返回实体数量限制，为null时使用默认限制{@link #DEFAULT_LIMIT}
   * @param windowStart
   *          查询起始时间窗口（开区间，仅包含晚于此时间的实体），为null时不限制起始时间
   * @param windowEnd
   *          查询结束时间窗口（闭区间，仅包含早于等于此时间的实体），为null时默认值为{@link Long#MAX_VALUE}
   * @param fromId
   *          分页起始实体ID，不为null时返回该ID及之前的实体，用于分页查询
   * @param fromTs
   *          插入时间过滤戳，不为null时忽略该时间之后插入的实体，基于存储系统插入时间而非实体起始时间
   * @param primaryFilter
   *          主键过滤条件，仅返回匹配该主键的实体，为null不过滤，基于索引查询性能高
   * @param secondaryFilters
   *          副过滤条件，仅返回匹配所有给定主键/其他信息的实体，无索引需要全表扫描
   * @param fieldsToRetrieve
   *          指定需要返回的实体字段，为null返回全部字段，如果仅包含LAST_EVENT_ONLY则每个实体只返回最新事件
   * @param checkAcl
   *          ACL权限检查器，用于验证查询者对实体的访问权限
   * @return 封装查询结果的TimelineEntities对象
   * @throws IOException 读取存储时发生IO异常
   */
  TimelineEntities getEntities(String entityType,
      Long limit, Long windowStart, Long windowEnd, String fromId, Long fromTs,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fieldsToRetrieve, CheckAcl checkAcl) throws IOException;

  /**
   * 根据实体ID和类型查询单个实体的完整信息，可指定返回字段
   * 
   * @param entityId
   *          待查询的实体ID
   * @param entityType
   *          待查询的实体类型
   * @param fieldsToRetrieve
   *          指定需要返回的实体字段，为null返回全部字段
   * @return 封装查询结果的TimelineEntity对象
   * @throws IOException 读取存储时发生IO异常
   */
  TimelineEntity getEntity(String entityId, String entityType, EnumSet<Field>
      fieldsToRetrieve) throws IOException;

  /**
   * 批量查询同类型实体的时间线事件，每个实体的事件按时间戳降序排序
   * 
   * @param entityType
   *          待查询的实体类型
   * @param entityIds
   *          待查询的实体ID集合
   * @param limit
   *          每个实体返回事件数量限制，为null使用默认限制{@link #DEFAULT_LIMIT}
   * @param windowStart
   *          时间窗口起始（开区间，仅返回晚于此时间的事件），为null不限制
   * @param windowEnd
   *          时间窗口结束（闭区间，仅返回早于等于此时间的事件），为null不限制
   * @param eventTypes
   *          限定返回的事件类型，为null返回所有类型
   * @return 封装查询结果的TimelineEvents对象
   * @throws IOException 读取存储时发生IO异常
   */
  TimelineEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd, Set<String> eventTypes) throws IOException;

  /**
   * 根据域ID查询单个时间线域信息
   * 
   * @param domainId
   *          待查询的域ID
   * @return 封装查询结果的TimelineDomain对象
   * @throws IOException 读取存储时发生IO异常
   */
  TimelineDomain getDomain(
      String domainId) throws IOException;

  /**
   * 查询指定用户拥有的所有时间线域，结果先按创建时间降序、再按修改时间降序排序
   * 
   * @param owner
   *          域所有者用户名
   * @return 封装查询结果的TimelineDomains对象
   * @throws IOException 读取存储时发生IO异常
   */
  TimelineDomains getDomains(String owner) throws IOException;
}