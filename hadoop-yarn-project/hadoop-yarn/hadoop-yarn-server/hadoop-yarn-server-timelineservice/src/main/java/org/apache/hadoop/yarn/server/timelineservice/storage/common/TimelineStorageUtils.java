// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter.TimelineFilterType;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;

/**
 * 时间线服务存储模块通用工具类，提供时间线实体各类过滤器匹配工具函数，被时间线读取器和写入器共享使用。
 */
@Public
@Unstable
public final class TimelineStorageUtils {
  private TimelineStorageUtils() {
  }

  /**
   * 多值键过滤器匹配，用于relatesTo/isRelatedTo实体关系过滤。
   *
   * @param entity 待匹配的时间线实体，持有关系数据
   * @param keyValuesFilter 多值键过滤器
   * @param entityFiltersType 过滤器类型（关系类型）
   * @return true匹配成功，false匹配失败
   */
  private static boolean matchKeyValuesFilter(TimelineEntity entity,
      TimelineKeyValuesFilter keyValuesFilter,
      TimelineEntityFiltersType entityFiltersType) {
    Map<String, Set<String>> relations = null;
    // 根据关系类型获取对应关系集合
    if (entityFiltersType == TimelineEntityFiltersType.IS_RELATED_TO) {
      relations = entity.getIsRelatedToEntities();
    } else if (entityFiltersType == TimelineEntityFiltersType.RELATES_TO) {
      relations = entity.getRelatesToEntities();
    }
    // 实体没有该类型关系，匹配失败
    if (relations == null) {
      return false;
    }
    // 获取对应键关联的实体ID集合
    Set<String> ids = relations.get(keyValuesFilter.getKey());
    // 该键没有关联实体，匹配失败
    if (ids == null) {
      return false;
    }
    boolean matched = false;
    // 遍历所有过滤值依次匹配
    for (Object id : keyValuesFilter.getValues()) {
      // 根据比较运算符判断匹配结果：EQUAL要求id存在，NOT_EQUAL要求id不存在
      matched = !(ids.contains(id) ^
          keyValuesFilter.getCompareOp() == TimelineCompareOp.EQUAL);
      // 任意一个值不匹配，直接返回失败
      if (!matched) {
        return false;
      }
    }
    // 所有值都匹配成功
    return true;
  }

  /**
   * 匹配实体的relatesTo关系过滤条件。
   *
   * @param entity 待匹配的时间线实体
   * @param relatesTo 关系过滤条件列表
   * @return true匹配成功，false匹配失败
   * @throws IOException 遇到不支持的过滤器类型时抛出
   */
  public static boolean matchRelatesTo(TimelineEntity entity,
      TimelineFilterList relatesTo) throws IOException {
    return matchFilters(
        entity, relatesTo, TimelineEntityFiltersType.RELATES_TO);
  }

  /**
   * 匹配实体的isRelatedTo关系过滤条件。
   *
   * @param entity 待匹配的时间线实体
   * @param isRelatedTo 关系过滤条件列表
   * @return true匹配成功，false匹配失败
   * @throws IOException 遇到不支持的过滤器类型时抛出
   */
  public static boolean matchIsRelatedTo(TimelineEntity entity,
      TimelineFilterList isRelatedTo) throws IOException {
    return matchFilters(
        entity, isRelatedTo, TimelineEntityFiltersType.IS_RELATED_TO);
  }

  /**
   * 单键值对过滤器匹配，用于配置和信息字段过滤。
   *
   * @param entity 待匹配的时间线实体，持有配置/信息数据
   * @param kvFilter 单键值过滤器
   * @param entityFiltersType 过滤器类型（配置/信息）
   * @return true匹配成功，false匹配失败
   */
  private static boolean matchKeyValueFilter(TimelineEntity entity,
      TimelineKeyValueFilter kvFilter,
      TimelineEntityFiltersType entityFiltersType) {
    Map<String, ? extends Object> map = null;
    // 根据过滤器类型获取对应键值对集合
    if (entityFiltersType == TimelineEntityFiltersType.CONFIG) {
      map = entity.getConfigs();
    } else if (entityFiltersType == TimelineEntityFiltersType.INFO) {
      map = entity.getInfo();
    }
    // 实体没有该类型数据，匹配失败
    if (map == null) {
      return false;
    }
    // 获取对应键的值
    Object value = map.get(kvFilter.getKey());
    // 键不存在，匹配失败
    if (value == null) {
      return false;
    }
    // 根据比较运算符判断匹配结果：EQUAL要求值相等，NOT_EQUAL要求值不等
    return !(value.equals(kvFilter.getValue()) ^
        kvFilter.getCompareOp() == TimelineCompareOp.EQUAL);
  }

  /**
   * 匹配实体的配置字段过滤条件。
   *
   * @param entity 待匹配的时间线实体
   * @param configFilters 配置过滤条件列表
   * @return true匹配成功，false匹配失败
   * @throws IOException 遇到不支持的过滤器类型时抛出
   */
  public static boolean matchConfigFilters(TimelineEntity entity,
      TimelineFilterList configFilters) throws IOException {
    return
        matchFilters(entity, configFilters, TimelineEntityFiltersType.CONFIG);
  }

  /**
   * 匹配实体的信息字段过滤条件。
   *
   * @param entity 待匹配的时间线实体
   * @param infoFilters 信息过滤条件列表
   * @return true匹配成功，false匹配失败
   * @throws IOException 遇到不支持的过滤器类型时抛出
   */
  public static boolean matchInfoFilters(TimelineEntity entity,
      TimelineFilterList infoFilters) throws IOException {
    return matchFilters(entity, infoFilters, TimelineEntityFiltersType.INFO);
  }

  /**
   * 存在性过滤器匹配，用于事件过滤。
   *
   * @param entity 待匹配的时间线实体，持有事件数据
   * @param existsFilter 存在性过滤器
   * @param entityFiltersType 过滤器类型
   * @return true匹配成功，false匹配失败
   */
  private static boolean matchExistsFilter(TimelineEntity entity,
      TimelineExistsFilter existsFilter,
      TimelineEntityFiltersType entityFiltersType) {
    // 存在性过滤器仅支持事件类型过滤
    if (entityFiltersType != TimelineEntityFiltersType.EVENT) {
      return false;
    }
    // 收集实体所有事件ID
    Set<String> eventIds = new HashSet<String>();
    for (TimelineEvent event : entity.getEvents()) {
      eventIds.add(event.getId());
    }
    // 根据比较运算符判断匹配结果：EQUAL要求事件存在，NOT_EQUAL要求事件不存在
    return !(eventIds.contains(existsFilter.getValue()) ^
        existsFilter.getCompareOp() == TimelineCompareOp.EQUAL);
  }

  /**
   * 匹配实体的事件过滤条件。
   *
   * @param entity 待匹配的时间线实体
   * @param eventFilters 事件过滤条件列表
   * @return true匹配成功，false匹配失败
   * @throws IOException 遇到不支持的过滤器类型时抛出
   */
  public static boolean matchEventFilters(TimelineEntity entity,
      TimelineFilterList eventFilters) throws IOException {
    return matchFilters(entity, eventFilters, TimelineEntityFiltersType.EVENT);
  }

  /**
   * 根据比较运算符比较两个长整型值。
   *
   * @param compareOp 比较运算符
   * @param val1 待比较值1
   * @param val2 待比较值2
   * @return true比较关系成立，false不成立
   */
  private static boolean compareValues(TimelineCompareOp compareOp,
      long val1, long val2) {
    switch (compareOp) {
    case LESS_THAN:
      return val1 < val2;
    case LESS_OR_EQUAL:
      return val1 <= val2;
    case EQUAL:
      return val1 == val2;
    case NOT_EQUAL:
      return val1 != val2;
    case GREATER_OR_EQUAL:
      return val1 >= val2;
    case GREATER_THAN:
      return val1 > val2;
    default:
      throw new RuntimeException("Unknown TimelineCompareOp " +
          compareOp.name());
    }
  }

  /**
   * 比较过滤器匹配，用于指标过滤。
   *
   * @param entity 待匹配的时间线实体，持有指标数据
   * @param compareFilter 比较过滤器
   * @param entityFiltersType 过滤器类型
   * @return true匹配成功，false匹配失败
   * @throws IOException 过滤器包含非整型值时抛出
   */
  private static boolean matchCompareFilter(TimelineEntity entity,
      TimelineCompareFilter compareFilter,
      TimelineEntityFiltersType entityFiltersType) throws IOException {
    // 比较过滤器仅支持指标类型过滤
    if (entityFiltersType != TimelineEntityFiltersType.METRIC) {
      return false;
    }
    // 指标过滤器要求过滤值必须是整型
    if (!isIntegralValue(compareFilter.getValue())) {
      throw new IOException("Metric filters has non integral values");
    }
    // 构建指标ID到指标对象的映射
    Map<String, TimelineMetric> metricMap =
        new HashMap<String, TimelineMetric>();
    for (TimelineMetric metric : entity.getMetrics()) {
      metricMap.put(metric.getId(), metric);
    }
    // 根据键获取对应指标
    TimelineMetric metric = metricMap.get(compareFilter.getKey());
    // 指标不存在，匹配失败
    if (metric == null) {
      return false;
    }
    // 使用指标最新值和过滤值比较，返回匹配结果
    return compareValues(compareFilter.getCompareOp(),
        metric.getValuesJAXB().firstEntry().getValue().longValue(),
        ((Number)compareFilter.getValue()).longValue());
  }

  /**
   * 匹配实体的指标过滤条件。
   *
   * @param entity 待匹配的时间线实体
   * @param metricFilters 指标过滤条件列表
   * @return true匹配成功，false匹配失败
   * @throws IOException 遇到不支持的过滤器类型时抛出
   */
  public static boolean matchMetricFilters(TimelineEntity entity,
      TimelineFilterList metricFilters) throws IOException {
    return matchFilters(
        entity, metricFilters, TimelineEntityFiltersType.METRIC);
  }

  /**
   * 过滤器匹配通用核心逻辑，遍历过滤器列表根据过滤器类型分发匹配。
   *
   * @param entity 待匹配的时间线实体
   * @param filters 过滤器列表
   * @param entityFiltersType 当前匹配的过滤器大类
   * @return true匹配成功，false匹配失败
   * @throws IOException 遇到不支持的过滤器类型时抛出
   */
  private static boolean matchFilters(TimelineEntity entity,
      TimelineFilterList filters, TimelineEntityFiltersType entityFiltersType)
      throws IOException {
    // 过滤器列表为空，匹配失败
    if (filters == null || filters.getFilterList().isEmpty()) {
      return false;
    }
    // 获取当前过滤器列表的组合运算符（AND/OR）
    TimelineFilterList.Operator operator = filters.getOperator();
    // 遍历所有过滤器依次匹配
    for (TimelineFilter filter : filters.getFilterList()) {
      TimelineFilterType filterType = filter.getFilterType();
      // 检查当前过滤器类型是否被当前大类支持
      if (!entityFiltersType.isValidFilter(filterType)) {
        throw new IOException("Unsupported filter " + filterType);
      }
      boolean matched = false;
      // 根据过滤器类型调用对应匹配逻辑
      switch (filterType) {
      case LIST:
        // 嵌套过滤器列表，递归匹配
        matched = matchFilters(entity, (TimelineFilterList)filter,
            entityFiltersType);
        break;
      case COMPARE:
        // 比较过滤器匹配（指标）
        matched = matchCompareFilter(entity, (TimelineCompareFilter)filter,
            entityFiltersType);
        break;
      case EXISTS:
        // 存在性过滤器匹配（事件）
        matched = matchExistsFilter(entity, (TimelineExistsFilter)filter,
            entityFiltersType);
        break;
      case KEY_VALUE:
        // 单键值过滤器匹配（配置/信息）
        matched = matchKeyValueFilter(entity, (TimelineKeyValueFilter)filter,
            entityFiltersType);
        break;
      case KEY_VALUES:
        // 多值键过滤器匹配（关系）
        matched = matchKeyValuesFilter(entity, (TimelineKeyValuesFilter)filter,
            entityFiltersType);
        break;
      default:
        throw new IOException("Unsupported filter " + filterType);
      }
      // 当前过滤器匹配失败处理
      if (!matched) {
        // AND运算符：任意失败直接返回失败
        if(operator == TimelineFilterList.Operator.AND) {
          return false;
        }
      } else {
        // 当前过滤器匹配成功处理
        // OR运算符：任意成功直接返回成功
        if(operator == TimelineFilterList.Operator.OR) {
          return true;
        }
      }
    }
    // 遍历完所有过滤器：AND运算符说明全部匹配成功返回true；OR运算符说明全部失败返回false
    return operator == TimelineFilterList.Operator.AND;
  }

  /**
   * 检查对象是否为整型类型（Short/Integer/Long）。
   *
   * @param obj 待检查对象
   * @return true是整型类型，false不是
   */
  public static boolean isIntegralValue(Object obj) {
    return (obj instanceof Short) || (obj instanceof Integer) ||
        (obj instanceof Long);
  }
}