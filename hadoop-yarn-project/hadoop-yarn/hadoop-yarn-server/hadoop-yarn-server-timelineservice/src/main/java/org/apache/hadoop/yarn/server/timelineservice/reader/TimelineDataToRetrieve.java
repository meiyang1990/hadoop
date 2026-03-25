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

import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;

/**
 * 时间线实体查询时需要获取的数据封装类，封装了需要返回哪些数据的查询条件
 * <br>
 * 需要获取的数据包含以下部分：<br>
 * <ul>
 * <li><b>confsToRetrieve</b> - 决定返回哪些配置项，由包含TimelinePrefixFilter的
 * TimelineFilterList表示，可以是精确匹配配置键或前缀匹配。如果为null或空，只要fieldsToRetrieve包含
 * CONFIG或ALL，就返回所有配置。注意该过滤器不用于筛选实体，只用于决定实体返回哪些配置项</li>
 * <li><b>metricsToRetrieve</b> - 决定返回哪些指标，由包含TimelinePrefixFilter的
 * TimelineFilterList表示，可以是精确匹配指标ID或前缀匹配。如果为null或空，只要fieldsToRetrieve包含
 * METRICS或ALL，就返回所有指标。注意该过滤器不用于筛选实体，只用于决定实体返回哪些指标</li>
 * <li><b>fieldsToRetrieve</b> - 指定需要获取实体对象的哪些字段，参考{@link Field}。如果为null，
 * 只返回实体ID、实体类型、创建时间三个基础字段；如果指定ALL则返回所有字段</li>
 * <li><b>metricsLimit</b> - 如果fieldsToRetrieve包含METRICS/ALL或指定了metricsToRetrieve，
 * 该参数限制返回指标数量的上限。不查询指标时该参数被忽略</li>
 * <li><b>metricsTimeStart</b> - 只返回该时间戳之后的指标值，为null或小于0时默认是0</li>
 * <li><b>metricsTimeEnd</b> - 只返回该时间戳之前的指标值，为null或小于0时默认是{@link Long#MAX_VALUE}
 * </li>
 * </ul>
 */
@Private
@Unstable
public class TimelineDataToRetrieve {
  // 需要返回的配置项过滤器列表
  private TimelineFilterList confsToRetrieve;
  // 需要返回的指标过滤器列表
  private TimelineFilterList metricsToRetrieve;
  // 需要获取的实体字段集合
  private EnumSet<Field> fieldsToRetrieve;
  // 返回指标的数量上限
  private Integer metricsLimit;
  // 指标时间范围起始戳
  private Long metricsTimeBegin;
  // 指标时间范围结束戳
  private Long metricsTimeEnd;
  // 默认指标起始时间
  private static final long DEFAULT_METRICS_BEGIN_TIME = 0L;
  // 默认指标结束时间
  private static final long DEFAULT_METRICS_END_TIME = Long.MAX_VALUE;

  /**
   * 默认返回指标数量上限。
   */
  public static final Integer DEFAULT_METRICS_LIMIT = 1;

  /**
   * 无参构造函数，使用默认值初始化所有参数
   */
  public TimelineDataToRetrieve() {
    this(null, null, null, null, null, null);
  }

  /**
   * 全参数构造函数，初始化所有查询条件并做参数校验
   * @param confs 需要返回的配置项过滤器列表
   * @param metrics 需要返回的指标过滤器列表
   * @param fields 需要获取的实体字段集合
   * @param limitForMetrics 返回指标的数量上限
   * @param metricTimeBegin 指标时间范围起始戳
   * @param metricTimeEnd 指标时间范围结束戳
   */
  public TimelineDataToRetrieve(TimelineFilterList confs,
      TimelineFilterList metrics, EnumSet<Field> fields,
      Integer limitForMetrics, Long metricTimeBegin, Long metricTimeEnd) {
    this.confsToRetrieve = confs;
    this.metricsToRetrieve = metrics;
    this.fieldsToRetrieve = fields;
    // 处理指标数量限制，非法值使用默认值
    if (limitForMetrics == null || limitForMetrics < 1) {
      this.metricsLimit = DEFAULT_METRICS_LIMIT;
    } else {
      this.metricsLimit = limitForMetrics;
    }

    // 字段集合为空时初始化为空枚举集合
    if (this.fieldsToRetrieve == null) {
      this.fieldsToRetrieve = EnumSet.noneOf(Field.class);
    }
    // 处理指标起始时间，非法值使用默认值
    if (metricTimeBegin == null || metricTimeBegin < 0) {
      this.metricsTimeBegin = DEFAULT_METRICS_BEGIN_TIME;
    } else {
      this.metricsTimeBegin = metricTimeBegin;
    }
    // 处理指标结束时间，非法值使用默认值
    if (metricTimeEnd == null || metricTimeEnd < 0) {
      this.metricsTimeEnd = DEFAULT_METRICS_END_TIME;
    } else {
      this.metricsTimeEnd = metricTimeEnd;
    }
    // 校验时间范围合法性，起始时间不能大于结束时间
    if (this.metricsTimeBegin > this.metricsTimeEnd) {
      throw new IllegalArgumentException("metricstimebegin should not be " +
          "greater than metricstimeend");
    }
  }

  public TimelineFilterList getConfsToRetrieve() {
    return confsToRetrieve;
  }

  public void setConfsToRetrieve(TimelineFilterList confs) {
    this.confsToRetrieve = confs;
  }

  public TimelineFilterList getMetricsToRetrieve() {
    return metricsToRetrieve;
  }

  public void setMetricsToRetrieve(TimelineFilterList metrics) {
    this.metricsToRetrieve = metrics;
  }

  public EnumSet<Field> getFieldsToRetrieve() {
    return fieldsToRetrieve;
  }

  public void setFieldsToRetrieve(EnumSet<Field> fields) {
    this.fieldsToRetrieve = fields;
  }

  /**
   * 如果指定了配置或指标过滤条件，自动将对应字段添加到需要返回的字段集合中
   */
  public void addFieldsBasedOnConfsAndMetricsToRetrieve() {
    // 如果已指定配置过滤条件，且未添加CONFIGS字段，自动添加该字段
    if (!fieldsToRetrieve.contains(Field.CONFIGS) && confsToRetrieve != null &&
        !confsToRetrieve.getFilterList().isEmpty()) {
      fieldsToRetrieve.add(Field.CONFIGS);
    }
    // 如果已指定指标过滤条件，且未添加METRICS字段，自动添加该字段
    if (!fieldsToRetrieve.contains(Field.METRICS) &&
        metricsToRetrieve != null &&
        !metricsToRetrieve.getFilterList().isEmpty()) {
      fieldsToRetrieve.add(Field.METRICS);
    }
  }

  public Integer getMetricsLimit() {
    return metricsLimit;
  }

  public Long getMetricsTimeBegin() {
    return this.metricsTimeBegin;
  }

  public Long getMetricsTimeEnd() {
    return metricsTimeEnd;
  }

  public void setMetricsLimit(Integer limit) {
    // 非法值使用默认值
    if (limit == null || limit < 1) {
      this.metricsLimit = DEFAULT_METRICS_LIMIT;
    } else {
      this.metricsLimit = limit;
    }
  }
}