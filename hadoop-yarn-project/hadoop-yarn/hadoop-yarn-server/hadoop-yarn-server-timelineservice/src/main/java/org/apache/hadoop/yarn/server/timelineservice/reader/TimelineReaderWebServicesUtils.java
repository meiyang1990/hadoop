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

import java.security.Principal;
import java.util.EnumSet;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;

/**
 * 时间线Reader Web服务工具类，提供查询参数解析、用户信息获取等通用能力。
 */
public final class TimelineReaderWebServicesUtils {

  private TimelineReaderWebServicesUtils() {
  }

  /**
   * 从路径参数解析构建时间线Reader上下文对象。
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 流名称
   * @param flowRunId 流运行ID
   * @param appId 应用ID
   * @param entityType 实体类型
   * @param entityIdPrefix 实体ID前缀
   * @param entityId 实体ID
   * @return 构建完成的时间线Reader上下文
   */
  static TimelineReaderContext createTimelineReaderContext(String clusterId,
      String userId, String flowName, String flowRunId, String appId,
      String entityType, String entityIdPrefix, String entityId) {
    return new TimelineReaderContext(parseStr(clusterId), parseStr(userId),
        parseStr(flowName), parseLongStr(flowRunId), parseStr(appId),
        parseStr(entityType), parseLongStr(entityIdPrefix), parseStr(entityId));
  }

  /**
   * 从路径参数解析构建带代理用户的时间线Reader上下文对象。
   * @param clusterId 集群ID
   * @param userId 用户ID
   * @param flowName 流名称
   * @param flowRunId 流运行ID
   * @param appId 应用ID
   * @param entityType 实体类型
   * @param entityIdPrefix 实体ID前缀
   * @param entityId 实体ID
   * @param doAsUser 代理用户
   * @return 构建完成的时间线Reader上下文
   */
  static TimelineReaderContext createTimelineReaderContext(String clusterId,
      String userId, String flowName, String flowRunId, String appId,
      String entityType, String entityIdPrefix, String entityId,
      String doAsUser) {
    return new TimelineReaderContext(parseStr(clusterId), parseStr(userId),
        parseStr(flowName), parseLongStr(flowRunId), parseStr(appId),
        parseStr(entityType), parseLongStr(entityIdPrefix), parseStr(entityId),
        parseStr(doAsUser));
  }

  /**
   * 解析查询字符串参数构建时间线实体过滤条件对象。
   * @param limit 返回实体数量上限
   * @param createdTimeStart 实体创建时间起始
   * @param createdTimeEnd 实体创建时间结束
   * @param relatesTo 关联实体过滤条件
   * @param isRelatedTo 被关联实体过滤条件
   * @param infofilters 信息字段过滤条件
   * @param conffilters 配置字段过滤条件
   * @param metricfilters 指标过滤条件
   * @param eventfilters 事件过滤条件
   * @param fromid 分页起始实体ID
   * @return 构建完成的实体过滤条件对象
   * @throws TimelineParseException 解析失败时抛出异常
   */
  static TimelineEntityFilters createTimelineEntityFilters(String limit,
      String createdTimeStart, String createdTimeEnd, String relatesTo,
      String isRelatedTo, String infofilters, String conffilters,
      String metricfilters, String eventfilters,
      String fromid) throws TimelineParseException {
    return createTimelineEntityFilters(
        limit, parseLongStr(createdTimeStart),
        parseLongStr(createdTimeEnd),
        relatesTo, isRelatedTo, infofilters,
        conffilters, metricfilters, eventfilters, fromid);
  }

  /**
   * 基于已解析的时间参数构建时间线实体过滤条件对象。
   * @param limit 返回实体数量上限
   * @param createdTimeStart 实体创建时间起始
   * @param createdTimeEnd 实体创建时间结束
   * @param relatesTo 关联实体过滤条件
   * @param isRelatedTo 被关联实体过滤条件
   * @param infofilters 信息字段过滤条件
   * @param conffilters 配置字段过滤条件
   * @param metricfilters 指标过滤条件
   * @param eventfilters 事件过滤条件
   * @param fromid 分页起始实体ID
   * @return 构建完成的实体过滤条件对象
   * @throws TimelineParseException 解析失败时抛出异常
   */
  static TimelineEntityFilters createTimelineEntityFilters(String limit,
      Long createdTimeStart, Long createdTimeEnd, String relatesTo,
      String isRelatedTo, String infofilters, String conffilters,
      String metricfilters, String eventfilters,
      String fromid) throws TimelineParseException {
    return new TimelineEntityFilters.Builder()
        // 设置返回实体数量上限
        .entityLimit(parseLongStr(limit))
        // 设置创建时间起始
        .createdTimeBegin(createdTimeStart)
        // 设置创建时间结束
        .createTimeEnd(createdTimeEnd)
        // 解析并设置关联实体过滤条件
        .relatesTo(parseRelationFilters(relatesTo))
        // 解析并设置被关联实体过滤条件
        .isRelatedTo(parseRelationFilters(isRelatedTo))
        // 解析并设置信息字段过滤条件
        .infoFilters(parseKVFilters(infofilters, false))
        // 解析并设置配置字段过滤条件
        .configFilters(parseKVFilters(conffilters, true))
        // 解析并设置指标过滤条件
        .metricFilters(parseMetricFilters(metricfilters))
        // 解析并设置事件过滤条件
        .eventFilters(parseEventFilters(eventfilters))
        // 解析并设置分页起始实体ID
        .fromId(parseStr(fromid)).build();
  }

  /**
   * 解析查询参数构建需要返回的时间线数据对象。
   * @param confs 需要返回的配置列表
   * @param metrics 需要返回的指标列表
   * @param fields 需要返回的字段列表
   * @param metricsLimit 返回指标数量上限
   * @param metricsTimeBegin 指标时间起始
   * @param metricsTimeEnd 指标时间结束
   * @return 构建完成的待获取数据对象
   * @throws TimelineParseException 解析失败时抛出异常
   */
  static TimelineDataToRetrieve createTimelineDataToRetrieve(String confs,
      String metrics, String fields, String metricsLimit,
      String metricsTimeBegin, String metricsTimeEnd)
      throws TimelineParseException {
    return new TimelineDataToRetrieve(parseDataToRetrieve(confs),
        parseDataToRetrieve(metrics), parseFieldsStr(fields,
        TimelineParseConstants.COMMA_DELIMITER), parseIntStr(metricsLimit),
        parseLongStr(metricsTimeBegin), parseLongStr(metricsTimeEnd));
  }

  /**
   * 解析事件过滤表达式。
   * @param expr 事件过滤表达式
   * @return 解析完成的过滤列表
   * @throws TimelineParseException 解析失败时抛出异常
   */
  static TimelineFilterList parseEventFilters(String expr)
      throws TimelineParseException {
    return parseFilters(new TimelineParserForExistFilters(expr,
        TimelineParseConstants.COMMA_CHAR));
  }

  /**
   * 解析关系过滤表达式。
   * @param expr 关系过滤表达式
   * @return 解析完成的过滤列表
   * @throws TimelineParseException 解析失败时抛出异常
   */
  static TimelineFilterList parseRelationFilters(String expr)
      throws TimelineParseException {
    return parseFilters(new TimelineParserForRelationFilters(expr,
        TimelineParseConstants.COMMA_CHAR,
        TimelineParseConstants.COLON_DELIMITER));
  }

  /**
   * 通用过滤器解析方法，执行解析后关闭解析器资源。
   * @param parser 过滤器解析器
   * @return 解析完成的过滤列表
   * @throws TimelineParseException 解析失败时抛出异常
   */
  private static TimelineFilterList parseFilters(TimelineParser parser)
      throws TimelineParseException {
    try {
      return parser.parse();
    } finally {
      IOUtils.closeStream(parser);
    }
  }

  /**
   * 解析键值对类型过滤表达式（信息/配置过滤）。
   * @param expr 键值对过滤表达式
   * @param valueAsString 是否将值作为字符串解析，true为配置过滤，false为信息过滤
   * @return 解析完成的过滤列表
   * @throws TimelineParseException 解析失败时抛出异常
   */
  static TimelineFilterList parseKVFilters(String expr, boolean valueAsString)
      throws TimelineParseException {
    return parseFilters(new TimelineParserForKVFilters(expr, valueAsString));
  }

  /**
   * 解析逗号分隔的字段字符串转换为Field枚举集合。
   * @param str 待解析字段字符串
   * @param delimiter 分隔符
   * @return 解析完成的Field枚举集合
   */
  static EnumSet<Field> parseFieldsStr(String str, String delimiter) {
    if (str == null) {
      return null;
    }
    // 按分隔符切分字符串
    String[] strs = str.split(delimiter);
    // 创建空枚举集合
    EnumSet<Field> fieldList = EnumSet.noneOf(Field.class);
    for (String s : strs) {
      try {
        // 转换为枚举并添加到集合
        fieldList.add(Field.valueOf(s.trim().toUpperCase()));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(s + " is not a valid field.");
      }
    }
    return fieldList;
  }

  /**
   * 解析指标过滤表达式。
   * @param expr 指标过滤表达式
   * @return 解析完成的过滤列表
   * @throws TimelineParseException 解析失败时抛出异常
   */
  static TimelineFilterList parseMetricFilters(String expr)
      throws TimelineParseException {
    return parseFilters(new TimelineParserForNumericFilters(expr));
  }

  /**
   * 将字符串解析为Long类型，空输入返回null。
   * @param str 待解析字符串
   * @return 解析后的Long值，输入为null时返回null
   */
  static Long parseLongStr(String str) {
    return str == null ? null : Long.parseLong(str.trim());
  }

  /**
   * 将字符串解析为Integer类型，空输入返回null。
   * @param str 待解析字符串
   * @return 解析后的Integer值，输入为null时返回null
   */
  static Integer parseIntStr(String str) {
    return str == null ? null : Integer.parseInt(str.trim());
  }

  /**
   * 修剪字符串，空字符串或全空格返回null。
   * @param str 待处理字符串
   * @return 修剪后的字符串，空输入返回null
   */
  static String parseStr(String str) {
    return StringUtils.trimToNull(str);
  }

  /**
   * 从HTTP请求中获取请求用户的UGI对象。
   * @param req HTTP请求对象
   * @return 请求用户的UGI对象，未找到用户信息返回null
   */
  public static UserGroupInformation getUser(HttpServletRequest req) {
    Principal princ = req.getUserPrincipal();
    String remoteUser = princ == null ? null : princ.getName();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }

    return callerUGI;
  }

  /**
   * 从UGI对象中获取用户名，空UGI返回空字符串。
   * @param callerUGI 调用者UGI对象
   * @return 修剪后的用户名
   */
  static String getUserName(UserGroupInformation callerUGI) {
    return ((callerUGI != null) ? callerUGI.getUserName().trim() : "");
  }

  /**
   * 解析待获取数据（配置/指标）列表表达式。
   * @param expr 待获取数据表达式
   * @return 解析完成的过滤列表
   * @throws TimelineParseException 解析失败时抛出异常
   */
  static TimelineFilterList parseDataToRetrieve(String expr)
        throws TimelineParseException {
    return parseFilters(new TimelineParserForDataToRetrieve(expr));
  }
}