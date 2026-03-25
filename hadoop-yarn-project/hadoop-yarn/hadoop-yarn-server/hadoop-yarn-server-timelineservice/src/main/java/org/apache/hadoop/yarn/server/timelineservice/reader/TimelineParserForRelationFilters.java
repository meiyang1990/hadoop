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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;

/**
 * 文件概述：时间线服务关系过滤器表达式解析器，负责解析基于关系运算的查询过滤条件
 * 
 * 用于解析关系类型的过滤表达式，生成键值对过滤条件，供时间线数据查询使用。
 */
@Private
@Unstable
class TimelineParserForRelationFilters extends
    TimelineParserForEqualityExpr {
  // 键值对分隔符，用于分割关系过滤器中的键和值
  private final String valueDelimiter;

  /**
   * 构造关系过滤器解析器，初始化解析参数
   * @param expression 待解析的关系过滤表达式
   * @param valuesDelim 多个值之间的分隔字符
   * @param valueDelim 键与值之间的分隔字符串
   */
  public TimelineParserForRelationFilters(String expression, char valuesDelim,
      String valueDelim) {
    super(expression, "Relation Filter", valuesDelim);
    valueDelimiter = valueDelim;
  }

  @Override
  protected TimelineFilter createFilter() {
    // 创建键值对过滤器实例
    return new TimelineKeyValuesFilter();
  }

  @Override
  protected void setCompareOpToCurrentFilter(TimelineCompareOp compareOp) {
    // 将比较操作符设置到当前键值对过滤器中
    ((TimelineKeyValuesFilter)getCurrentFilter()).setCompareOp(compareOp);
  }

  @Override
  protected void setValueToCurrentFilter(String value)
       throws TimelineParseException {
    if (value != null) {
      // 按分隔符拆分键值对字符串
      String[] pairStrs = value.split(valueDelimiter);
      // 拆分结果不足2段，表达式格式错误
      if (pairStrs.length < 2) {
        throw new TimelineParseException("Invalid relation filter expression");
      }
      // 提取过滤键，去除首尾空白字符
      String key = pairStrs[0].trim();
      // 收集所有过滤值
      Set<Object> values = new HashSet<Object>();
      for (int i = 1; i < pairStrs.length; i++) {
        values.add(pairStrs[i].trim());
      }
      // 将解析出的键和值设置到当前过滤器
      ((TimelineKeyValuesFilter)getCurrentFilter()).
          setKeyAndValues(key, values);
    }
  }
}