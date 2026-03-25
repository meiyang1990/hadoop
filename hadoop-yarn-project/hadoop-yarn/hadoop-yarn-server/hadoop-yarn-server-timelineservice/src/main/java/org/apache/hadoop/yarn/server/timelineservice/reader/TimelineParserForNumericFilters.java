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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;

/**
 * 文件说明：时间线服务数值过滤器解析器，专门用于解析数值类过滤条件(如指标过滤)表达式
 * 
 * 用于解析数值过滤器，例如指标过滤器。将字符串形式的比较表达式转换为Timeline过滤器对象
 */
@Private
@Unstable
class TimelineParserForNumericFilters extends TimelineParserForCompareExpr {

  /**
   * 构造函数，初始化数值过滤器解析器
   * @param expression 需要解析的过滤表达式字符串
   */
  public TimelineParserForNumericFilters(String expression) {
    super(expression, "Metric Filter");
  }

  /**
   * 创建数值比较过滤器实例
   * @return 新建的Timeline比较过滤器对象
   */
  protected TimelineFilter createFilter() {
    return new TimelineCompareFilter();
  }

  @Override
  /**
   * 为当前过滤器设置比较操作符和键必须存在标志
   * @param compareOp 比较操作符(等于、大于、小于等)
   * @param keyMustExistFlag 过滤键必须存在的标志，为true表示过滤键不存在时结果不匹配
   */
  protected void setCompareOpToCurrentFilter(TimelineCompareOp compareOp,
      boolean keyMustExistFlag) {
    ((TimelineCompareFilter)getCurrentFilter()).setCompareOp(
        compareOp, keyMustExistFlag);
  }

  /**
   * 解析字符串值为数值对象，验证是否为合法整数类型
   * @param strValue 需要解析的字符串值
   * @return 解析后的数值对象
   * @throws TimelineParseException 解析失败或值不是合法数值时抛出异常
   */
  protected Object parseValue(String strValue) throws TimelineParseException {
    Object value = null;
    try {
      // 使用通用对象读取器反序列化字符串为Java对象
      value = GenericObjectMapper.OBJECT_READER.readValue(strValue);
    } catch (IOException e) {
      throw new TimelineParseException("Value cannot be parsed.");
    }
    // 验证解析结果是非空整数类型数值
    if (value == null || !(TimelineStorageUtils.isIntegralValue(value))) {
      throw new TimelineParseException("Value is not a number.");
    }
    return value;
  }

  /**
   * 将解析后的数值设置到当前过滤器中
   * @param value 解析完成的数值对象
   */
  protected void setValueToCurrentFilter(Object value) {
    TimelineFilter currentFilter = getCurrentFilter();
    if (currentFilter != null) {
      ((TimelineCompareFilter)currentFilter).setValue(value);
    }
  }
}