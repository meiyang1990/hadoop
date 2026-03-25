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
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;

/**
 * 时间线服务键值过滤器表达式解析器，用于解析配置、信息等键值类型的过滤表达式。
 * 继承通用比较表达式解析器，处理键值过滤特有的逻辑。
 */
@Private
@Unstable
class TimelineParserForKVFilters extends TimelineParserForCompareExpr {
  // 标识是否需要将过滤值强制按字符串解析
  private final boolean valueAsString;

  /**
   * 构造键值过滤器解析器。
   * @param expression 待解析的过滤表达式字符串
   * @param valAsStr 是否将值强制解析为字符串
   */
  public TimelineParserForKVFilters(String expression, boolean valAsStr) {
    super(expression, "Config/Info Filter");
    this.valueAsString = valAsStr;
  }

  /**
   * 创建键值过滤器实例。
   * @return 新建的键值过滤器对象
   */
  protected TimelineFilter createFilter() {
    return new TimelineKeyValueFilter();
  }

  /**
   * 解析过滤值，根据配置决定是否反序列化为对象或保留字符串。
   * @param strValue 待解析的字符串值
   * @return 解析后的对象
   */
  protected Object parseValue(String strValue) {
    if (!valueAsString) {
      try {
        // 尝试反序列化为对应类型对象
        return GenericObjectMapper.OBJECT_READER.readValue(strValue);
      } catch (IOException e) {
        // 反序列化失败，回退为原字符串
        return strValue;
      }
    } else {
      // 强制返回原字符串
      return strValue;
    }
  }

  @Override
  protected void setCompareOpToCurrentFilter(TimelineCompareOp compareOp,
      boolean keyMustExistFlag) throws TimelineParseException {
    // 键值过滤仅支持相等/不等两种比较操作
    if (compareOp != TimelineCompareOp.EQUAL &&
        compareOp != TimelineCompareOp.NOT_EQUAL) {
      throw new TimelineParseException("TimelineCompareOp for kv-filter " +
          "should be EQUAL or NOT_EQUAL");
    }
    ((TimelineKeyValueFilter)getCurrentFilter()).setCompareOp(
        compareOp, keyMustExistFlag);
  }

  @Override
  protected void setValueToCurrentFilter(Object value) {
    TimelineFilter currentFilter = getCurrentFilter();
    if (currentFilter != null) {
      ((TimelineKeyValueFilter)currentFilter).setValue(value);
    }
  }
}