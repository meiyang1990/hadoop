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
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;

/**
 * 存在性过滤器解析器，用于解析如事件过滤器这类需要检查值是否存在的过滤表达式。
 * 这类过滤器会检查指定值是否存在，例如事件过滤器会检查事件是否存在，仅返回匹配的实体。
 */
@Private
@Unstable
class TimelineParserForExistFilters extends TimelineParserForEqualityExpr {

  /**
   * 构造存在性过滤器解析器
   * @param expression 待解析的过滤表达式
   * @param delimiter 分隔符
   */
  public TimelineParserForExistFilters(String expression, char delimiter) {
    super(expression, "Event Filter", delimiter);
  }

  /**
   * 创建存在性过滤对象
   * @return 存在性过滤器实例
   */
  protected TimelineFilter createFilter() {
    return new TimelineExistsFilter();
  }

  protected void setValueToCurrentFilter(String value) {
    ((TimelineExistsFilter)getCurrentFilter()).setValue(value);
  }

  protected void setCompareOpToCurrentFilter(TimelineCompareOp compareOp) {
    ((TimelineExistsFilter)getCurrentFilter()).setCompareOp(compareOp);
  }
}