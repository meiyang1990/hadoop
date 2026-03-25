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
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;

/**
 * 用于解析待检索的指标或配置的查询表达式，生成对应的过滤条件列表
 */
@Private
@Unstable
public class TimelineParserForDataToRetrieve implements TimelineParser {
  private String expr;
  private final int exprLength;

  /**
   * 构造解析器，初始化待解析的查询表达式
   * @param expression 待解析的查询表达式
   */
  public TimelineParserForDataToRetrieve(String expression) {
    this.expr = expression;
    if (expression != null) {
      this.expr = expr.trim();
      exprLength = expr.length();
    } else {
      exprLength = 0;
    }
  }

  @Override
  public TimelineFilterList parse() throws TimelineParseException {
    // 表达式为空直接返回null
    if (expr == null || exprLength == 0) {
      return null;
    }
    TimelineCompareOp compareOp = null;
    // 查找左括号位置
    int openingBracketIndex =
        expr.indexOf(TimelineParseConstants.OPENING_BRACKET_CHAR);
    // 表达式开头是NOT符号，代表不等于过滤
    if (expr.charAt(0) == TimelineParseConstants.NOT_CHAR) {
      // 不存在左括号，表达式非法
      if (openingBracketIndex == -1) {
        throw new TimelineParseException("Invalid config/metric to retrieve " +
            "expression");
      }
      // NOT符号和左括号之间必须只有一个操作符，否则非法
      if (openingBracketIndex != 1 &&
          expr.substring(1, openingBracketIndex + 1).trim().length() != 1) {
        throw new TimelineParseException("Invalid config/metric to retrieve " +
            "expression");
      }
      compareOp = TimelineCompareOp.NOT_EQUAL;
    } else if (openingBracketIndex <= 0) {
      // 无左括号默认是等于匹配
      compareOp = TimelineCompareOp.EQUAL;
    }
    // 获取表达式最后一个字符
    char lastChar = expr.charAt(exprLength - 1);
    // 不等于匹配必须以右括号结尾，否则非法
    if (compareOp == TimelineCompareOp.NOT_EQUAL &&
        lastChar != TimelineParseConstants.CLOSING_BRACKET_CHAR) {
      throw new TimelineParseException("Invalid config/metric to retrieve " +
          "expression");
    }
    // 存在左右括号，提取括号内的实际检索内容
    if (openingBracketIndex != -1 &&
        expr.charAt(exprLength - 1) ==
            TimelineParseConstants.CLOSING_BRACKET_CHAR) {
      expr = expr.substring(openingBracketIndex + 1, exprLength - 1).trim();
    }
    // 提取后内容为空直接返回null
    if (expr.isEmpty()) {
      return null;
    }
    // 不等于匹配各条件之间是AND关系，等于匹配是OR关系
    Operator op =
        (compareOp == TimelineCompareOp.NOT_EQUAL) ? Operator.AND : Operator.OR;
    // 创建过滤列表
    TimelineFilterList list = new TimelineFilterList(op);
    // 按逗号分割多个检索项
    String[] splits = expr.split(TimelineParseConstants.COMMA_DELIMITER);
    // 为每个检索项创建前缀过滤器并添加到列表
    for (String split : splits) {
      list.addFilter(new TimelinePrefixFilter(compareOp, split.trim()));
    }
    return list;
  }

  @Override
  public void close() {
  }
}