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

import java.util.Deque;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;

/**
 * 时间线比较表达式解析抽象基类，负责将用户输入的比较表达式字符串解析为 TimelineFilter 过滤器树
 * 比较表达式格式：(key 比较符 value) 逻辑运算符 (key 比较符 value)
 * 比较符支持：等于(eq)、不等于(ne，键不存在也匹配)、存在且不等于(ene)、小于(lt)、大于(gt)、小于等于(le)、大于等于(ge)
 * 逻辑运算符支持：AND、OR
 */
@Private
@Unstable
abstract class TimelineParserForCompareExpr implements TimelineParser {
  /**
   * 解析状态枚举，标记当前解析到表达式的哪个部分
   */
  private enum ParseState {
    /** 正在解析键 */
    PARSING_KEY,
    /** 正在解析值 */
    PARSING_VALUE,
    /** 正在解析逻辑运算符 */
    PARSING_OP,
    /** 正在解析比较运算符 */
    PARSING_COMPAREOP
  }
  // 原始表达式字符串
  private final String expr;
  // 转换为小写的表达式字符串，用于不区分大小写的比较符/运算符匹配
  private final String exprInLowerCase;
  // 表达式名称，用于错误日志标识
  private final String exprName;
  // 当前解析偏移量
  private int offset = 0;
  // 当前键/值段开始偏移量
  private int kvStartOffset = 0;
  // 表达式总长度
  private final int exprLength;
  // 当前解析状态
  private ParseState currentParseState = ParseState.PARSING_KEY;
  // 过滤器列表栈，用于处理嵌套括号，保存外层逻辑运算符列表
  private Deque<TimelineFilterList> filterListStack = new LinkedList<>();
  // 当前正在构建的比较过滤器
  private TimelineFilter currentFilter = null;
  // 当前正在构建的过滤器列表（逻辑分组）
  private TimelineFilterList filterList = null;

  /**
   * 构造比较表达式解析器
   * @param expression 待解析的表达式字符串
   * @param name 表达式名称，用于错误标识
   */
  public TimelineParserForCompareExpr(String expression, String name) {
    if (expression != null) {
      expr = expression.trim();
      exprLength = expr.length();
      exprInLowerCase = expr.toLowerCase();
    } else {
      expr = null;
      exprInLowerCase = null;
      exprLength = 0;
    }
    this.exprName = name;
  }

  protected TimelineFilter getCurrentFilter() {
    return currentFilter;
  }

  protected TimelineFilter getFilterList() {
    return filterList;
  }

  /**
   * 创建具体类型的过滤器，由子类实现
   * @return 新建的过滤器实例
   */
  protected abstract TimelineFilter createFilter();

  /**
   * 解析字符串形式的值为具体类型，由子类实现
   * @param strValue 字符串值
   * @return 解析后的对象
   * @throws TimelineParseException 解析失败抛出异常
   */
  protected abstract Object parseValue(String strValue)
      throws TimelineParseException;

  /**
   * 将比较运算符设置到当前过滤器，由子类实现
   * @param compareOp 比较运算符
   * @param keyMustExistFlag 键是否必须存在才能匹配
   * @throws TimelineParseException 设置失败抛出异常
   */
  protected abstract void setCompareOpToCurrentFilter(
      TimelineCompareOp compareOp, boolean keyMustExistFlag)
      throws TimelineParseException;

  /**
   * 将解析后的值设置到当前过滤器，由子类实现
   * @param value 解析后的值
   */
  protected abstract void setValueToCurrentFilter(Object value);

  /**
   * 处理空格字符，根据当前解析状态提取键或值
   * @throws TimelineParseException 解析错误抛出异常
   */
  private void handleSpaceChar() throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_KEY ||
        currentParseState == ParseState.PARSING_VALUE) {
      if (kvStartOffset == offset) {
        kvStartOffset++;
        offset++;
        return;
      }
      String str = expr.substring(kvStartOffset, offset);
      if (currentParseState == ParseState.PARSING_KEY) {
        if (currentFilter == null) {
          currentFilter = createFilter();
        }
        ((TimelineCompareFilter)currentFilter).setKey(str);
        currentParseState = ParseState.PARSING_COMPAREOP;
      } else if (currentParseState == ParseState.PARSING_VALUE) {
        if (currentFilter != null) {
          setValueToCurrentFilter(parseValue(str));
        }
        currentParseState = ParseState.PARSING_OP;
      }
    }
    offset++;
  }

  /**
   * 处理左括号，保存当前过滤器列表到栈，开始新的嵌套分组
   * @throws TimelineParseException 位置错误抛出异常
   */
  private void handleOpeningBracketChar() throws TimelineParseException {
    if (currentParseState != ParseState.PARSING_KEY) {
      throw new TimelineParseException("Encountered unexpected opening " +
          "bracket while parsing " + exprName + ".");
    }
    offset++;
    kvStartOffset = offset;
    filterListStack.push(filterList);
    filterList = null;
  }

  /**
   * 处理右括号，结束当前嵌套分组，将当前分组合并到外层过滤器列表
   * @throws TimelineParseException 括号不匹配或位置错误抛出异常
   */
  private void handleClosingBracketChar() throws TimelineParseException {
    if (currentParseState != ParseState.PARSING_VALUE &&
        currentParseState != ParseState.PARSING_OP) {
      throw new TimelineParseException("Encountered unexpected closing " +
          "bracket while parsing " + exprName + ".");
    }
    if (!filterListStack.isEmpty()) {
      if (currentParseState == ParseState.PARSING_VALUE) {
        setValueToCurrentFilter(
            parseValue(expr.substring(kvStartOffset, offset)));
        currentParseState = ParseState.PARSING_OP;
      }
      if (currentFilter != null) {
        filterList.addFilter(currentFilter);
      }
      // 弹出栈中保存的外层过滤器列表，将当前分组添加到外层
      TimelineFilterList fList = filterListStack.pop();
      if (fList != null) {
        fList.addFilter(filterList);
        filterList = fList;
      }
      currentFilter = null;
      offset++;
      kvStartOffset = offset;
    } else {
      throw new TimelineParseException("Encountered unexpected closing " +
          "bracket while parsing " + exprName + ".");
    }
  }

  /**
   * 解析比较运算符，识别不同的比较操作符并设置到当前过滤器
   * @throws TimelineParseException 无法识别运算符抛出异常
   */
  private void parseCompareOp() throws TimelineParseException {
    if (offset + 2 >= exprLength) {
      throw new TimelineParseException("Compare op cannot be parsed for " +
          exprName + ".");
    }
    TimelineCompareOp compareOp = null;
    boolean keyExistFlag = true;
    if (expr.charAt(offset + 2) == TimelineParseConstants.SPACE_CHAR) {
      if (exprInLowerCase.startsWith("eq", offset)) {
        compareOp = TimelineCompareOp.EQUAL;
      } else if (exprInLowerCase.startsWith("ne", offset)) {
        compareOp = TimelineCompareOp.NOT_EQUAL;
        keyExistFlag = false;
      } else if (exprInLowerCase.startsWith("lt", offset)) {
        compareOp = TimelineCompareOp.LESS_THAN;
      } else if (exprInLowerCase.startsWith("le", offset)) {
        compareOp = TimelineCompareOp.LESS_OR_EQUAL;
      } else if (exprInLowerCase.startsWith("gt", offset)) {
        compareOp = TimelineCompareOp.GREATER_THAN;
      } else if (exprInLowerCase.startsWith("ge", offset)) {
        compareOp = TimelineCompareOp.GREATER_OR_EQUAL;
      }
      offset = offset + 3;
    } else if (exprInLowerCase.startsWith("ene ", offset)) {
      // 不等比较，但要求键必须存在
      compareOp = TimelineCompareOp.NOT_EQUAL;
      offset = offset + 4;
    }
    if (compareOp == null) {
      throw new TimelineParseException("Compare op cannot be parsed for " +
          exprName + ".");
    }
    setCompareOpToCurrentFilter(compareOp, keyExistFlag);
    kvStartOffset = offset;
    currentParseState = ParseState.PARSING_VALUE;
  }

  /**
   * 解析逻辑运算符（AND/OR），处理过滤器分组
   * @param closingBracket 是否刚处理完右括号
   * @throws TimelineParseException 无法识别运算符抛出异常
   */
  private void parseOp(boolean closingBracket) throws TimelineParseException {
    Operator operator = null;
    if (exprInLowerCase.startsWith("or ", offset)) {
      operator = Operator.OR;
      offset = offset + 3;
    } else if (exprInLowerCase.startsWith("and ", offset)) {
      operator = Operator.AND;
      offset = offset + 4;
    }
    if (operator == null) {
      throw new TimelineParseException("Operator cannot be parsed for " +
          exprName + ".");
    }
    if (filterList == null) {
      filterList = new TimelineFilterList(operator);
    }
    if (currentFilter != null) {
      filterList.addFilter(currentFilter);
    }
    if (closingBracket || filterList.getOperator() != operator) {
      filterList = new TimelineFilterList(operator, filterList);
    }
    currentFilter = null;
    kvStartOffset = offset;
    currentParseState = ParseState.PARSING_KEY;
  }

  /**
   * 执行表达式解析，生成过滤器树
   * @return 解析完成的根过滤器列表
   * @throws TimelineParseException 解析错误抛出异常
   */
  @Override
  public TimelineFilterList parse() throws TimelineParseException {
    if (expr == null || exprLength == 0) {
      return null;
    }
    boolean closingBracket = false;
    while (offset < exprLength) {
      char offsetChar = expr.charAt(offset);
      switch(offsetChar) {
      case TimelineParseConstants.SPACE_CHAR:
        handleSpaceChar();
        break;
      case TimelineParseConstants.OPENING_BRACKET_CHAR:
        handleOpeningBracketChar();
        break;
      case TimelineParseConstants.CLOSING_BRACKET_CHAR:
        handleClosingBracketChar();
        closingBracket = true;
        break;
      default: // 其他字符
        // 根据当前状态处理
        if (currentParseState == ParseState.PARSING_COMPAREOP) {
          parseCompareOp();
        } else if (currentParseState == ParseState.PARSING_OP) {
          parseOp(closingBracket);
          closingBracket = false;
        } else {
          // 键或值的一部分，继续偏移
          offset++;
        }
        break;
      }
    }
    if (!filterListStack.isEmpty()) {
      filterListStack.clear();
      throw new TimelineParseException("Encountered improper brackets while " +
          "parsing " + exprName + ".");
    }
    if (currentParseState == ParseState.PARSING_VALUE) {
      setValueToCurrentFilter(
          parseValue(expr.substring(kvStartOffset, offset)));
    }
    if (filterList == null || filterList.getFilterList().isEmpty()) {
      if (currentFilter == null) {
        throw new TimelineParseException(
            "Invalid expression provided for " + exprName);
      } else {
        filterList = new TimelineFilterList(currentFilter);
      }
    } else if (currentFilter != null) {
      filterList.addFilter(currentFilter);
    }
    return filterList;
  }

  /**
   * 清理解析过程中的临时数据，释放资源
   */
  @Override
  public void close() {
    if (filterListStack != null) {
      filterListStack.clear();
    }
    filterList = null;
    currentFilter = null;
  }
}