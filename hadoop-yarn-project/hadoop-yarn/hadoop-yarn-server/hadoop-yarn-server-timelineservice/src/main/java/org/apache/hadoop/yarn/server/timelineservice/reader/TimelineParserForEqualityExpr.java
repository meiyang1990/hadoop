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
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;

/**
 * 文件级：时间线服务相等表达式解析抽象基类，负责解析形如(val,val) OP !(val,val)的相等比较表达式
 * 抽象类 for parsing equality expressions. This means the values in
 * expression would either be equal or not equal.
 * Equality expressions are of the form :
 * (&lt;value&gt;,&lt;value&gt;,&lt;value&gt;) &lt;op&gt; !(&lt;value&gt;,
 * &lt;value&gt;)
 *
 * Here, "!" means all the values should not exist/should not be equal.
 * If not specified, they should exist/be equal.
 *
 * op is a logical operator and can be either AND or OR.
 *
 * The way values will be interpreted would also depend on implementation.
 *
 * For instance for event filters this expression may look like,
 * (event1,event2) AND !(event3,event4)
 * This means for an entity to match, event1 and event2 should exist. But event3
 * and event4 should not exist.
 */
@Private
@Unstable
abstract class TimelineParserForEqualityExpr implements TimelineParser {
  /**
   * 解析状态枚举，定义不同阶段的解析状态
   */
  private enum ParseState {
    PARSING_VALUE,    // 正在解析值
    PARSING_OP,       // 正在解析逻辑操作符（AND/OR）
    PARSING_COMPAREOP // 正在解析比较操作符（等于/不等于）
  }
  // 待解析的原始表达式
  private final String expr;
  // 转换为小写的表达式，用于不区分大小写的操作符匹配
  private final String exprInLowerCase;
  // 表达式名称，用于错误信息标识
  private final String exprName;
  // 当前解析偏移量
  private int offset = 0;
  // 当前解析值的起始偏移量
  private int startOffset = 0;
  // 表达式总长度
  private final int exprLength;
  // 当前解析状态
  private ParseState currentParseState = ParseState.PARSING_COMPAREOP;
  // 当前使用的比较操作符
  private TimelineCompareOp currentCompareOp = null;
  // 过滤器栈，存储括号层级对应的过滤器列表，用于嵌套括号处理
  private Deque<TimelineFilterList> filterListStack = new LinkedList<>();
  // 当前正在构造的过滤器
  private TimelineFilter currentFilter = null;
  // 当前层级的过滤器列表
  private TimelineFilterList filterList = null;
  // 分隔值的分隔符
  private final char delimiter;

  /**
   * 构造函数，初始化相等表达式解析器
   * @param expression 待解析的表达式字符串
   * @param name 表达式名称，用于错误提示
   * @param delim 值分隔符
   */
  public TimelineParserForEqualityExpr(String expression, String name,
      char delim) {
    if (expression != null) {
      expr = expression.trim();
      exprLength = expr.length();
      exprInLowerCase = expr.toLowerCase();
    } else {
      exprLength = 0;
      expr = null;
      exprInLowerCase = null;
    }
    exprName = name;
    delimiter = delim;
  }

  protected TimelineFilter getCurrentFilter() {
    return currentFilter;
  }

  protected TimelineFilter getFilterList() {
    return filterList;
  }

  /**
   * 根据具体实现创建过滤器实例
   * @return 创建好的过滤器实例
   */
  protected abstract TimelineFilter createFilter();

  /**
   * 为当前过滤器设置比较操作符
   * @param compareOp 待设置的比较操作符
   * @throws TimelineParseException 解析错误时抛出异常
   */
  protected abstract void setCompareOpToCurrentFilter(
      TimelineCompareOp compareOp) throws TimelineParseException;

  /**
   * 为当前过滤器设置解析到的值
   * @param value 待设置的值
   * @throws TimelineParseException 解析错误时抛出异常
   */
  protected abstract void setValueToCurrentFilter(String value)
      throws TimelineParseException;

  /**
   * 创建过滤器并设置值到当前过滤器
   * @param checkIfNull 是否检查当前过滤器为空才创建新过滤器
   * @throws TimelineParseException 解析错误时抛出异常
   */
  private void createAndSetFilter(boolean checkIfNull)
      throws TimelineParseException {
    if (!checkIfNull || currentFilter == null) {
      currentFilter = createFilter();
      setCompareOpToCurrentFilter(currentCompareOp);
    }
    setValueToCurrentFilter(expr.substring(startOffset, offset).trim());
  }

  /**
   * 处理空格字符，根据当前解析状态做不同处理
   * @throws TimelineParseException 解析错误时抛出异常
   */
  private void handleSpaceChar() throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_VALUE) {
      if (startOffset == offset) {
        // 空格在值开头，跳过空格，起始偏移后移
        startOffset++;
      } else {
        // 值解析完成，创建过滤器，切换到操作符解析状态
        createAndSetFilter(true);
        currentParseState = ParseState.PARSING_OP;
      }
    }
    // 偏移量后移
    offset++;
  }

  /**
   * 处理分隔符，完成当前值解析，准备解析下一个值
   * @throws TimelineParseException 解析错误时抛出异常
   */
  private void handleDelimiter() throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_OP ||
        currentParseState == ParseState.PARSING_VALUE) {
      if (currentParseState == ParseState.PARSING_VALUE) {
        // 完成当前值解析，创建过滤器
        createAndSetFilter(false);
      }
      if (filterList == null) {
        filterList = new TimelineFilterList();
      }
      // 将当前过滤器加入列表，重置当前过滤器准备解析下一个值
      filterList.addFilter(currentFilter);
      currentFilter = null;
      offset++;
      startOffset = offset;
      currentParseState = ParseState.PARSING_VALUE;
    } else {
      throw new TimelineParseException("Invalid " + exprName + "expression.");
    }
  }

  /**
   * 处理左括号，开始一个新的过滤器列表层级
   * @param encounteredNot 是否前面带非操作符!
   * @throws TimelineParseException 解析错误时抛出异常
   */
  private void handleOpeningBracketChar(boolean encounteredNot)
      throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_COMPAREOP ||
        currentParseState == ParseState.PARSING_VALUE) {
      offset++;
      startOffset = offset;
      // 将当前过滤器列表压栈保存，开始新层级
      filterListStack.push(filterList);
      filterList = null;
      if (currentFilter == null) {
        currentFilter = createFilter();
      }
      // 根据是否带!设置比较操作符
      currentCompareOp = encounteredNot ?
          TimelineCompareOp.NOT_EQUAL : TimelineCompareOp.EQUAL;
      setCompareOpToCurrentFilter(currentCompareOp);
      // 切换到值解析状态
      currentParseState = ParseState.PARSING_VALUE;
    } else {
      throw new TimelineParseException("Encountered unexpected opening " +
          "bracket while parsing " + exprName + ".");
    }
  }

  /**
   * 处理非操作符!，处理后期待左括号
   * @throws TimelineParseException 解析错误时抛出异常
   */
  private void handleNotChar() throws TimelineParseException {
    if (currentParseState == ParseState.PARSING_COMPAREOP ||
        currentParseState == ParseState.PARSING_VALUE) {
      offset++;
      // 跳过!和左括号之间的空格
      while (offset < exprLength &&
          expr.charAt(offset) == TimelineParseConstants.SPACE_CHAR) {
        offset++;
      }
      if (offset == exprLength) {
        throw new TimelineParseException("Invalid " + exprName + "expression");
      }
      // !后必须接左括号
      if (expr.charAt(offset) == TimelineParseConstants.OPENING_BRACKET_CHAR) {
        handleOpeningBracketChar(true);
      } else {
        throw new TimelineParseException("Invalid " + exprName + "expression");
      }
    } else {
      throw new TimelineParseException("Encountered unexpected not(!) char " +
         "while parsing " + exprName + ".");
    }
  }

  /**
   * 处理右括号，结束当前层级，合并到上层过滤器列表
   * @throws TimelineParseException 解析错误时抛出异常
   */
  private void handleClosingBracketChar() throws TimelineParseException {
    if (currentParseState != ParseState.PARSING_VALUE &&
        currentParseState != ParseState.PARSING_OP) {
      throw new TimelineParseException("Encountered unexpected closing " +
          "bracket while parsing " + exprName + ".");
    }
    if (!filterListStack.isEmpty()) {
      if (currentParseState == ParseState.PARSING_VALUE) {
        if (startOffset != offset) {
          // 完成括号内最后一个值解析
          createAndSetFilter(true);
          currentParseState = ParseState.PARSING_OP;
        }
      }
      if (filterList == null) {
        filterList = new TimelineFilterList();
      }
      if (currentFilter != null) {
        filterList.addFilter(currentFilter);
      }
      // 弹出栈中保存的上层过滤器列表，合并当前列表到上层
      TimelineFilterList fList = filterListStack.pop();
      if (fList != null) {
        fList.addFilter(filterList);
        filterList = fList;
      }
      currentFilter = null;
      offset++;
      startOffset = offset;
    } else {
      throw new TimelineParseException("Encountered unexpected closing " +
          "bracket while parsing " + exprName + ".");
    }
  }

  /**
   * 解析逻辑操作符AND/OR，设置过滤器列表的逻辑操作符
   * @param closingBracket 是否刚刚处理完右括号
   * @throws TimelineParseException 解析错误时抛出异常
   */
  private void parseOp(boolean closingBracket) throws TimelineParseException {
    Operator operator = null;
    // 匹配OR操作符
    if (exprInLowerCase.startsWith("or ", offset)) {
      operator = Operator.OR;
      offset = offset + 3;
    } else if (exprInLowerCase.startsWith("and ", offset)) {
      // 匹配AND操作符
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
    // 如果刚处理完右括号或操作符变更，创建新的过滤器列表层级
    if (closingBracket || filterList.getOperator() != operator) {
      filterList = new TimelineFilterList(operator, filterList);
    }
    currentFilter = null;
    startOffset = offset;
    // 切换到比较操作符解析状态
    currentParseState = ParseState.PARSING_COMPAREOP;
  }

  /**
   * 解析比较操作符，默认使用EQUAL比较操作符
   * @throws TimelineParseException 解析错误时抛出异常
   */
  private void parseCompareOp() throws TimelineParseException {
    if (currentFilter == null) {
      currentFilter = createFilter();
    }
    // 未显式指定!时默认等于
    currentCompareOp = TimelineCompareOp.EQUAL;
    setCompareOpToCurrentFilter(currentCompareOp);
    // 切换到值解析状态
    currentParseState = ParseState.PARSING_VALUE;
  }

  @Override
  /**
   * 执行表达式解析，生成过滤器列表
   * @return 解析完成的过滤器列表
   * @throws TimelineParseException 解析错误时抛出异常
   */
  public TimelineFilterList parse() throws TimelineParseException {
    if (expr == null || exprLength == 0) {
      return null;
    }
    boolean closingBracket = false;
    // 遍历表达式每个字符，按字符类型分发处理
    while (offset < exprLength) {
      char offsetChar = expr.charAt(offset);
      switch(offsetChar) {
      case TimelineParseConstants.NOT_CHAR:
        handleNotChar();
        break;
      case TimelineParseConstants.SPACE_CHAR:
        handleSpaceChar();
        break;
      case TimelineParseConstants.OPENING_BRACKET_CHAR:
        handleOpeningBracketChar(false);
        break;
      case TimelineParseConstants.CLOSING_BRACKET_CHAR:
        handleClosingBracketChar();
        closingBracket = true;
        break;
      default: // other characters.
        if (offsetChar == delimiter) {
          handleDelimiter();
        } else if (currentParseState == ParseState.PARSING_COMPAREOP) {
          parseCompareOp();
        } else if (currentParseState == ParseState.PARSING_OP) {
          parseOp(closingBracket);
          closingBracket = false;
        } else {
          offset++;
        }
        break;
      }
    }
    // 解析结束后栈不为空说明括号不匹配
    if (!filterListStack.isEmpty()) {
      filterListStack.clear();
      throw new TimelineParseException("Encountered improper brackets while " +
          "parsing " + exprName + ".");
    }
    // 处理表达式末尾未完成的值解析
    if (currentParseState == ParseState.PARSING_VALUE) {
      if (startOffset != offset) {
        createAndSetFilter(true);
      }
    }
    // 处理结果为空或只有单个过滤器的情况
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

  @Override
  /**
   * 清理解析过程中的临时资源
   */
  public void close() {
    if (filterListStack != null) {
      filterListStack.clear();
    }
    currentFilter = null;
    filterList = null;
  }
}