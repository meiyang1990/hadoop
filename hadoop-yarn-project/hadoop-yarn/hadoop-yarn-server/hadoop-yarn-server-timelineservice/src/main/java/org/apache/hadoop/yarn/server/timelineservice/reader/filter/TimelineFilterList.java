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

package org.apache.hadoop.yarn.server.timelineservice.reader.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * 时间线服务过滤器列表实现，维护有序过滤器集合，通过指定逻辑操作符（AND/OR）对所有过滤器进行组合求值，
 * 支持嵌套过滤器列表以构建层级过滤条件，实现复杂的组合过滤逻辑。
 */
@Private
@Unstable
public class TimelineFilterList extends TimelineFilter {
  /**
   * 定义过滤器列表中多个条件的逻辑操作符，AND要求所有条件匹配，OR要求至少一个条件匹配。
   */
  @Private
  @Unstable
  public enum Operator {
    AND,
    OR
  }

  private Operator operator;
  private List<TimelineFilter> filterList = new ArrayList<TimelineFilter>();

  /**
   * 使用默认AND操作符构造过滤器列表。
   * @param filters 要包含的过滤器数组
   */
  public TimelineFilterList(TimelineFilter...filters) {
    this(Operator.AND, filters);
  }

  /**
   * 使用默认AND操作符构造空过滤器列表。
   */
  public TimelineFilterList() {
    this(Operator.AND);
  }

  /**
   * 使用指定操作符构造空过滤器列表。
   * @param op 逻辑操作符
   */
  public TimelineFilterList(Operator op) {
    this.operator = op;
  }

  /**
   * 使用指定操作符和过滤器数组构造过滤器列表。
   * @param op 逻辑操作符
   * @param filters 要包含的过滤器数组
   */
  public TimelineFilterList(Operator op, TimelineFilter...filters) {
    this.operator = op;
    this.filterList = new ArrayList<TimelineFilter>(Arrays.asList(filters));
  }

  @Override
  public TimelineFilterType getFilterType() {
    return TimelineFilterType.LIST;
  }

  /**
   * 获取当前过滤器列表包含的所有过滤器。
   *
   * @return 过滤器列表
   */
  public List<TimelineFilter> getFilterList() {
    return filterList;
  }

  /**
   * 获取当前使用的逻辑操作符。
   *
   * @return 逻辑操作符
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * 设置逻辑操作符。
   * @param op 要设置的操作符
   */
  public void setOperator(Operator op) {
    operator = op;
  }

  /**
   * 向过滤器列表添加一个新的过滤器。
   * @param filter 要添加的过滤器
   */
  public void addFilter(TimelineFilter filter) {
    filterList.add(filter);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result + ((filterList == null) ? 0 : filterList.hashCode());
    result =
        prime * result + ((operator == null) ? 0 : operator.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TimelineFilterList other = (TimelineFilterList) obj;
    if (operator != other.operator) {
      return false;
    }
    if (filterList == null) {
      if (other.filterList != null) {
        return false;
      }
    } else if (!filterList.equals(other.filterList)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("TimelineFilterList %s (%d): %s",
        this.operator, this.filterList.size(), this.filterList.toString());
  }
}