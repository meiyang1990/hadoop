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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * 时间线服务前缀匹配过滤器，基于字符串前缀对时间线实体/事件进行过滤，
 * 支持匹配前缀或不匹配前缀两种匹配方式。
 */
@Private
@Unstable
public class TimelinePrefixFilter extends TimelineFilter {

  /** 比较操作符，仅支持EQUAL（匹配前缀）或NOT_EQUAL（不匹配前缀） */
  private TimelineCompareOp compareOp;
  /** 待匹配的前缀字符串 */
  private String prefix;

  /**
   * 默认构造函数。
   */
  public TimelinePrefixFilter() {
  }

  /**
   * 构造前缀过滤器，校验操作符合法性。
   * @param op 比较操作符，仅允许EQUAL或NOT_EQUAL
   * @param prefix 待匹配的前缀字符串
   */
  public TimelinePrefixFilter(TimelineCompareOp op, String prefix) {
    this.prefix = prefix;
    if (op != TimelineCompareOp.EQUAL && op != TimelineCompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("CompareOp for prefix filter should " +
          "be EQUAL or NOT_EQUAL");
    }
    this.compareOp = op;
  }

  @Override
  public TimelineFilterType getFilterType() {
    return TimelineFilterType.PREFIX;
  }

  /**
   * 获取待匹配的前缀字符串。
   * @return 前缀字符串
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * 获取比较操作符。
   * @return 比较操作符
   */
  public TimelineCompareOp getCompareOp() {
    return compareOp;
  }

  @Override
  public String toString() {
    return String.format("%s (%s %s)",
        this.getClass().getSimpleName(), this.compareOp.name(), this.prefix);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((compareOp == null) ? 0 : compareOp.hashCode());
    result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
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
    TimelinePrefixFilter other = (TimelinePrefixFilter) obj;
    if (compareOp != other.compareOp) {
      return false;
    }
    if (prefix == null) {
      if (other.prefix != null) {
        return false;
      }
    } else if (!prefix.equals(other.prefix)){
      return false;
    }
    return true;
  }
}