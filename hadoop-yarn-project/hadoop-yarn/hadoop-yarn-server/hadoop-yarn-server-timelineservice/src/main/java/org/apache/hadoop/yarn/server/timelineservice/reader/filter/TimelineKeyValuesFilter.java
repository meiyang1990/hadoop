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

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * 时间线服务多值键过滤器，用于根据单个键的多个值进行过滤，支持相等或不等匹配，过滤后端存储中的实体数据。
 */
@Private
@Unstable
public class TimelineKeyValuesFilter extends TimelineFilter {
  private TimelineCompareOp compareOp;
  private String key;
  private Set<Object> values;

  public TimelineKeyValuesFilter() {
  }

  /**
   * 构造多值键过滤器，校验比较操作类型必须为EQUAL或NOT_EQUAL。
   * @param op 比较操作符
   * @param key 过滤键名
   * @param values 过滤值集合
   */
  public TimelineKeyValuesFilter(TimelineCompareOp op, String key,
      Set<Object> values) {
    if (op != TimelineCompareOp.EQUAL && op != TimelineCompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("TimelineCompareOp for multi value "
          + "equality filter should be EQUAL or NOT_EQUAL");
    }
    this.compareOp = op;
    this.key = key;
    this.values = values;
  }

  @Override
  public TimelineFilterType getFilterType() {
    return TimelineFilterType.KEY_VALUES;
  }

  public String getKey() {
    return key;
  }

  public Set<Object> getValues() {
    return values;
  }

  public void setKeyAndValues(String keyForValues, Set<Object> vals) {
    key = keyForValues;
    values = vals;
  }

  public void setCompareOp(TimelineCompareOp op) {
    compareOp = op;
  }

  public TimelineCompareOp getCompareOp() {
    return compareOp;
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s:%s)",
        this.getClass().getSimpleName(), this.compareOp.name(),
        this.key, (values == null) ? "" : values.toString());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((compareOp == null) ? 0 : compareOp.hashCode());
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((values == null) ? 0 : values.hashCode());
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
    TimelineKeyValuesFilter other = (TimelineKeyValuesFilter) obj;
    if (compareOp != other.compareOp) {
      return false;
    }
    if (key == null) {
      if (other.key != null) {
        return false;
      }
    } else if (!key.equals(other.key)) {
      return false;
    }
    if (values == null) {
      if (other.values != null) {
        return false;
      }
    } else if (!values.equals(other.values)) {
      return false;
    }
    return true;
  }
}