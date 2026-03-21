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
 * 时间线服务比较过滤器，基于键值对和比较运算符对实体进行过滤。
 * 根据指定的比较关系（等于、不等于、大于等）筛选符合条件的时间线实体。
 */
@Private
@Unstable
public class TimelineCompareFilter extends TimelineFilter {

  /** 比较操作符 */
  private TimelineCompareOp compareOp;
  /** 待比较的键名称 */
  private String key;
  /** 待比较的目标值 */
  private Object value;
  /** 当比较操作是不等于时，该标记决定键不存在时是否过滤掉实体。true表示键必须存在才会被保留，false表示键不存在也会被保留 */
  private boolean keyMustExist = true;

  public TimelineCompareFilter() {
  }

  /**
   * 构造带完整参数的比较过滤器。
   * @param op 比较操作符
   * @param key 待比较的键
   * @param val 待比较的目标值
   * @param keyMustExistFlag 不等于比较时，键是否必须存在
   */
  public TimelineCompareFilter(TimelineCompareOp op, String key, Object val,
       boolean keyMustExistFlag) {
    this.compareOp = op;
    this.key = key;
    this.value = val;
    // 仅当不等于操作时才使用传入的标记，其他操作强制要求键必须存在
    if (op == TimelineCompareOp.NOT_EQUAL) {
      this.keyMustExist = keyMustExistFlag;
    } else {
      this.keyMustExist = true;
    }
  }

  /**
   * 构造比较过滤器，默认不等于比较时要求键必须存在。
   * @param op 比较操作符
   * @param key 待比较的键
   * @param val 待比较的目标值
   */
  public TimelineCompareFilter(TimelineCompareOp op, String key, Object val) {
    this(op, key, val, true);
  }

  @Override
  public TimelineFilterType getFilterType() {
    return TimelineFilterType.COMPARE;
  }

  public TimelineCompareOp getCompareOp() {
    return compareOp;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String keyToBeSet) {
    key = keyToBeSet;
  }

  public Object getValue() {
    return value;
  }

  /**
   * 设置比较操作符和键存在标记。
   * @param timelineCompareOp 比较操作符
   * @param keyExistFlag 不等于比较时键是否必须存在
   */
  public void setCompareOp(TimelineCompareOp timelineCompareOp,
      boolean keyExistFlag) {
    this.compareOp = timelineCompareOp;
    // 仅当操作符为不等于时更新键存在标记
    if (timelineCompareOp == TimelineCompareOp.NOT_EQUAL) {
      this.keyMustExist = keyExistFlag;
    }
  }

  public void setValue(Object val) {
    value = val;
  }

  public boolean getKeyMustExist() {
    return keyMustExist;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((compareOp == null) ? 0 : compareOp.hashCode());
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + (keyMustExist ? 1231 : 1237);
    result = prime * result + ((value == null) ? 0 : value.hashCode());
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
    TimelineCompareFilter other = (TimelineCompareFilter) obj;
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
    if (keyMustExist != other.keyMustExist) {
      return false;
    }
    if (value == null) {
      if (other.value != null) {
        return false;
      }
    } else if (!value.equals(other.value)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s:%s:%b)",
        this.getClass().getSimpleName(), this.compareOp.name(),
        this.key, this.value, this.keyMustExist);
  }
}