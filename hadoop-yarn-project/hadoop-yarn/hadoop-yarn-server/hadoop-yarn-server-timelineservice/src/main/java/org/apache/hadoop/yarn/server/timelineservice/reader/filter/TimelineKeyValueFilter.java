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
 * 时间线服务键值对过滤器，基于键值对是否相等对后端存储中的数据进行过滤。
 * 继承自TimelineCompareFilter，仅支持相等/不相等两种比较操作。
 */
@Private
@Unstable
public class TimelineKeyValueFilter extends TimelineCompareFilter {
  public TimelineKeyValueFilter() {
  }

  /**
   * 构造键值对过滤器。
   * @param op 比较操作，仅支持EQUAL或NOT_EQUAL
   * @param key 过滤键名
   * @param val 过滤值
   * @param keyMustExistFlag 键必须存在标识，为true则要求数据中必须包含该键
   */
  public TimelineKeyValueFilter(TimelineCompareOp op, String key, Object val,
      boolean keyMustExistFlag) {
    super(op, key, val, keyMustExistFlag);
    // 检查操作符合法性，仅允许相等或不等比较
    if (op != TimelineCompareOp.EQUAL && op != TimelineCompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("TimelineCompareOp for equality"
          + " filter should be EQUAL or NOT_EQUAL");
    }
  }

  public TimelineKeyValueFilter(TimelineCompareOp op, String key, Object val) {
    this(op, key, val, true);
  }

  @Override
  public TimelineFilterType getFilterType() {
    // 返回过滤器类型：键值对过滤
    return TimelineFilterType.KEY_VALUE;
  }

  /**
   * 设置比较操作符，同时检查操作符合法性。
   * @param timelineCompareOp 比较操作，仅支持EQUAL或NOT_EQUAL
   * @param keyExistFlag 键必须存在标识
   */
  public void setCompareOp(TimelineCompareOp timelineCompareOp,
      boolean keyExistFlag) {
    // 检查操作符合法性，仅允许相等或不等比较
    if (timelineCompareOp != TimelineCompareOp.EQUAL &&
        timelineCompareOp != TimelineCompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("TimelineCompareOp for equality"
          + " filter should be EQUAL or NOT_EQUAL");
    }
    super.setCompareOp(timelineCompareOp, keyExistFlag);
  }
}