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
 * 时间线服务过滤器比较操作枚举，定义过滤查询时使用的比较运算符。
 */
@Private
@Unstable
public enum TimelineCompareOp {
  /** 小于比较 */
  LESS_THAN,
  /** 小于等于比较 */
  LESS_OR_EQUAL,
  /** 等于比较 */
  EQUAL,
  /** 不等于比较 */
  NOT_EQUAL,
  /** 大于等于比较 */
  GREATER_OR_EQUAL,
  /** 大于比较 */
  GREATER_THAN
}