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
package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 作业优先级枚举，定义MapReduce作业调度优先级
 * <p>
 * 核心职责：为YARN资源调度提供作业优先级分级标识，用于调度器分配资源时的优先级排序
 * <ul>
 *   <li>DEFAULT：用户提交作业未指定优先级时使用，让YARN根据自身配置选择默认优先级</li>
 *   <li>UNDEFINED_PRIORITY：处理YARN支持的整数优先级中不属于预定义五级的情况，用于兼容自定义优先级</li>
 * </ul>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum JobPriority {
  /** 最高优先级 */
  VERY_HIGH,
  /** 高优先级 */
  HIGH,
  /** 普通优先级 */
  NORMAL,
  /** 低优先级 */
  LOW,
  /** 最低优先级 */
  VERY_LOW,
  /** 默认优先级，用户未指定时使用 */
  DEFAULT,
  /** 未定义优先级，兼容YARN自定义整数优先级 */
  UNDEFINED_PRIORITY;
}