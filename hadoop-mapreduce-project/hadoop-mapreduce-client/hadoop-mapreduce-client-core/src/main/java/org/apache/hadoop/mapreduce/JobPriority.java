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
package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * MapReduce作业优先级枚举定义，用于标识YARN集群中运行作业的调度优先级
 * <p>
 * 特殊优先级说明：
 * <ul>
 * <li>DEFAULT：提交作业时用户未指定优先级，YARN将根据自身配置选择默认优先级</li>
 * <li>UNDEFINED_PRIORITY：YARN支持整数类型优先级，除预定义的5个标准优先级外，
 * 其他整数值优先级统一用该枚举标识</li>
 * </ul>
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum JobPriority {
  /** 最高优先级 */
  VERY_HIGH,
  /** 高优先级 */
  HIGH,
  /** 正常优先级 */
  NORMAL,
  /** 低优先级 */
  LOW,
  /** 最低优先级 */
  VERY_LOW,
  /** 默认优先级：用户未指定优先级时使用 */
  DEFAULT,
  /** 未定义优先级：用于标识YARN中预定义范围外的整数值优先级 */
  UNDEFINED_PRIORITY;
}