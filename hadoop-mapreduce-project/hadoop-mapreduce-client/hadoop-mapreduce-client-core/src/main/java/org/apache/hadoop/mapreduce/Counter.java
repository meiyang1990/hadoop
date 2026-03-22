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
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.Writable;

/**
 * MapReduce作业进度统计计数器接口，用于跟踪MapReduce作业执行过程中的各类指标统计。
 * 
 * <p>计数器可以由MapReduce框架或用户应用定义，每个计数器拥有唯一名称，维护一个长整型统计值。
 * 计数器按分组进行组织，同一分组通常来自同一个枚举类，方便统一管理同一类指标。</p>
 * 
 * <p>常用于统计作业处理的记录数、字节数、异常次数等各类运行指标，用于作业监控和调优分析。</p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Counter extends Writable {

  /**
   * 设置计数器的展示名称
   * @param displayName 计数器展示名称
   * @deprecated 已废弃，默认无操作
   */
  @Deprecated
  void setDisplayName(String displayName);

  /**
   * 获取计数器的名称
   * @return 计数器名称
   */
  String getName();

  /**
   * 获取计数器的展示名称，用于前端UI展示
   * @return 面向用户展示的计数器名称
   */
  String getDisplayName();

  /**
   * 获取计数器当前的统计值
   * @return 计数器当前值
   */
  long getValue();

  /**
   * 直接设置计数器的统计值
   * @param value 要设置的值
   */
  void setValue(long value);

  /**
   * 将计数器的值增加指定增量
   * @param incr 增量值，可正可负
   */
  void increment(long incr);
 
  @Private
  /**
   * 如果当前是外观包装对象，获取其底层包装的原始计数器对象
   * @return 底层原始计数器对象
   */
  Counter getUnderlyingCounter();
}