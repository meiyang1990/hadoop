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
import org.apache.hadoop.util.Progressable;

/**
 * MapReduce任务尝试运行上下文接口，定义任务尝试运行过程中需要使用的核心上下文能力
 * 为单个任务尝试实例提供配置访问、进度报告、状态更新、计数器统计等功能
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TaskAttemptContext extends JobContext, Progressable {

  /**
   * 获取当前任务尝试的唯一ID标识
   * @return 当前任务尝试的唯一ID对象
   */
  public TaskAttemptID getTaskAttemptID();

  /**
   * 设置当前任务的状态描述信息，用于对外展示任务当前运行状态
   * @param msg 任务状态描述字符串
   */
  public void setStatus(String msg);

  /**
   * 获取最近一次设置的任务状态描述信息
   * @return 当前任务的状态描述字符串
   */
  public String getStatus();
  
  /**
   * 获取当前任务尝试的运行进度
   * @return 进度值，范围为0.0到1.0（闭区间），0表示未开始，1表示完成
   */
  public abstract float getProgress();

  /**
   * 根据枚举类型获取对应的计数器，用于任务执行过程中的指标统计
   * @param counterName 计数器枚举名称
   * @return 对应枚举名称的计数器对象
   */
  public Counter getCounter(Enum<?> counterName);

  /**
   * 根据分组名和计数器名获取对应的计数器，用于任务执行过程中的自定义指标统计
   * @param groupName 计数器分组名称
   * @param counterName 计数器名称
   * @return 对应分组和名称的计数器对象
   */
  public Counter getCounter(String groupName, String counterName);

}