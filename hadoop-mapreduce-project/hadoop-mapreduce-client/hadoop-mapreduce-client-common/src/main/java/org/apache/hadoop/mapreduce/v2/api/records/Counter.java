// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE
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

package org.apache.hadoop.mapreduce.v2.api.records;

/**
 * 代表MapReduce任务中的一个统计计数器，用于记录作业/任务运行过程中的各类指标统计
 * 如Map输入记录数、Reduce输出字节数等自定义或内置统计
 */
public interface Counter {
  /**
   * 获取计数器的唯一名称
   * @return 计数器的标识名称
   */
  public abstract String getName();
  
  /**
   * 获取计数器用于UI展示的显示名称
   * @return 计数器的可读显示名称
   */
  public abstract String getDisplayName();
  
  /**
   * 获取计数器当前的统计值
   * @return 计数器的当前值
   */
  public abstract long getValue();
  
  /**
   * 设置计数器的唯一名称
   * @param name 计数器的标识名称
   */
  public abstract void setName(String name);
  
  /**
   * 设置计数器的UI显示名称
   * @param displayName 计数器的可读显示名称
   */
  public abstract void setDisplayName(String displayName);
  
  /**
   * 设置计数器的当前统计值
   * @param value 要设置的计数器值
   */
  public abstract void setValue(long value);
}