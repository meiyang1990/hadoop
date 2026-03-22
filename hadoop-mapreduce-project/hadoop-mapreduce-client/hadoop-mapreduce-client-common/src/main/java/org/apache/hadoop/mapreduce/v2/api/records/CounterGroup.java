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

package org.apache.hadoop.mapreduce.v2.api.records;

import java.util.Map;

/**
 * 计数器分组接口，定义了MapReduce任务中同类计数器的分组聚合能力
 * 用于将同一类业务/系统指标的计数器归为同一分组，方便统计结果分类展示和管理
 */
public interface CounterGroup {
  /**
   * 获取计数器分组的唯一名称
   * @return 计数器分组名称
   */
  public abstract String getName();
  /**
   * 获取计数器分组的展示名称，用于UI展示等场景
   * @return 计数器分组展示名称
   */
  public abstract String getDisplayName();
  
  /**
   * 获取该分组下所有计数器的集合
   * @return 以计数器名称为键、计数器对象为值的Map
   */
  public abstract Map<String, Counter> getAllCounters();
  /**
   * 根据计数器名称获取对应计数器对象
   * @param key 计数器名称
   * @return 对应的计数器对象
   */
  public abstract Counter getCounter(String key);
  
  /**
   * 设置计数器分组的唯一名称
   * @param name 分组名称
   */
  public abstract void setName(String name);
  /**
   * 设置计数器分组的展示名称
   * @param displayName 分组展示名称
   */
  public abstract void setDisplayName(String displayName);
  
  /**
   * 将多个计数器批量添加到当前分组
   * @param counters 待添加的计数器集合
   */
  public abstract void addAllCounters(Map<String, Counter> counters);
  /**
   * 设置指定名称的计数器，覆盖已有值
   * @param key 计数器名称
   * @param value 计数器对象
   */
  public abstract void setCounter(String key, Counter value);
  /**
   * 移除指定名称的计数器
   * @param key 待移除计数器的名称
   */
  public abstract void removeCounter(String key);
  /**
   * 清空该分组下所有计数器
   */
  public abstract void clearCounters();
}