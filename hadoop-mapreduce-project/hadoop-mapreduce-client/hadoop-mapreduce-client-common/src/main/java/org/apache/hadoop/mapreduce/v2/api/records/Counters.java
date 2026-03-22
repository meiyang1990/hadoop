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
 * MapReduce任务计数器集合接口，统一管理任务运行过程中产生的各类统计指标
 * 负责按分组组织所有计数器，支持查询、修改和增量更新统计值
 */
public interface Counters {
  /**
   * 获取所有计数器分组
   * @return 以分组名称为key，计数器分组对象为value的映射表
   */
  public abstract Map<String, CounterGroup> getAllCounterGroups();
  /**
   * 根据分组名称获取指定计数器分组
   * @param key 计数器分组名称
   * @return 对应的计数器分组对象，不存在则返回null
   */
  public abstract CounterGroup getCounterGroup(String key);
  /**
   * 根据枚举类型获取对应计数器（用于按枚举定义的内置计数器场景）
   * @param key 计数器对应的枚举实例
   * @return 对应的计数器对象
   */
  public abstract Counter getCounter(Enum<?> key);
  
  /**
   * 将传入的所有计数器分组添加到当前集合中
   * @param counterGroups 待添加的计数器分组映射表
   */
  public abstract void addAllCounterGroups(Map<String, CounterGroup> counterGroups);
  /**
   * 设置指定名称的计数器分组，覆盖已存在的分组
   * @param key 计数器分组名称
   * @param value 计数器分组对象
   */
  public abstract void setCounterGroup(String key, CounterGroup value);
  /**
   * 移除指定名称的计数器分组
   * @param key 待移除分组的名称
   */
  public abstract void removeCounterGroup(String key);
  /**
   * 清空当前集合中所有计数器分组
   */
  public abstract void clearCounterGroups();
  
  /**
   * 对指定枚举类型计数器执行增量更新
   * @param key 目标计数器对应的枚举实例
   * @param amount 增量值，可正可负
   */
  public abstract void incrCounter(Enum<?> key, long amount);
}