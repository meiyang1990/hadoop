// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.counters;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;

/**
 * MapReduce计数器组基础抽象接口，定义计数器组的通用操作契约
 * 所有具体计数器组实现都需要遵循该接口规范，支持序列化和迭代遍历
 * 
 * @param <T> 当前组包含的计数器具体类型
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CounterGroupBase<T extends Counter>
    extends Writable, Iterable<T> {

  /**
   * 获取计数器组的内部名称
   * @return 计数器组的内部唯一标识名称
   */
  String getName();

  /**
   * 获取计数器组的展示名称
   * @return 面向用户的友好可读名称
   */
  String getDisplayName();

  /**
   * 设置计数器组的展示名称
   * @param displayName 要设置的友好展示名称
   */
  void setDisplayName(String displayName);

  /**
   * 添加一个已构造好的计数器到当前组
   * @param counter 要添加的计数器对象
   */
  void addCounter(T counter);

  /**
   * 根据参数构造并添加新计数器到当前组
   * @param name 计数器内部名称
   * @param displayName 计数器展示名称
   * @param value 计数器初始值
   * @return 构造并添加完成的计数器对象
   */
  T addCounter(String name, String displayName, long value);

  /**
   * 根据名称查找计数器，不存在则创建新计数器
   * @param counterName 计数器内部名称
   * @param displayName 计数器展示名称（创建新计数器时使用）
   * @return 找到或新增的计数器对象
   */
  T findCounter(String counterName, String displayName);

  /**
   * 根据名称查找计数器，可指定不存在时是否创建
   * @param counterName 计数器内部名称
   * @param create 不存在时是否创建新计数器
   * @return 找到或新增的计数器对象，如果create为false且未找到则返回null
   */
  T findCounter(String counterName, boolean create);

  /**
   * 根据名称查找计数器，不存在则自动创建
   * @param counterName 计数器内部名称
   * @return 找到或新增的计数器对象
   */
  T findCounter(String counterName);

  /**
   * 获取当前组包含的计数器总数
   * @return 当前组中计数器的数量
   */
  int size();

  /**
   * 将另一个计数器组的所有计数器值累加至当前组对应计数器
   * 不存在对应计数器则新增，用于计数器合并场景
   * @param rightGroup 要累加的源计数器组
   */
  void incrAllCounters(CounterGroupBase<T> rightGroup);
  
  @Private
  /**
   * 如果当前对象是包装 facade，获取底层实际的计数器组对象
   * 用于解包包装类，获取原始实现，仅框架内部使用
   * @return 当前对象包装的底层原始计数器组
   */
  CounterGroupBase<T> getUnderlyingGroup();
}