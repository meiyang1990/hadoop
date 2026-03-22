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
import org.apache.hadoop.mapreduce.counters.CounterGroupBase;

/**
 * 文件概述：MapReduce作业计数器分组接口，定义了一组逻辑相关计数器的集合抽象
 * 
 * 计数器分组接口，将一组逻辑相关的计数器聚合在一起。通常，一个分组对应一个
 * {@link Enum}枚举子类，分组中的计数器对应枚举的各个取值。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface CounterGroup extends CounterGroupBase<Counter> {
  // 本质上是类型别名，简化用户使用，避免编写泛型语法
}