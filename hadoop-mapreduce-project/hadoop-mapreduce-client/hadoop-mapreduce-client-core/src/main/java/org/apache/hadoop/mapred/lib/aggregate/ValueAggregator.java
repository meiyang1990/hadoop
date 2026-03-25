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

package org.apache.hadoop.mapred.lib.aggregate;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件说明：MapReduce旧版API中值聚合器接口，定义了聚合计算的基础协议
 * 核心作用：为MapReduce聚合框架提供统一的聚合器接口规范，用于对相同key的多个value进行聚合计算
 * 对应新版mapreduce API中的同名接口，保持旧版API兼容性
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ValueAggregator<E> extends 
    org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregator<E> {
}