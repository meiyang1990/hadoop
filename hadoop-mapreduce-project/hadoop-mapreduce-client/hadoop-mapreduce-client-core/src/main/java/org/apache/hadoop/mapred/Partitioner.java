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
 * MapReduce 分区器接口，负责对Map输出的中间键空间进行分区。
 * 
 * <p><code>Partitioner</code> 控制Map输出中间结果键的分区规则，通常基于哈希函数从键（或键的子集）计算得到分区编号。
 * 分区总数与作业的Reduce任务数量相等，因此该组件决定了每个Map输出的中间键（及对应记录）会被发送到哪个Reduce任务进行归约处理。</p>
 *
 * <p>注意：只有当作业存在多个Reduce任务时，才会创建Partitioner实例。</p>
 * 
 * @see Reducer
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Partitioner<K2, V2> extends JobConfigurable {
  
  /** 
   * 根据给定键、值和总分区数，计算得到该记录对应的分区编号。
   * 通常实现为对键（或键的子集）的哈希函数计算。
   *
   * @param key 待分区的键
   * @param value 待分区的值
   * @param numPartitions 作业的总分区数（即Reduce任务总数）
   * @return 该键对应的分区编号
   */
  int getPartition(K2 key, V2 value, int numPartitions);
}