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
import org.apache.hadoop.conf.Configurable;

/** 
 * MapReduce分区器抽象基类，负责对Map阶段输出的中间结果键空间进行分区。
 * 
 * <p>分区器控制Map输出中间键的分区规则，通常基于键（或键的子集）通过哈希函数计算分区编号。
 * 分区总数与作业的Reduce任务数量相等，因此分区器决定了每个中间键（对应记录）会被发送到哪一个Reduce任务进行归约处理。</p>
 *
 * <p>注意：只有当作业存在多个Reduce任务时，才会创建分区器实例。</p>
 *
 * <p>注意：如果需要让分区器类获取作业的配置对象，需要实现{@link Configurable}接口。</p>
 * 
 * @see Reducer
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class Partitioner<KEY, VALUE> {
  
  /** 
   * 根据给定的键、值和分区总数（即作业Reduce任务总数）计算得到该记录对应的分区编号。
   * 通常基于键的全部或子集计算哈希值得到分区。
   *   
   * @param key  需要分区的键
   * @param value 对应条目的值
   * @param numPartitions 分区总数，等于作业Reduce任务数量
   * @return 该键对应的分区编号
   */
  public abstract int getPartition(KEY key, VALUE value, int numPartitions);
  
}