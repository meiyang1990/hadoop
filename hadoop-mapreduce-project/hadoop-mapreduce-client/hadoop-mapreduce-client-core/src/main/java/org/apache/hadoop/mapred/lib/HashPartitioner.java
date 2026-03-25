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

package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;

/** 
 * 基于键的哈希值实现数据分区，是MapReduce默认分区器。
 * 将Map输出的键根据其哈希值均匀分配到不同Reduce任务，实现数据负载均衡。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HashPartitioner<K2, V2> implements Partitioner<K2, V2> {

  /**
   * 配置分区器，HashPartitioner无需额外配置，此处为空实现。
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {}

  /**
   * 根据键的哈希值计算该键对应当分到哪个Reduce任务。
   * @param key Map输出键
   * @param value Map输出值
   * @param numReduceTasks 总Reduce任务数
   * @return 分区编号，对应目标Reduce任务索引
   */
  public int getPartition(K2 key, V2 value,
                          int numReduceTasks) {
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }

}