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

package org.apache.hadoop.mapreduce.lib.partition;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @fileoverview HashPartitioner是MapReduce框架默认的分区器，基于键的哈希值对数据进行分区，将相同哈希键的数据发送到同一个Reduce任务。
 * 本文件实现了基于哈希的分区逻辑，是MapReduce中最常用的分区实现。
 */
/** 基于键对象的hashCode实现数据分区，是MapReduce默认分区器 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HashPartitioner<K, V> extends Partitioner<K, V> {

  /**
   * 根据键的哈希值计算该数据应分配到的分区编号
   * @param key 输入数据的键
   * @param value 输入数据的值
   * @param numReduceTasks 总Reduce任务数量，即总分区数
   * @return 分区编号，范围为[0, numReduceTasks-1]
   */
  public int getPartition(K key, V value,
                          int numReduceTasks) {
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }

}