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
 * 对键的哈希值进行重哈希的分区器，通过改进哈希分布，让数据更均匀地分布到各个Reduce分区
 * 解决原始哈希分布不均匀的问题，可以优化Reduce任务执行时间，不会对现有性能造成负面影响
 * 特别适用于键分布存在规律模式的Integer、Long类型键场景，能够提升数据分发的均匀性
 *  @since 2.0.3
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RehashPartitioner<K, V> extends Partitioner<K, V> {

  /** 用于提升哈希质量的质数种子 */
  private static final int SEED = 1591267453;

  /**
   * 对键的原始哈希值进行重哈希，计算该键应该分配到的Reduce分区编号
   * @param key 分区键
   * @param value 分区值
   * @param numReduceTasks 总Reduce任务数
   * @return 分区编号，范围从0到numReduceTasks-1
   */
  public int getPartition(K key, V value, int numReduceTasks) {
    // 使用种子异或原始哈希值，打乱原始分布
    int h = SEED ^ key.hashCode();
    // 多轮异或移位，进一步打散哈希位分布
    h ^= (h >>> 20) ^ (h >>> 12);
    h = h ^ (h >>> 7) ^ (h >>> 4);

    // 去除符号位后对Reduce任务数取模，得到最终分区编号
    return (h & Integer.MAX_VALUE) % numReduceTasks;
  }
}