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
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * 文件所属模块：Hadoop MapReduce 客户端核心模块
 * 该分区器根据 BinaryComparable 类型键的字节数组中可配置的一段字节进行分区，
 * 适合对二进制格式的键按指定分段划分到不同Reduce任务。
 * 是旧MapReduce API对新API BinaryPartitioner的适配实现。
 * 
 * Partition {@link BinaryComparable} keys using a configurable part of 
 * the bytes array returned by {@link BinaryComparable#getBytes()}. 
 * 
 * @see org.apache.hadoop.mapreduce.lib.partition.BinaryPartitioner
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BinaryPartitioner<V>  
  extends org.apache.hadoop.mapreduce.lib.partition.BinaryPartitioner<V>
  implements Partitioner<BinaryComparable, V> {

  /**
   * 配置分区器，将旧API的JobConf配置传递给父类处理
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    super.setConf(job);
  }
  
}