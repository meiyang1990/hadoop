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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**   
 * 基于键的指定字段对Map输出键进行分区的分区器，兼容旧版MapReduce API
 * 分区规则通过 -k pos1[,pos2] 格式定义，可精确指定用于分区的键字段范围：
 * pos格式为 f[.c][opts]，其中f是字段编号，c是字段内起始字符位置
 * 字段和字符位置从1开始计数；pos2中0表示对应字段的最后一个字符
 * 若pos1省略'.c'则默认从字段第一个字符开始；若pos2省略'.c'则默认到字段最后一个字符结束
 * 另可参考 {@link KeyFieldBasedComparator} 实现对应字段排序功能
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyFieldBasedPartitioner<K2, V2> extends 
  org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner<K2, V2> 
  implements Partitioner<K2, V2> {

  /**
   * 配置分区器，从作业配置中加载分区规则
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    super.setConf(job);
  }
}