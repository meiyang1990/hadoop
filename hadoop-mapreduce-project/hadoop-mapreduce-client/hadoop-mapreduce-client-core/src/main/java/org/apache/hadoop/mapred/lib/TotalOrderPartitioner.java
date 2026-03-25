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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * 文件级注释：全排序分区器，为旧版MapReduce API提供全排序分区功能，通过读取外部生成的划分点实现全局有序输出。
 * 该类是新版mapreduce包下TotalOrderPartitioner的旧API兼容包装类。
 * 
 * 分区器，通过从外部生成的源读取划分点，实现输出数据的全局有序分区。
 * 用于将Map输出按key范围均匀分区到不同Reduce，保证最终输出整体有序。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TotalOrderPartitioner<K ,V>
    extends org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner<K, V>
    implements Partitioner<K,V> {

  /**
   * 构造函数：创建空的全排序分区器实例。
   */
  public TotalOrderPartitioner() { }

  /**
   * 配置分区器，从作业配置中加载划分点信息。
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    super.setConf(job);
  }

  /**
   * 设置存储有序分区键集的SequenceFile路径。
   * 对于R个Reduce任务，该文件中必须包含R-1个划分键。
   * @param job 作业配置
   * @param p 分区文件路径
   * @deprecated 请使用 {@link #setPartitionFile(Configuration, Path)} 替代
   */
  @Deprecated
  public static void setPartitionFile(JobConf job, Path p) {
    org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner.
            setPartitionFile(job, p);
  }

  /**
   * 获取存储有序分区键集的SequenceFile路径。
   * @param job 作业配置
   * @return 分区文件路径字符串
   * @deprecated 请使用 {@link #getPartitionFile(Configuration)} 替代
   */
  @Deprecated
  public static String getPartitionFile(JobConf job) {
    return org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner.
            getPartitionFile(job);
  }
}