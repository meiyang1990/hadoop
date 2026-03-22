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

package org.apache.hadoop.mapred.pipes;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Pipes框架的分区器实现，支持C++端手动指定分区，或回退到用户配置的Java分区器
 * 用于Hadoop Pipes（允许C++编写MapReduce任务）场景，适配混合Java/C++的分区需求
 */
class PipesPartitioner<K extends WritableComparable,
                       V extends Writable>
  implements Partitioner<K, V> {
  
  // 线程本地缓存，存储当前记录手动指定的分区编号
  private static final ThreadLocal<Integer> CACHE = new ThreadLocal<Integer>();
  // 用户配置的备用Java分区器实例
  private Partitioner<K, V> part = null;
  
  /**
   * 配置分区器，从作业配置中加载用户指定的Java分区器并实例化
   * @param conf 作业配置对象
   */
  @SuppressWarnings("unchecked")
  public void configure(JobConf conf) {
    // 通过反射创建用户指定的Java分区器实例
    part =
      ReflectionUtils.newInstance(Submitter.getJavaPartitioner(conf), conf);
  }

  /**
   * 设置当前线程下一条记录的手动分区编号，供C++端设置分区使用
   * @param newValue 下一条记录要使用的分区编号
   */
  static void setNextPartition(int newValue) {
    CACHE.set(newValue);
  }

  /**
   * 计算当前键值对对应的分区编号，优先使用手动指定的分区，否则回退到用户Java分区器
   * @param key 分区键
   * @param value 分区值
   * @param numPartitions Reduce分区总数
   * @return 最终分区编号
   */
  public int getPartition(K key, V value, 
                          int numPartitions) {
    // 获取当前线程缓存的手动分区编号
    Integer result = CACHE.get();
    // 没有手动指定分区时，调用用户Java分区器计算
    if (result == null) {
      return part.getPartition(key, value, numPartitions);
    } else {
      // 使用手动指定的分区，并清空缓存
      return result;
    }
  }

}