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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;

/** 
 * 实现恒等映射函数，将输入的键值对不做修改直接输出。
 * 常用于MapReduce中不需要Map阶段做处理、只需要Shuffle排序的场景，例如全量排序、仅Reduce阶段计算的作业。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class IdentityMapper<K, V>
    extends MapReduceBase implements Mapper<K, V, K, V> {

  /** 
   * 恒等映射方法，将输入的键值对直接输出，不做任何转换处理。
   * @param key 输入键
   * @param val 输入值
   * @param output 输出收集器
   * @param reporter 作业进度报告器
   * @throws IOException 输出收集时可能抛出IO异常
   */
  public void map(K key, V val,
                  OutputCollector<K, V> output, Reporter reporter)
    throws IOException {
    output.collect(key, val);
  }
}