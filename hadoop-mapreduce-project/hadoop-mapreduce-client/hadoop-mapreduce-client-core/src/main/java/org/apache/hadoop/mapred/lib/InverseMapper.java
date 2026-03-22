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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/** 
 * 倒置键值对的Mapper实现，将输入的键和值交换后输出
 * 常用于需要反转键值关系的MapReduce处理场景，例如倒排索引构建
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InverseMapper<K, V>
    extends MapReduceBase implements Mapper<K, V, V, K> {

  /**
   * 倒置键值对映射函数，交换输入的键和值后输出
   * @param key 输入键
   * @param value 输入值
   * @param output 输出收集器
   * @param reporter 任务报告器
   * @throws IOException 输出异常
   */
  public void map(K key, V value,
                  OutputCollector<V, K> output, Reporter reporter)
    throws IOException {
    output.collect(value, key);
  }
  
}