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

import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;

/** 
 * 恒等Reducer，不执行任何归约操作，直接将所有输入键值对输出。
 * 适用于不需要聚合操作的MapReduce作业，直接将Map输出原样输出。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class IdentityReducer<K, V>
    extends MapReduceBase implements Reducer<K, V, K, V> {

  /** 
   * 直接将所有输入的键与值对输出到结果收集器，不做任何归约处理。
   * @param key 归约输入键
   * @param values 当前键对应的所有值迭代器
   * @param output 结果输出收集器
   * @param reporter 作业进度报告器
   * @throws IOException 输出异常时抛出
   */
  public void reduce(K key, Iterator<V> values,
                     OutputCollector<K, V> output, Reporter reporter)
    throws IOException {
    // 遍历所有值，逐个输出原键值对
    while (values.hasNext()) {
      output.collect(key, values.next());
    }
  }
	
}