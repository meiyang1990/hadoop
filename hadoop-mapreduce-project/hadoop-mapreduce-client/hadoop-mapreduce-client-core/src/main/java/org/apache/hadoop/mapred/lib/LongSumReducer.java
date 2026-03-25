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

import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;

/** 
 * 对相同key的所有Long类型值进行求和的Reducer实现，是MapReduce中常用的聚合类工具
 * @param <K> 输入输出key的类型，不做修改直接输出
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LongSumReducer<K> extends MapReduceBase
    implements Reducer<K, LongWritable, K, LongWritable> {

  /**
   * 对同一个key对应的所有LongWritable值累加求和，并输出key和最终总和
   * @param key 输入的聚合key
   * @param values 当前key对应的所有LongWritable值迭代器
   * @param output 结果输出收集器
   * @param reporter 任务进度报告器
   * @throws IOException 输出过程IO异常
   */
  public void reduce(K key, Iterator<LongWritable> values,
                     OutputCollector<K, LongWritable> output,
                     Reporter reporter)
    throws IOException {

    // 初始化累加和为0
    long sum = 0;
    // 遍历所有值累加求和
    while (values.hasNext()) {
      sum += values.next().get();
    }

    // 输出当前key的求和结果
    output.collect(key, new LongWritable(sum));
  }

}