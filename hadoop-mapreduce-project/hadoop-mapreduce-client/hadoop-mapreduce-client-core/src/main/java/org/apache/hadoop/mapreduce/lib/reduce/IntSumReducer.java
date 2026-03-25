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

package org.apache.hadoop.mapreduce.lib.reduce;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 通用整数求和Reducer实现，对同一key对应的所有整数值进行累加求和
 * 适用于词频统计等需要对同一分组下整数求和的经典MapReduce场景
 * @param <Key> 输入输出key的类型，该类不修改key，直接输出原始key
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class IntSumReducer<Key> extends Reducer<Key,IntWritable,
                                                Key,IntWritable> {
  // 存储累加结果的可写对象
  private IntWritable result = new IntWritable();

  /**
   * 对同一key下的所有IntWritable值进行累加，并输出结果
   * @param key 输入分组的key
   * @param values 该key对应的所有IntWritable值迭代器
   * @param context MapReduce上下文对象，用于输出结果
   * @throws IOException 输出时IO异常
   * @throws InterruptedException 中断异常
   */
  public void reduce(Key key, Iterable<IntWritable> values, 
                     Context context) throws IOException, InterruptedException {
    // 初始化累加和
    int sum = 0;
    // 遍历所有值并累加
    for (IntWritable val : values) {
      sum += val.get();
    }
    // 将累加结果设置到可写对象中
    result.set(sum);
    // 输出key和对应的累加和
    context.write(key, result);
  }

}