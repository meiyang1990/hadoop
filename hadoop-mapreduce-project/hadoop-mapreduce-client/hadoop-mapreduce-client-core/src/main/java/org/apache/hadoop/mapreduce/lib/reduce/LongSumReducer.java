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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 长整型值求和Reducer，对相同Key对应的所有LongWritable值累加求和后输出
 * 是MapReduce中常用的基础归约组件，适用于计数、求和等经典聚合场景
 * @param <KEY> 输入输出Key类型，不做修改直接输出
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LongSumReducer<KEY> extends Reducer<KEY, LongWritable,
                                                 KEY,LongWritable> {

  private LongWritable result = new LongWritable();

  /**
   * 对相同Key的所有长整型值累加求和，输出结果
   * @param key 输入键，直接输出不修改
   * @param values 对应Key的所有长整型可迭代集合
   * @param context MapReduce任务上下文对象
   * @throws IOException 输出写入失败时抛出
   * @throws InterruptedException 任务被中断时抛出
   */
  public void reduce(KEY key, Iterable<LongWritable> values,
                     Context context) throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }

}