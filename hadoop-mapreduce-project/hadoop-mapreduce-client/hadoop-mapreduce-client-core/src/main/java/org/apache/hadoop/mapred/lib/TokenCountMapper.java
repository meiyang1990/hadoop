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
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 分词计数Mapper，将输入文本按分词切分，输出<分词, 计数1>键值对，用于词频统计类MapReduce作业
 * 使用{@link StringTokenizer}对文本行进行分词切分，是旧版MapReduce API的经典示例Mapper实现
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TokenCountMapper<K> extends MapReduceBase
    implements Mapper<K, Text, Text, LongWritable> {

  /**
   * 对输入文本行进行分词处理，输出每个分词对应的计数1
   * @param key 输入键（行偏移量，本实现未使用）
   * @param value 输入值，文本行内容
   * @param output 输出收集器，用于输出<分词, 计数>键值对
   * @param reporter 报告器，用于上报进度和计数器
   * @throws IOException 输出过程IO异常
   */
  public void map(K key, Text value,
                  OutputCollector<Text, LongWritable> output,
                  Reporter reporter)
    throws IOException {
    // 将输入Text转换为字符串
    String text = value.toString();       // value is line of text

    // 对文本行进行分词切分
    StringTokenizer st = new StringTokenizer(text);
    while (st.hasMoreTokens()) {
      // 输出当前分词，计数记为1，供Reduce阶段汇总总频次
      output.collect(new Text(st.nextToken()), new LongWritable(1));
    }  
  }
  
}