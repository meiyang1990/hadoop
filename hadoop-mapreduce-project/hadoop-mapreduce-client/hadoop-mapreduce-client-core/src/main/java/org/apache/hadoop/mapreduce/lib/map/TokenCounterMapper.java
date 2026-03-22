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

package org.apache.hadoop.mapreduce.lib.map;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 分词计数Mapper，将输入文本按分词拆分后，每个分词输出计数为1的键值对
 * 是经典词频统计示例中的标准Mapper实现
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TokenCounterMapper extends Mapper<Object, Text, Text, IntWritable>{
    
  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  
  /**
   * 对输入文本进行分词，每个分词输出一次计数1
   * @param key 输入键（未使用）
   * @param value 输入文本行
   * @param context MapReduce上下文对象
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  @Override
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
    // 对当前文本行按空白字符分词
    StringTokenizer itr = new StringTokenizer(value.toString());
    // 遍历所有分词
    while (itr.hasMoreTokens()) {
      // 设置当前分词到可复用Text对象
      word.set(itr.nextToken());
      // 输出分词+计数1的键值对，供Reducer汇总
      context.write(word, one);
    }
  }
}