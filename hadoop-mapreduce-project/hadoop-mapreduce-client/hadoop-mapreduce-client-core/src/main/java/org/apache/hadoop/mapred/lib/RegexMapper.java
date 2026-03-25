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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 基于正则表达式提取匹配文本的旧MapReduce API Mapper实现
 * 从输入文本中提取所有匹配正则表达式的内容，以提取结果为键，计数1为值输出
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RegexMapper<K> extends MapReduceBase
    implements Mapper<K, Text, Text, LongWritable> {

  // 编译后的正则表达式模式
  private Pattern pattern;
  // 需要提取的正则匹配分组编号
  private int group;

  /**
   * 从作业配置中初始化正则表达式和分组参数
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    // 从配置中获取正则表达式字符串并编译
    pattern = Pattern.compile(job.get(org.apache.hadoop.mapreduce.lib.map.
                RegexMapper.PATTERN));
    // 从配置中获取分组编号，默认取第0组（整个匹配结果）
    group = job.getInt(org.apache.hadoop.mapreduce.lib.map.
              RegexMapper.GROUP, 0);
  }

  /**
   * 对输入文本执行正则匹配，提取所有匹配结果并输出计数
   * @param key 输入键（未使用）
   * @param value 输入文本值
   * @param output 输出收集器
   * @param reporter 作业报告器
   * @throws IOException IO异常
   */
  public void map(K key, Text value,
                  OutputCollector<Text, LongWritable> output,
                  Reporter reporter)
    throws IOException {
    // 将输入Text转换为字符串
    String text = value.toString();
    // 创建匹配器
    Matcher matcher = pattern.matcher(text);
    // 遍历所有匹配结果
    while (matcher.find()) {
      // 输出匹配到的分组内容作为键，计数1作为值
      output.collect(new Text(matcher.group(group)), new LongWritable(1));
    }
  }

}