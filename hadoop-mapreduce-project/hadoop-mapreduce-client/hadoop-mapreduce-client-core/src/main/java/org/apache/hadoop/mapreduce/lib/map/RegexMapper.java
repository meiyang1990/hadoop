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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 正则表达式匹配Mapper，从输入文本中提取匹配正则表达式的内容并输出计数
 * 常用于词频统计、文本特征提取等场景，输出为匹配到的文本+计数1
 */
/** A {@link Mapper} that extracts text matching a regular expression. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RegexMapper<K> extends Mapper<K, Text, Text, LongWritable> {

  /** 正则表达式配置项key */
  public static String PATTERN = "mapreduce.mapper.regex";
  /** 正则捕获分组配置项key */
  public static String GROUP = "mapreduce.mapper.regexmapper..group";
  private Pattern pattern;
  private int group;

  /**
   * 初始化正则匹配器，从作业配置中加载正则表达式和捕获分组
   * @param context Mapper运行上下文
   */
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    pattern = Pattern.compile(conf.get(PATTERN));
    group = conf.getInt(GROUP, 0);
  }

  /**
   * 对输入文本行进行正则匹配，输出所有匹配结果，每个匹配结果输出一次计数1
   * @param key 输入键
   * @param value 输入文本行
   * @param context Mapper运行上下文
   * @throws IOException
   * @throws InterruptedException
   */
  public void map(K key, Text value,
                  Context context)
    throws IOException, InterruptedException {
    String text = value.toString();
    Matcher matcher = pattern.matcher(text);
    while (matcher.find()) {
      context.write(new Text(matcher.group(group)), new LongWritable(1));
    }
  }
}