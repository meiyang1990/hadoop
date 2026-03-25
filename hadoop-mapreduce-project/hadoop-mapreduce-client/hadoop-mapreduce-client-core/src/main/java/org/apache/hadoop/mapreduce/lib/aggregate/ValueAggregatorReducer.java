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

package org.apache.hadoop.mapreduce.lib.aggregate;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 文件：ValueAggregatorReducer.java
 * 所属模块：hadoop-mapreduce-client-core
 * 核心职责：实现了Hadoop聚合框架的通用Reducer逻辑，根据键中携带的聚合类型，
 *          对Map阶段输出的相同分组值执行对应聚合计算，并输出最终聚合结果
 * 设计目的：支持数据驱动的动态聚合计算，允许通过键前缀指定聚合类型，无需修改Reducer代码即可支持不同聚合逻辑
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorReducer<K1 extends WritableComparable<?>,
                                    V1 extends Writable>
  extends Reducer<Text, Text, Text, Text> {

  /**
   * Reducer初始化方法，执行聚合框架的全局初始化工作
   * @param context 任务上下文对象
   * @throws IOException  IO异常
   * @throws InterruptedException 中断异常
   */
  public void setup(Context context) 
      throws IOException, InterruptedException {
    ValueAggregatorJobBase.setup(context.getConfiguration());
  }

  /**
   * 核心reduce方法，对同一分组的输入值执行指定的聚合计算，并输出结果
   * @param key 输入键，键前缀标识聚合类型，后缀为实际分组键
   * @param values 当前分组的所有输入值集合
   * @param context 任务上下文对象，用于输出结果
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void reduce(Text key, Iterable<Text> values,
      Context context) throws IOException, InterruptedException {
    // 将输入键转换为字符串
    String keyStr = key.toString();
    // 查找聚合类型分隔符位置
    int pos = keyStr.indexOf(ValueAggregatorDescriptor.TYPE_SEPARATOR);
    // 提取聚合类型
    String type = keyStr.substring(0, pos);
    // 提取实际分组键，去掉聚合类型前缀
    keyStr = keyStr.substring(pos + 
               ValueAggregatorDescriptor.TYPE_SEPARATOR.length());
    // 从配置中获取去重聚合允许的最大唯一值数量，默认无限制
    long uniqCount = context.getConfiguration().
      getLong(UniqValueCount.MAX_NUM_UNIQUE_VALUES, Long.MAX_VALUE);
    // 根据聚合类型生成对应的聚合器实例
    ValueAggregator aggregator = ValueAggregatorBaseDescriptor
      .generateValueAggregator(type, uniqCount);
    // 将当前分组所有值添加到聚合器中
    for (Text value : values) {
      aggregator.addNextValue(value);
    }

    // 获取聚合计算结果报表
    String val = aggregator.getReport();
    // 构造输出键（实际分组键）
    key = new Text(keyStr);
    // 写出分组键和对应聚合结果
    context.write(key, new Text(val));
  }
}