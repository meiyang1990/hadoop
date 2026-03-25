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
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 文件：ValueAggregatorCombiner.java
 * 所属模块：MapReduce 核心库 -> 聚合计算模块
 * 功能：实现 ValueAggregator 聚合框架的通用 Combiner 组件
 * 作用：在 Map 节点本地提前聚合相同 key 的值，减少 Shuffle 阶段的数据传输量，提升聚合计算整体性能
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorCombiner<K1 extends WritableComparable<?>,
                                     V1 extends Writable>
  extends Reducer<Text, Text, Text, Text> {

  /**
   * 对 Map 输出的相同 key 的所有值进行本地预聚合
   * @param key 输入 key，格式为 [聚合类型]#[具体 key]，前缀标识需要使用的聚合类型
   * @param values Map 输出的当前 key 对应的所有值
   * @param context MR 上下文对象，用于输出聚合结果
   * @throws IOException
   * @throws InterruptedException
   */
  public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    // 将 key 转为字符串进行解析
    String keyStr = key.toString();
    // 定位聚合类型与具体业务 key 的分隔符位置
    int pos = keyStr.indexOf(ValueAggregatorDescriptor.TYPE_SEPARATOR);
    // 提取聚合类型
    String type = keyStr.substring(0, pos);
    // 从配置中获取去重聚合允许的最大唯一值数量，默认无限制
    long uniqCount = context.getConfiguration().
      getLong(UniqValueCount.MAX_NUM_UNIQUE_VALUES, Long.MAX_VALUE);
    // 根据聚合类型生成对应聚合器实例
    ValueAggregator aggregator = ValueAggregatorBaseDescriptor
      .generateValueAggregator(type, uniqCount);
    // 将所有输入值添加到聚合器中
    for (Text val : values) {
      aggregator.addNextValue(val);
    }
    // 获取聚合后的结果列表
    Iterator<?> outputs = aggregator.getCombinerOutput().iterator();

    // 遍历所有聚合结果，输出到上下文
    while (outputs.hasNext()) {
      Object v = outputs.next();
      if (v instanceof Text) {
        context.write(key, (Text)v);
      } else {
        context.write(key, new Text(v.toString()));
      }
    }
  }
}