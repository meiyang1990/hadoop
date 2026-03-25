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

package org.apache.hadoop.mapred.lib.aggregate;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 文件所属模块: hadoop-mapreduce-client-core
 * 核心职责: 实现了Aggregate框架中通用的归约逻辑，对Map阶段输出的聚合结果做最终聚合计算
 * 主要功能: 为基于数据驱动的聚合计算提供归约阶段实现，根据键中携带的聚合类型生成对应聚合器完成计算
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
/**
 * ValueAggregator框架的通用Reducer实现，负责对Map输出结果按类型完成最终聚合
 * 核心职责: 根据输入键携带的聚合类型信息，生成对应聚合器，对所有Map输出值做聚合计算并输出结果
 * 支持数据驱动的聚合计算，不同聚合类型可复用同一个Reducer逻辑
 */
public class ValueAggregatorReducer<K1 extends WritableComparable,
                                    V1 extends Writable>
  extends ValueAggregatorJobBase<K1, V1> {

  /**
   * 归约方法，根据聚合类型对相同分组的所有值做聚合计算，并输出最终聚合结果
   * @param key 输入键，格式为[聚合类型]#[聚合分组键]，前缀表示聚合计算类型
   * @param values 同一分组下所有Map阶段输出的待聚合值
   * @param output 输出收集器，用于输出最终聚合结果
   * @param reporter 任务进度报告器
   * @throws IOException 输出异常时抛出
   */
  public void reduce(Text key, Iterator<Text> values,
                     OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    // 将输入键转为字符串
    String keyStr = key.toString();
    // 找到聚合类型与分组键的分隔符位置
    int pos = keyStr.indexOf(ValueAggregatorDescriptor.TYPE_SEPARATOR);
    // 提取聚合类型
    String type = keyStr.substring(0, pos);
    // 提取实际的业务分组键
    keyStr = keyStr.substring(pos
                              + ValueAggregatorDescriptor.TYPE_SEPARATOR.length());

    // 根据聚合类型生成对应的聚合器实例
    ValueAggregator aggregator = ValueAggregatorBaseDescriptor
      .generateValueAggregator(type);
    // 将所有待聚合值添加到聚合器中
    while (values.hasNext()) {
      aggregator.addNextValue(values.next());
    }

    // 获取聚合计算结果
    String val = aggregator.getReport();
    // 使用业务分组键作为输出键
    key = new Text(keyStr);
    // 输出最终聚合结果
    output.collect(key, new Text(val));
  }

  /**
   * Map方法，该类作为Reducer使用，不应该被调用
   * @param arg0 Map输入键
   * @param arg1 Map输入值
   * @param arg2 输出收集器
   * @param arg3 进度报告器
   * @throws IOException 总是抛出异常表示该方法不应该被调用
   */
  public void map(K1 arg0, V1 arg1, OutputCollector<Text, Text> arg2,
                  Reporter arg3) throws IOException {
    throw new IOException ("should not be called\n");
  }
}