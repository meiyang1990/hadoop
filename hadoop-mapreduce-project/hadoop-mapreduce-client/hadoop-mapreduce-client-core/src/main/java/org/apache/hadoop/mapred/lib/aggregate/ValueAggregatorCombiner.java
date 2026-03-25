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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 文件说明: ValueAggregator框架的通用Combiner实现，用于MapReduce聚合任务中，在Map端提前聚合相同key的数据，减少网络传输量
 *
 * 本类实现了Aggregate聚合框架的Combiner逻辑，对Map输出的相同key按指定聚合类型提前做局部聚合，优化MapReduce作业性能。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorCombiner<K1 extends WritableComparable,
                                     V1 extends Writable>
  extends ValueAggregatorJobBase<K1, V1> {

  /**
   * 功能说明: 配置Combiner，该Combiner不需要额外初始化配置
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {

  }

  /** 
   * 功能说明: 对相同key的Map输出值进行局部聚合
   * @param key 输入key，Text类型，前缀指示聚合类型
   * @param values 待聚合的value迭代器
   * @param output 输出收集器，用于收集聚合结果
   * @param reporter 任务报告器，用于上报进度与状态
   * @throws IOException 输出时可能抛出IO异常
   */
  public void reduce(Text key, Iterator<Text> values,
                     OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    // 解析key字符串
    String keyStr = key.toString();
    // 定位聚合类型分隔符位置
    int pos = keyStr.indexOf(ValueAggregatorDescriptor.TYPE_SEPARATOR);
    // 提取聚合类型
    String type = keyStr.substring(0, pos);
    // 根据聚合类型生成对应的聚合器实例
    ValueAggregator aggregator = ValueAggregatorBaseDescriptor
      .generateValueAggregator(type);
    // 将所有待聚合值添加到聚合器中
    while (values.hasNext()) {
      aggregator.addNextValue(values.next());
    }
    // 获取聚合后的结果迭代器
    Iterator outputs = aggregator.getCombinerOutput().iterator();

    // 输出所有聚合结果
    while (outputs.hasNext()) {
      Object v = outputs.next();
      if (v instanceof Text) {
        output.collect(key, (Text)v);
      } else {
        output.collect(key, new Text(v.toString()));
      }
    }
  }

  /** 
   * 功能说明: 关闭Combiner，无需额外清理操作
   * @throws IOException 可能抛出IO异常
   */
  public void close() throws IOException {

  }

  /** 
   * 功能说明: Map方法，Combiner不会执行map逻辑，该方法不应被调用
   * @param arg0 Map输入key
   * @param arg1 Map输入value
   * @param arg2 输出收集器
   * @param arg3 任务报告器
   * @throws IOException 始终抛出异常提示该方法不应被调用
   */
  public void map(K1 arg0, V1 arg1, OutputCollector<Text, Text> arg2,
                  Reporter arg3) throws IOException {
    throw new IOException ("should not be called\n");
  }
}