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
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 文件: ValueAggregatorMapper.java
 * 所属模块: MapReduce计算框架核心客户端
 * 核心职责: 为MapReduce聚合框架提供通用的Mapper实现，基于聚合描述符动态生成聚合键值对
 * 设计目的: 实现通用化的Map端聚合预处理，让用户可以通过配置描述符定义不同聚合逻辑，无需自定义Mapper
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorMapper<K1 extends WritableComparable,
                                   V1 extends Writable>
  extends ValueAggregatorJobBase<K1, V1> {

  /**
   * Map阶段处理函数，根据聚合描述符生成聚合键值对并输出
   * @param key 输入键
   * @param value 输入值
   * @param output 输出收集器
   * @param reporter 进度报告器
   * @throws IOException 输出异常
   */
  public void map(K1 key, V1 value,
                  OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    // 遍历所有聚合描述符
    Iterator iter = this.aggregatorDescriptorList.iterator();
    while (iter.hasNext()) {
      ValueAggregatorDescriptor ad = (ValueAggregatorDescriptor) iter.next();
      // 根据当前输入生成对应聚合键值对
      Iterator<Entry<Text, Text>> ens =
        ad.generateKeyValPairs(key, value).iterator();
      // 输出所有生成的聚合键值对
      while (ens.hasNext()) {
        Entry<Text, Text> en = ens.next();
        output.collect(en.getKey(), en.getValue());
      }
    }
  }

  /**
   * Reduce阶段处理函数，该Mapper实现不会使用Reduce逻辑，调用即抛出异常
   * @param arg0 输入键
   * @param arg1 输入值迭代器
   * @param arg2 输出收集器
   * @param arg3 进度报告器
   * @throws IOException 固定抛出异常，不应该被调用
   */
  public void reduce(Text arg0, Iterator<Text> arg1,
                     OutputCollector<Text, Text> arg2,
                     Reporter arg3) throws IOException {
    throw new IOException("should not be called\n");
  }
}