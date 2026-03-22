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
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ValueAggregator框架的通用Mapper实现，负责预处理输入数据并生成聚合键值对
 * 用于MapReduce聚合计算场景，在Map阶段提前完成分组合并准备工作
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorMapper<K1 extends WritableComparable<?>,
                                   V1 extends Writable>
  extends Mapper<K1, V1, Text, Text> {

  /**
   * Mapper初始化方法，负责完成聚合框架的全局初始化
   * @param context MapReduce任务上下文
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void setup(Context context) 
      throws IOException, InterruptedException {
    ValueAggregatorJobBase.setup(context.getConfiguration());
  }
  
  /**
   * Map阶段核心处理方法，遍历所有聚合描述符生成聚合键值对并输出
   * @param key 输入键
   * @param value 输入值
   * @param context MapReduce任务上下文
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  public void map(K1 key, V1 value,
      Context context) throws IOException, InterruptedException  {

    // 遍历全局聚合描述符列表
    Iterator<?> iter = 
      ValueAggregatorJobBase.aggregatorDescriptorList.iterator();
    while (iter.hasNext()) {
      ValueAggregatorDescriptor ad = (ValueAggregatorDescriptor) iter.next();
      // 由聚合描述符生成当前输入记录对应的聚合键值对
      Iterator<Entry<Text, Text>> ens =
        ad.generateKeyValPairs(key, value).iterator();
      // 输出所有生成的聚合键值对
      while (ens.hasNext()) {
        Entry<Text, Text> en = ens.next();
        context.write(en.getKey(), en.getValue());
      }
    }
  }
}