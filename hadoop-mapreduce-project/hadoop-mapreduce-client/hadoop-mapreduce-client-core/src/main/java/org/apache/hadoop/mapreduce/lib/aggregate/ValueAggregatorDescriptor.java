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

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * 文件说明: 值聚合器描述符接口，定义了基于聚合的MapReduce作业中，Mapper端处理输入键值对、生成聚合键值对的约定
 * 
 * 核心职责: 该接口定义了聚合描述符需要实现的规范，用于将原始输入键值对转换为带聚合类型标识的键值对，
 * 供后续Reduce/Combiner阶段按照对应的聚合类型完成聚合计算。
 * 在基于Aggregate框架的MapReduce作业中，Mapper会根据配置创建一个或多个该接口的实现，
 * 对每个输入键值对生成对应的聚合id/值对，完成聚合任务的预处理。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ValueAggregatorDescriptor {

  // 聚合类型与自定义部分的分隔符
  public static final String TYPE_SEPARATOR = ":";

  // 用于计数聚合的固定值1
  public static final Text ONE = new Text("1");

  /**
   * 为输入键值对生成聚合id/值对列表
   * 该方法由基于Aggregate框架作业的Mapper调用，将原始输入转换为带聚合类型标识的键值对，
   * 供后续Reduce/Combiner阶段根据聚合类型执行对应的聚合计算
   * 
   * @param key 输入原始键
   * @param val 输入原始值
   * @return 聚合id/值对列表，聚合id中编码了聚合类型，用于指导Reduce/Combiner阶段的聚合方式
   */
  public ArrayList<Entry<Text, Text>> generateKeyValPairs(Object key,
                                                          Object val);

  /**
   * 根据配置初始化聚合描述符对象
   * 
   * @param conf 作业配置对象，包含初始化该描述符所需的配置参数
   */
  public void configure(Configuration conf);
}