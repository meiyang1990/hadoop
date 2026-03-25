// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file unless in compliance
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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/**
 * 文件说明: MapReduce旧API聚合框架的值聚合器描述符接口，定义了聚合器描述符必须实现的契约
 * 核心职责: 为基于Aggregate的MapReduce作业提供聚合描述能力，根据输入键值对生成聚合ID/值对
 * 
 * 业务作用: 在基于聚合框架的MapReduce作业中，Mapper阶段使用该接口的实现类
 * 将输入的原始键值对转换为带聚合类型标识的键值对，供Reduce/Combiner阶段进行汇总计算
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ValueAggregatorDescriptor extends 
    org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorDescriptor {

  // 聚合类型分隔符，从新版API继承
  public static final String TYPE_SEPARATOR = org.apache.hadoop.mapreduce.
      lib.aggregate.ValueAggregatorDescriptor.TYPE_SEPARATOR;

  // 计数聚合专用值常量，代表计数1
  public static final Text ONE = org.apache.hadoop.mapreduce.
      lib.aggregate.ValueAggregatorDescriptor.ONE;

  /**
   * 配置聚合器描述符，从作业配置中加载初始化参数
   * 
   * @param job 作业配置对象，包含聚合器描述符所需的配置信息
   */
  public void configure(JobConf job);
}