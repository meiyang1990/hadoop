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
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

/**
 * 文件概述：该抽象类是MapReduce聚合框架中聚合任务的基础类，实现了Mapper和Reducer接口，
 * 提供了聚合描述符加载、配置初始化等公共通用功能，供具体的聚合Mapper、Reducer和Combiner类继承扩展。
 * 
 * 该抽象类实现了Aggregate通用mapper、reducer和combiner类的公共功能。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ValueAggregatorJobBase<K1 extends WritableComparable,
                                             V1 extends Writable>
  implements Mapper<K1, V1, Text, Text>, Reducer<Text, Text, Text, Text> {

  // 存储所有聚合描述符实例的列表
  protected ArrayList<ValueAggregatorDescriptor> aggregatorDescriptorList = null;

  /**
   * 配置初始化方法，在任务启动时调用，完成聚合描述符配置加载和日志输出
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {
    this.initializeMySpec(job);
    this.logSpec();
  }

  /**
   * 根据配置字符串解析并创建对应的聚合描述符实例
   * @param spec 聚合描述符配置字符串
   * @param job 作业配置对象
   * @return 解析得到的聚合描述符实例，不匹配规则时返回null
   */
  private static ValueAggregatorDescriptor getValueAggregatorDescriptor(
      String spec, JobConf job) {
    if (spec == null)
      return null;
    // 按逗号分割配置字符串，保留空分段
    String[] segments = spec.split(",", -1);
    // 第一个分段为聚合类型
    String type = segments[0];
    // 如果是用户自定义聚合类型，创建对应的用户自定义聚合描述符
    if (type.compareToIgnoreCase("UserDefined") == 0) {
      String className = segments[1];
      return new UserDefinedValueAggregatorDescriptor(className, job);
    }
    return null;
  }

  /**
   * 从作业配置中读取所有聚合描述符配置，解析并创建实例列表
   * @param job 作业配置对象
   * @return 解析完成的聚合描述符实例列表
   */
  private static ArrayList<ValueAggregatorDescriptor> getAggregatorDescriptors(JobConf job) {
    // 聚合描述符配置项前缀
    String advn = "aggregator.descriptor";
    // 获取配置的聚合描述符数量
    int num = job.getInt(advn + ".num", 0);
    ArrayList<ValueAggregatorDescriptor> retv = new ArrayList<ValueAggregatorDescriptor>(num);
    // 遍历每个聚合描述符配置
    for (int i = 0; i < num; i++) {
      String spec = job.get(advn + "." + i);
      ValueAggregatorDescriptor ad = getValueAggregatorDescriptor(spec, job);
      if (ad != null) {
        retv.add(ad);
      }
    }
    return retv;
  }

  /**
   * 初始化当前实例的聚合描述符列表，如果未配置则使用默认值
   * @param job 作业配置对象
   */
  private void initializeMySpec(JobConf job) {
    this.aggregatorDescriptorList = getAggregatorDescriptors(job);
    // 如果没有配置任何聚合描述符，添加默认的基础聚合描述符
    if (this.aggregatorDescriptorList.size() == 0) {
      this.aggregatorDescriptorList
          .add(new UserDefinedValueAggregatorDescriptor(
              ValueAggregatorBaseDescriptor.class.getCanonicalName(), job));
    }
  }

  /**
   * 输出聚合配置日志，空实现，子类可重写输出自定义日志
   */
  protected void logSpec() {

  }

  /**
   * 资源清理方法，任务结束时调用，空实现，子类可重写实现自定义清理逻辑
   * @throws IOException 资源清理时可能抛出IO异常
   */
  public void close() throws IOException {
  }
}