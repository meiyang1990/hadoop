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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 文件：ValueAggregatorJobBase.java
 * 所属模块：MapReduce 核心库，聚合计算框架
 * 核心职责：ValueAggregator聚合计算框架的抽象基类，实现了聚合任务Mapper、Reducer、Combiner的通用基础功能，
 * 负责聚合器描述符的加载、初始化和管理，为具体聚合任务组件提供公共能力。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ValueAggregatorJobBase<K1 extends WritableComparable<?>,
                                             V1 extends Writable>
{
  // 聚合器描述符配置项前缀
  public static final String DESCRIPTOR = "mapreduce.aggregate.descriptor";
  // 聚合器描述符数量配置项
  public static final String DESCRIPTOR_NUM = 
    "mapreduce.aggregate.descriptor.num";
  // 用户自定义聚合器Jar包路径配置项
  public static final String USER_JAR = "mapreduce.aggregate.user.jar.file";
  
  // 缓存已初始化的聚合器描述符列表
  protected static ArrayList<ValueAggregatorDescriptor> aggregatorDescriptorList = null;

  /**
   * 聚合任务初始化方法，加载并初始化聚合器描述符，输出配置日志
   * @param job 任务配置对象
   */
  public static void setup(Configuration job) {
    initializeMySpec(job);
    logSpec();
  }

  /**
   * 根据配置字符串解析并创建聚合器描述符实例
   * @param spec 聚合器描述符配置字符串
   * @param conf 任务配置对象
   * @return 创建完成的聚合器描述符实例，解析失败返回null
   */
  protected static ValueAggregatorDescriptor getValueAggregatorDescriptor(
      String spec, Configuration conf) {
    if (spec == null)
      return null;
    // 按逗号分割配置项
    String[] segments = spec.split(",", -1);
    // 第一个分段为聚合器类型
    String type = segments[0];
    // 如果是用户自定义聚合器类型，加载用户指定类
    if (type.compareToIgnoreCase("UserDefined") == 0) {
      String className = segments[1];
      return new UserDefinedValueAggregatorDescriptor(className, conf);
    }
    return null;
  }

  /**
   * 从任务配置中加载所有聚合器描述符
   * @param conf 任务配置对象
   * @return 加载完成的聚合器描述符列表
   */
  protected static ArrayList<ValueAggregatorDescriptor> getAggregatorDescriptors(
      Configuration conf) {
    // 获取配置中声明的聚合器描述符数量
    int num = conf.getInt(DESCRIPTOR_NUM, 0);
    ArrayList<ValueAggregatorDescriptor> retv = 
      new ArrayList<ValueAggregatorDescriptor>(num);
    // 遍历加载每个聚合器描述符
    for (int i = 0; i < num; i++) {
      String spec = conf.get(DESCRIPTOR + "." + i);
      ValueAggregatorDescriptor ad = getValueAggregatorDescriptor(spec, conf);
      if (ad != null) {
        retv.add(ad);
      }
    }
    return retv;
  }

  /**
   * 初始化聚合器描述符列表，如果没有配置则添加默认的基础聚合器描述符
   * @param conf 任务配置对象
   */
  private static void initializeMySpec(Configuration conf) {
    aggregatorDescriptorList = getAggregatorDescriptors(conf);
    if (aggregatorDescriptorList.size() == 0) {
      aggregatorDescriptorList
          .add(new UserDefinedValueAggregatorDescriptor(
              ValueAggregatorBaseDescriptor.class.getCanonicalName(), conf));
    }
  }

  /**
   * 输出聚合配置日志，留给子类实现
   */
  protected static void logSpec() {
  }
}