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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;

/**
 * @file UserDefinedValueAggregatorDescriptor.java
 * @brief 用户自定义值聚合器描述器的包装类，适配旧版MapReduce API
 *
 * 为MapReduce聚合框架提供用户自定义聚合器描述器的动态加载能力，
 * 负责动态加载用户自定义类，并将请求转发给加载后的实例处理。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class UserDefinedValueAggregatorDescriptor extends org.apache.hadoop.
    mapreduce.lib.aggregate.UserDefinedValueAggregatorDescriptor
    implements ValueAggregatorDescriptor {

  /**
   * 根据类名动态加载并创建用户自定义类的实例
   * @param className 要创建实例的类的全限定名
   * @return 动态创建的类实例
   */
  public static Object createInstance(String className) {
    return org.apache.hadoop.mapreduce.lib.aggregate.
      UserDefinedValueAggregatorDescriptor.createInstance(className);
  }

  /**
   * 构造方法，加载并初始化用户自定义聚合器描述器实例
   * @param className 用户自定义描述器类的全限定名
   * @param job 作业配置对象，用于配置描述器
   */
  public UserDefinedValueAggregatorDescriptor(String className, JobConf job) {
    super(className, job);
    ((ValueAggregatorDescriptor)theAggregatorDescriptor).configure(job);
  }

  /**
   * 配置方法，此处预留扩展点无实际逻辑
   * @param job 作业配置对象
   */
  public void configure(JobConf job) {

  }

}