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

package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 该接口定义了可通过作业配置进行初始化的组件约定
 * 供MapReduce框架中需要根据作业配置初始化的组件实现，比如Partitioner、InputFormat等
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface JobConfigurable {
  /**
   * 使用给定的作业配置初始化当前组件实例
   * 在组件实例创建后会被MapReduce框架调用，完成组件的初始化工作
   * 
   * @param job 当前作业的配置对象
   */
  void configure(JobConf job);
}