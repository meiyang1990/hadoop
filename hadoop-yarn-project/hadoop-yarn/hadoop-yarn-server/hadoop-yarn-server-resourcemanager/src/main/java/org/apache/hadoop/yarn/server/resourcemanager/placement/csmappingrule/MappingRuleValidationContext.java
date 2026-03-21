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

package org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule;

import org.apache.hadoop.yarn.exceptions.YarnException;

import java.util.Set;

/**
 * YARN容量调度器应用队列映射规则验证上下文接口
 * 提供映射规则配置验证所需的所有方法和数据，通过抽离此接口降低映射规则相关代码对调度器核心模块的依赖，
 * 使得调度器核心逻辑可以独立修改不影响映射规则实现。
 */
public interface MappingRuleValidationContext {
  /**
   * 验证队列路径是否合法，检查该路径是否能匹配到已知队列，不考虑动态变量上下文。
   * @param queuePath 需要验证的队列路径
   * @return 验证通过返回true
   * @throws YarnException 如果提供的队列路径非法则抛出异常
   */
  boolean validateQueuePath(String queuePath) throws YarnException;

  /**
   * 检查队列路径是否为静态路径（不包含任何已定义的动态变量）。
   * @param queuePath 需要检查的队列路径
   * @return 没有动态片段返回true
   * @throws YarnException 如果路径包含非法片段（例如空片段）则抛出异常
   */
  boolean isPathStatic(String queuePath) throws YarnException;

  /**
   * 向验证上下文添加一个可变动态变量，用于判断路径是否包含动态片段。
   * @param variable 变量名称
   * @throws YarnException 如果该变量已经作为不可变变量存在则抛出异常
   */
  void addVariable(String variable) throws YarnException;

  /**
   * 向验证上下文添加一个不可变动态变量，用于判断路径是否包含动态片段。
   * @param variable 不可变变量名称
   * @throws YarnException 如果该变量已经作为可变变量存在则抛出异常
   */
  void addImmutableVariable(String variable) throws YarnException;

  /**
   * 获取上下文所有已知动态变量集合。
   * @return 所有已知变量的集合
   */
  Set<String> getVariables();
}