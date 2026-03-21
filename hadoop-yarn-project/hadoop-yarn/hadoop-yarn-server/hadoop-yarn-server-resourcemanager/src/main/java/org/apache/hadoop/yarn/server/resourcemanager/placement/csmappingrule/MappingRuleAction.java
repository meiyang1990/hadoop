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
import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;

/**
 * 容量调度器映射规则的动作接口，定义匹配规则命中后对应用提交的处理动作，
 * 负责决定应用最终应该放置到哪个队列，以及执行失败后的回退策略。
 */
public interface MappingRuleAction {
  /**
   * 获取主动作执行失败时的回退动作（例如目标队列不存在、引用不明确等场景）
   * @return 主动作失败时需要执行的回退动作
   */
  MappingRuleResult getFallback();

  /**
   * 动作的核心执行逻辑，根据映射上下文变量计算得到动作结果
   * @param variables 变量上下文，包含规则匹配所需的所有变量
   * @return 本次动作执行的结果
   */
  MappingRuleResult execute(VariableContext variables);


  /**
   * 设置回退动作为拒绝应用提交，动作执行失败时将直接拒绝该应用
   * @return 当前动作对象，支持方法链式调用
   */
  MappingRuleAction setFallbackReject();

  /**
   * 设置回退动作为跳过当前规则，动作执行失败时跳过本规则，继续尝试下一条匹配规则
   * @return 当前动作对象，支持方法链式调用
   */
  MappingRuleAction setFallbackSkip();

  /**
   * 设置回退动作为放置到默认队列，动作执行失败时将应用放入默认队列；
   * 如果默认队列也不存在，则会拒绝应用提交
   * @return 当前动作对象，支持方法链式调用
   */
  MappingRuleAction setFallbackDefaultPlacement();

  /**
   * 验证当前动作配置的合法性，检测到配置错误时需要抛出异常
   * @param ctx 验证上下文，包含验证所需的所有对象和辅助方法
   * @throws YarnException 验证失败时抛出该异常
   */
  void validate(MappingRuleValidationContext ctx) throws YarnException;
}