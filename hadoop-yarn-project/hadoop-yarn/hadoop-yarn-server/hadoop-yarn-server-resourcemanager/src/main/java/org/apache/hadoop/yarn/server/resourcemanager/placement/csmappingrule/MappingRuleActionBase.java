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

import org.apache.hadoop.yarn.server.resourcemanager.placement.VariableContext;

/**
 * 容量调度器队列映射规则动作的抽象基类，提供了 fallback 失败回退逻辑的统一实现，
 * 具体动作需要子类实现核心业务逻辑，是大多数映射规则动作的基础父类。
 */
public abstract class MappingRuleActionBase implements MappingRuleAction {
  /**
   * 默认回退行为是拒绝提交，当主动作执行失败时会拒绝应用提交。该行为可按规则覆盖。
   */
  private MappingRuleResult fallback = MappingRuleResult.createRejectResult();

  /**
   * 获取主动作执行失败时需要执行的回退动作结果。
   * 例如：目标队列不存在、引用存在歧义时触发该回退。
   * @return 主动作失败时要执行的回退动作结果
   */
  public MappingRuleResult getFallback() {
    return fallback;
  }

  /**
   * 设置回退行为为拒绝提交，当动作无法执行时应用会被拒绝。
   * @return 当前动作对象，支持方法链调用
   */
  public MappingRuleAction setFallbackReject() {
    fallback = MappingRuleResult.createRejectResult();
    return this;
  }

  /**
   * 设置回退行为为跳过当前规则，当动作无法执行时会跳过本规则继续匹配下一条规则。
   * @return 当前动作对象，支持方法链调用
   */
  public MappingRuleAction setFallbackSkip() {
    fallback = MappingRuleResult.createSkipResult();
    return this;
  }

  /**
   * 设置回退行为为放置到默认队列，当动作无法执行时应用会被放入默认队列，
   * 如果默认队列不存在则应用会被拒绝。
   * @return 当前动作对象，支持方法链调用
   */
  public MappingRuleAction setFallbackDefaultPlacement() {
    fallback = MappingRuleResult.createDefaultPlacementResult();
    return this;
  }

  /**
   * 动作核心执行逻辑，根据映射上下文判断动作的执行结果。
   * @param variables 变量上下文，包含所有可用于匹配的变量
   * @return 动作执行结果
   */
  public abstract MappingRuleResult execute(VariableContext variables);
}