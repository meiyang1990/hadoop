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
 * 容量调度器队列映射规则，封装用户定义的单个应用提交队列映射规则。
 * 每个规则包含匹配器和动作两部分：匹配器判断规则是否适用于当前提交的应用，动作定义匹配后需要执行的映射操作。
 * 同时支持回退动作，当主动作执行失败（例如目标队列不存在）时会执行回退逻辑。
 */
public class MappingRule {
  /** 用户映射规则类型标识 */
  public static final String USER_MAPPING = "u";
  /** 用户组映射规则类型标识 */
  public static final String GROUP_MAPPING = "g";
  /** 应用名称映射规则类型标识 */
  public static final String APPLICATION_MAPPING = "a";
  // 当前规则的匹配器
  private final MappingRuleMatcher matcher;
  // 当前规则匹配后执行的动作
  private final MappingRuleAction action;

  /**
   * 构造映射规则对象
   * @param matcher 规则匹配器
   * @param action 匹配后执行的动作
   */
  public MappingRule(MappingRuleMatcher matcher, MappingRuleAction action) {
    this.matcher = matcher;
    this.action = action;
  }

  /**
   * 评估当前规则是否匹配当前应用提交，返回匹配结果
   * @param variables 变量上下文，包含当前应用提交的所有上下文变量
   * @return 规则执行结果，如果不匹配返回跳过结果
   */
  public MappingRuleResult evaluate(VariableContext variables) {
    if (matcher.match(variables)) {
      return action.execute(variables);
    }

    return MappingRuleResult.createSkipResult();
  }

  /**
   * 获取当前规则动作的回退结果
   * @return 回退结果，当主动作失败时使用
   */
  public MappingRuleResult getFallback() {
    return action.getFallback();
  }

  /**
   * 从旧版格式配置创建映射规则，用于无类型标识的旧版应用映射规则
   * 旧版格式省略了应用映射的'a'标识，默认全部视为应用映射规则
   * @param source 匹配源，指定哪些应用会匹配该规则
   * @param path 目标队列路径，应用将被放置到该队列
   * @return 根据参数创建的映射规则
   */
  public static MappingRule createLegacyRule(String source, String path) {
    return createLegacyRule(APPLICATION_MAPPING, source, path);
  }

  /**
   * 从旧版格式配置创建映射规则，旧版格式为 [TYPE]:SOURCE:PATH 例如 u:bob:root.users.%user
   * @param type 规则类型，u=用户映射，g=用户组映射，a=应用名称映射
   * @param source 匹配源，指定哪些应用提交会匹配该规则
   * @param path 目标队列路径，应用将被放置到该队列
   * @return 根据参数创建的映射规则
   */
  public static MappingRule createLegacyRule(
      String type, String source, String path) {
    MappingRuleMatcher matcher;
    MappingRuleAction action = MappingRuleActions.createPlaceToQueueAction(
        path, true);
    // 旧版规则默认回退到默认队列放置，符合大多数场景
    action.setFallbackDefaultPlacement();

    // 根据规则类型创建对应匹配器
    switch (type) {
    case USER_MAPPING:
      if (source.equals("%user")) {
        // %user 表示匹配所有用户，创建全匹配 matcher
        matcher = MappingRuleMatchers.createAllMatcher();
      } else {
        // 创建指定用户名匹配 matcher
        matcher = MappingRuleMatchers.createUserMatcher(source);
      }
      break;
    case GROUP_MAPPING:
      // 创建用户组匹配 matcher
      matcher = MappingRuleMatchers.createUserGroupMatcher(source);
      break;
    case APPLICATION_MAPPING:
      // 创建应用名称匹配 matcher
      matcher = MappingRuleMatchers.createApplicationNameMatcher(source);
      break;
    default:
      throw new IllegalArgumentException("Invalid mapping rule type '" +
            type + "'");
    }

    return new MappingRule(matcher, action);
  }

  /**
   * 验证当前映射规则的有效性
   * @param ctx 验证上下文
   * @throws YarnException 规则验证失败时抛出异常
   */
  public void validate(MappingRuleValidationContext ctx)
      throws YarnException {
    this.action.validate(ctx);
  }

  @Override
  public String toString() {
    return "MappingRule{" +
      "matcher=" + matcher +
      ", action=" + action +
      '}';
  }
}