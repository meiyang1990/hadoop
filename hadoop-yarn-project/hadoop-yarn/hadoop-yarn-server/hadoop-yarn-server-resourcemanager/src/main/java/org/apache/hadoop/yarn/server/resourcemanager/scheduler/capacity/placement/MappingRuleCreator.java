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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement;

import static org.apache.hadoop.util.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleAction;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleActions;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleMatcher;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.MappingRuleMatchers;

// These are generated classes - use GeneratePojos class to create them
// if they are missing
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Policy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 容量调度器应用队列映射规则创建工厂，从JSON配置文件解析并生成内存中的映射规则对象
 * 用于将应用根据用户、用户组、应用名称等信息匹配并放置到对应的队列
 */
public class MappingRuleCreator {
  // 匹配所有用户的通配符
  private static final String ALL_USER = "*";
  private static Logger LOG = LoggerFactory.getLogger(MappingRuleCreator.class);

  /**
   * 从指定路径JSON文件加载映射规则配置
   * @param filePath JSON文件路径
   * @return 解析后的规则描述对象
   * @throws IOException 文件读取或解析失败
   */
  public MappingRulesDescription getMappingRulesFromJsonFile(String filePath)
      throws IOException {
    byte[] fileContents = Files.readAllBytes(Paths.get(filePath));
    return getMappingRulesFromJson(fileContents);
  }

  MappingRulesDescription getMappingRulesFromJson(byte[] contents)
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(contents, MappingRulesDescription.class);
  }

  MappingRulesDescription getMappingRulesFromJson(String contents)
      throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(contents, MappingRulesDescription.class);
  }

  /**
   * 从JSON文件解析并生成完整映射规则列表
   * @param jsonPath JSON文件路径
   * @return 可执行的映射规则列表
   * @throws IOException 文件读取或解析失败
   */
  public List<MappingRule> getMappingRulesFromFile(String jsonPath)
      throws IOException {
    MappingRulesDescription desc = getMappingRulesFromJsonFile(jsonPath);
    return getMappingRules(desc);
  }

  /**
   * 从JSON字符串解析并生成完整映射规则列表
   * @param json JSON格式的规则配置字符串
   * @return 可执行的映射规则列表
   * @throws IOException JSON解析失败
   */
  public List<MappingRule> getMappingRulesFromString(String json)
      throws IOException {
    MappingRulesDescription desc = getMappingRulesFromJson(json);
    return getMappingRules(desc);
  }

  /**
   * 将JSON解析后的规则描述对象转换为可执行的映射规则列表
   * @param rules JSON解析得到的规则描述
   * @return 可执行的映射规则列表
   */
  @VisibleForTesting
  List<MappingRule> getMappingRules(MappingRulesDescription rules) {
    List<MappingRule> mappingRules = new ArrayList<>();

    // 遍历每条规则，转换为内部可执行对象
    for (Rule rule : rules.getRules()) {
      // 检查规则必填参数是否完整
      checkMandatoryParameters(rule);

      // 创建匹配器
      MappingRuleMatcher matcher = createMatcher(rule);
      // 创建匹配后的放置动作
      MappingRuleAction action = createAction(rule);
      // 设置匹配失败后的回退策略
      setFallbackToAction(rule, action);

      // 组装完整规则并添加到结果列表
      MappingRule mappingRule = new MappingRule(matcher, action);
      mappingRules.add(mappingRule);
    }

    return mappingRules;
  }

  /**
   * 根据规则配置创建匹配器，决定哪些应用会命中当前规则
   * @param rule JSON规则配置对象
   * @return 匹配器实例
   */
  private MappingRuleMatcher createMatcher(Rule rule) {
    String matches = rule.getMatches();
    Type type = rule.getType();

    MappingRuleMatcher matcher = null;
    // 根据匹配类型创建不同匹配器
    switch (type) {
    case USER:
      // 匹配所有用户
      if (ALL_USER.equals(matches)) {
        matcher = MappingRuleMatchers.createAllMatcher();
      } else {
        // 匹配指定用户
        matcher = MappingRuleMatchers.createUserMatcher(matches);
      }
      break;
    case GROUP:
      // 用户组匹配不支持*通配符
      checkArgument(!ALL_USER.equals(matches), "Cannot match '*' for groups");
      matcher = MappingRuleMatchers.createUserGroupMatcher(matches);
      break;
    case APPLICATION:
      // 匹配指定应用名称
      matcher = MappingRuleMatchers.createApplicationNameMatcher(matches);
      break;
    default:
      throw new IllegalArgumentException("Unknown type: " + type);
    }

    return matcher;
  }

  /**
   * 根据规则配置创建放置动作，决定命中规则后应用如何放置到队列
   * @param rule JSON规则配置对象
   * @return 放置动作实例
   */
  private MappingRuleAction createAction(Rule rule) {
    Policy policy = rule.getPolicy();
    String queue = rule.getParentQueue();

    boolean create;
    // 如果未配置自动创建队列标记，默认允许自动创建
    if (rule.getCreate() == null) {
      LOG.debug("Create flag is not set for rule {},"
          + "using \"true\" as default", rule);
      create = true;
    } else {
      create = rule.getCreate();
    }

    MappingRuleAction action = null;
    // 根据放置策略创建不同放置动作
    switch (policy) {
    case DEFAULT_QUEUE:
      // 放置到默认队列
      action = MappingRuleActions.createPlaceToDefaultAction();
      break;
    case REJECT:
      // 拒绝应用提交
      action = MappingRuleActions.createRejectAction();
      break;
    case PRIMARY_GROUP:
      // 放置到用户主组同名队列
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(queue, "%primary_group"), create);
      break;
    case SECONDARY_GROUP:
      // 放置到用户副组同名队列
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(queue, "%secondary_group"), create);
      break;
    case PRIMARY_GROUP_USER:
      // 放置到 主组.用户名 格式队列
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(rule.getParentQueue(),
              "%primary_group.%user"), create);
      break;
    case SECONDARY_GROUP_USER:
      // 放置到 副组.用户名 格式队列
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(rule.getParentQueue(),
              "%secondary_group.%user"), create);
      break;
    case SPECIFIED:
      // 使用配置中指定的队列名称
      action = MappingRuleActions.createPlaceToQueueAction("%specified",
          create);
      break;
    case CUSTOM:
      // 使用自定义占位符格式的队列路径
      String customTarget = rule.getCustomPlacement();
      checkArgument(customTarget != null, "custom queue is undefined");
      action = MappingRuleActions.createPlaceToQueueAction(customTarget,
          create);
      break;
    case USER:
      // 放置到用户名同名队列
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(rule.getParentQueue(),
              "%user"), create);
      break;
    case APPLICATION_NAME:
      // 放置到应用名称同名队列
      action = MappingRuleActions.createPlaceToQueueAction(
          getTargetQueue(rule.getParentQueue(),
              "%application"), create);
      break;
    case SET_DEFAULT_QUEUE:
      // 更新应用默认队列，不直接放置
      String defaultQueue = rule.getValue();
      checkArgument(defaultQueue != null, "default queue is undefined");
      action = MappingRuleActions.createUpdateDefaultAction(defaultQueue);
      break;
    default:
      throw new IllegalArgumentException(
          "Unsupported policy: " + policy);
    }

    return action;
  }

  /**
   * 为放置动作设置匹配失败的回退策略
   * @param rule JSON规则配置对象
   * @param action 已创建的放置动作
   */
  private void setFallbackToAction(Rule rule, MappingRuleAction action) {
    FallbackResult fallbackResult = rule.getFallbackResult();

    // 未配置回退策略时，默认跳过当前规则
    if (fallbackResult == null) {
      action.setFallbackSkip();
      LOG.debug("Fallback is not defined for rule {}, using SKIP as default", rule);
      return;
    }

    // 根据配置设置不同回退策略
    switch (fallbackResult) {
    case PLACE_DEFAULT:
      // 回退到默认队列放置
      action.setFallbackDefaultPlacement();
      break;
    case REJECT:
      // 回退动作是拒绝应用提交
      action.setFallbackReject();
      break;
    case SKIP:
      // 回退动作是跳过当前规则，继续匹配下一条
      action.setFallbackSkip();
      break;
    default:
      throw new IllegalArgumentException(
          "Unsupported fallback rule " + fallbackResult);
    }
  }

  /**
   * 拼接父队列和占位符得到完整目标队列路径
   * @param parent 父队列名称
   * @param placeholder 子队列占位符
   * @return 完整目标队列路径
   */
  private String getTargetQueue(String parent, String placeholder) {
    return (parent == null) ? placeholder : parent + "." + placeholder;
  }

  /**
   * 检查规则必填参数是否完整合法
   * @param rule 待检查的规则配置
   */
  private void checkMandatoryParameters(Rule rule) {
    checkArgument(rule.getPolicy() != null, "Rule policy is undefined");
    checkArgument(rule.getType() != null, "Rule type is undefined");
    checkArgument(rule.getMatches() != null, "Match string is undefined");
    checkArgument(!StringUtils.isEmpty(rule.getMatches()),
        "Match string is empty");
  }
}