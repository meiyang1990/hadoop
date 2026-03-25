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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;

import java.util.ArrayList;
import java.util.Collection;

/**
 * 将容量调度器旧版（配置文件格式）的应用放置映射规则转换为新版JSON格式的转换器
 * 支持用户映射规则、用户组映射规则和应用名称映射规则的格式转换
 */
public class LegacyMappingRuleToJson {
  // 旧版规则解析辅助常量
  public static final String RULE_PART_DELIMITER = ":";
  public static final String PREFIX_USER_MAPPING = "u";
  public static final String PREFIX_GROUP_MAPPING = "g";

  // 旧版规则匹配器变量名
  public static final String MATCHER_APPLICATION = "%application";
  public static final String MATCHER_USER = "%user";

  // 旧版规则目标队列映射变量，可用于目标路径中动态替换
  public static final String MAPPING_PRIMARY_GROUP = "%primary_group";
  public static final String MAPPING_SECONDARY_GROUP = "%secondary_group";
  public static final String MAPPING_USER = MATCHER_USER;

  // JSON格式匹配全部用户的通配符
  public static final String JSON_MATCH_ALL = "*";

  // JSON规则定义中常用的节点名称常量
  public static final String JSON_NODE_POLICY = "policy";
  public static final String JSON_NODE_PARENT_QUEUE = "parentQueue";
  public static final String JSON_NODE_CUSTOM_PLACEMENT = "customPlacement";
  public static final String JSON_NODE_MATCHES = "matches";

  /**
   * 内部使用的Jackson ObjectMapper实例，用于创建JSON节点
   */
  private ObjectMapper objectMapper = new ObjectMapper();

  /**
   * 存储待转换的旧版用户/用户组映射规则列表
   */
  private Collection<String> userGroupMappingRules = new ArrayList<>();
  /**
   * 存储待转换的旧版应用名称映射规则列表
   */
  private Collection<String> applicationNameMappingRules = new ArrayList<>();

  /**
   * 设置旧版格式的用户用户组映射规则，格式与容量调度器配置文件中一致
   * 例如: u:bob:root.groups.%primary_group,u:%user:root.default
   *
   * @param rules 包含全部用户用户组映射规则的逗号分隔字符串
   * @return 当前对象，支持链式调用
   */
  public LegacyMappingRuleToJson setUserGroupMappingRules(String rules) {
    setUserGroupMappingRules(StringUtils.getTrimmedStringCollection(rules));
    return this;
  }

  /**
   * 设置用户用户组映射规则，集合中每个元素为一条规则
   *
   * @param rules 规则集合，每条一个条目
   * @return 当前对象，支持链式调用
   */
  public LegacyMappingRuleToJson setUserGroupMappingRules(
      Collection<String> rules) {
    if (rules != null) {
      userGroupMappingRules = rules;
    } else {
      userGroupMappingRules = new ArrayList<>();
    }
    return this;
  }

  /**
   * 设置旧版格式的应用名称映射规则，格式与容量调度器配置文件中一致
   * 例如: mapreduce:root.apps.%application,%application:root.default
   *
   * @param rules 包含全部应用名称映射规则的逗号分隔字符串
   * @return 当前对象，支持链式调用
   */
  public LegacyMappingRuleToJson setAppNameMappingRules(String rules) {
    setAppNameMappingRules(StringUtils.getTrimmedStringCollection(rules));
    return this;
  }

  /**
   * 设置应用名称映射规则，集合中每个元素为一条规则
   *
   * @param rules 规则集合，每条一个条目
   * @return 当前对象，支持链式调用
   */
  public LegacyMappingRuleToJson setAppNameMappingRules(
      Collection<String> rules) {
    if (rules != null) {
      applicationNameMappingRules = rules;
    } else {
      applicationNameMappingRules = new ArrayList<>();
    }

    return this;
  }

  /**
   * 执行转换，基于已设置的映射规则生成新版JSON格式配置
   * 需要先通过setAppNameMappingRules和setUserGroupMappingRules设置待转换规则
   * @return 转换后的JSON格式规则字符串，无规则时返回null
   */
  public String convert() {
    // 创建基础JSON配置结构
    ObjectNode rootNode = objectMapper.createObjectNode();
    ArrayNode rulesNode = objectMapper.createArrayNode();
    rootNode.set("rules", rulesNode);

    // 处理并添加所有用户用户组映射规则
    for (String rule : userGroupMappingRules) {
      rulesNode.add(convertUserGroupMappingRule(rule));
    }

    // 处理并添加所有应用名称映射规则
    for (String rule : applicationNameMappingRules) {
      rulesNode.add(convertAppNameMappingRule(rule));
    }

    // 无转换规则时返回null
    if (rulesNode.size() == 0) {
      return null;
    }

    try {
      // 格式化输出带缩进的JSON字符串
      return objectMapper
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(rootNode);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }

    return null;
  }

  /**
   * 处理单条用户/用户组映射规则，分发到对应创建方法生成JSON节点
   * @param rule 待转换的单条旧版格式规则
   * @return 转换后的规则JSON节点
   */
  ObjectNode convertUserGroupMappingRule(String rule) {
    // 按冒号分割规则，预期得到3个部分：类型、匹配值、目标队列
    String[] mapping = splitRule(rule, 3);
    String ruleType = mapping[0];
    String ruleMatch = mapping[1];
    String ruleTarget = mapping[2];

    if (ruleType.equals(PREFIX_USER_MAPPING)) {
      // 类型为用户映射，调用用户规则创建方法
      return createUserMappingRule(ruleMatch, ruleTarget);
    }

    if (ruleType.equals(PREFIX_GROUP_MAPPING)) {
      // 类型为用户组映射，调用用户组规则创建方法
      return createGroupMappingRule(ruleMatch, ruleTarget);
    }

    throw new IllegalArgumentException(
        "User group mapping rule must start with prefix '" +
            PREFIX_USER_MAPPING + "' or '" + PREFIX_GROUP_MAPPING + "'");
  }

  /**
   * 处理单条应用名称映射规则，生成对应JSON节点
   * @param rule 待转换的单条旧版格式规则
   * @return 转换后的规则JSON节点
   */
  ObjectNode convertAppNameMappingRule(String rule) {
    // 按冒号分割规则，预期得到2个部分：匹配值、目标队列
    String[] mapping = splitRule(rule, 2);
    String ruleMatch = mapping[0];
    String ruleTarget = mapping[1];

    return createApplicationNameMappingRule(ruleMatch, ruleTarget);
  }

  /**
   * 拆分规则字符串并校验格式，确保拆分后部分数量正确且无空值
   * @param rule 待拆分的映射规则
   * @param expectedParts 预期拆分后的部分数量
   * @return 拆分后的字符串数组
   * @throws IllegalArgumentException 当部分数量不匹配或存在空部分时抛出
   */
  private String[] splitRule(String rule, int expectedParts) {
    // 按分隔符拆分并修剪每个部分的空格
    String[] mapping = StringUtils
        .getTrimmedStringCollection(rule, RULE_PART_DELIMITER)
        .toArray(new String[] {});

    // 校验拆分后部分数量是否符合预期
    if (mapping.length != expectedParts) {
      throw new IllegalArgumentException("Invalid rule '" + rule +
          "' expected parts: " + expectedParts +
          " actual parts: " + mapping.length);
    }

    // 校验所有部分都不为空
    for (int i = 0; i < mapping.length; i++) {
      if (mapping[i].length() == 0) {
        throw new IllegalArgumentException("Invalid rule '" + rule +
            "' with empty part, mapping rules must not contain empty parts!");
      }
    }

    return mapping;
  }

  /**
   * 创建所有规则类型通用的默认规则节点，设置公共默认字段
   * @param type 规则类型，可选user/group/application
   * @return 已设置公共默认字段的JSON节点
   */
  private ObjectNode createDefaultRuleNode(String type) {
    return objectMapper
        .createObjectNode()
        .put("type", type)
        // 所有旧版规则默认降级策略为放置到默认队列
        .put("fallbackResult", "placeDefault")
        // 所有旧版规则默认允许自动创建队列
        .put("create", true);
  }

  /**
   * 创建单条用户映射规则的JSON节点
   * @param match 规则匹配部分，可以是具体用户名或%user匹配所有用户
   * @param target 目标队列路径，支持动态变量%user、%primary_group、%secondary_group
   * @return 表示该规则的JSON节点
   */
  private ObjectNode createUserMappingRule(String match, String target) {
    ObjectNode ruleNode = createDefaultRuleNode("user");
    QueuePath targetPath = new QueuePath(target);

    // 旧版%user匹配全部用户替换为新版JSON格式的*通配符
    if (match.equals(MATCHER_USER)) {
      match = JSON_MATCH_ALL;
    }
    ruleNode.put(JSON_NODE_MATCHES, match);

    // 根据叶子节点名称判断放置策略
    switch (targetPath.getLeafName()) {
    case MAPPING_USER:
      // 叶子为%user，策略为直接放置到对应用户名的队列
      ruleNode.put(JSON_NODE_POLICY, "user");
      if (targetPath.hasParent()) {
        // 解析父队列路径，获取父路径的叶子节点
        QueuePath targetParentPath =
            new QueuePath(targetPath.getParent());
        String parentShortName = targetParentPath.getLeafName();

        if (parentShortName.equals(MAPPING_PRIMARY_GROUP)) {
          // 父节点叶子为%primary_group，对应策略为primaryGroupUser（主用户组+用户名）
          ruleNode.put(JSON_NODE_POLICY, "primaryGroupUser");

          // 移除路径中已经被策略处理的%primary_group，避免重复拼接
          targetPath = new QueuePath(targetParentPath.getParent(),
              targetPath.getLeafName());
        } else if (parentShortName.equals(MAPPING_SECONDARY_GROUP)) {
          // 父节点叶子为%secondary_group，对应策略为secondaryGroupUser（次用户组+用户名）
          ruleNode.put(JSON_NODE_POLICY, "secondaryGroupUser");

          // 移除路径中已经被策略处理的%secondary_group，避免重复拼接
          targetPath = new QueuePath(targetParentPath.getParent(),
              targetPath.getLeafName());
        }

        // 此处对应[parent].%user映射模式
      }
      break;
    case MAPPING_PRIMARY_GROUP:
      // 叶子为%primary_group，对应策略为primaryGroup（放置到主用户组同名队列）
      ruleNode.put(JSON_NODE_POLICY, "primaryGroup");
      break;
    case MAPPING_SECONDARY_GROUP:
      // 叶子为%secondary_group，对应策略为secondaryGroup（放置到次用户组同名队列）
      ruleNode.put(JSON_NODE_POLICY, "secondaryGroup");
      break;
    default:
      // 静态固定路径，使用custom策略直接指定完整路径
      ruleNode.put(JSON_NODE_POLICY, "custom");
      ruleNode.put(JSON_NODE_CUSTOM_PLACEMENT, targetPath.getFullPath());
      break;
    }

    // 如果目标路径存在父队列，添加parentQueue字段
    if (targetPath.hasParent()) {
      ruleNode.put(JSON_NODE_PARENT_QUEUE, targetPath.getParent());
    }

    return ruleNode;
  }

  /**
   * 创建单条用户组映射规则的JSON节点
   * @param match 待匹配的用户组名称
   * @param target 目标队列路径，支持动态变量%user
   * @return 表示该规则的JSON节点
   */
  private ObjectNode createGroupMappingRule(String match, String target) {
    ObjectNode ruleNode = createDefaultRuleNode("group");
    QueuePath targetPath = new QueuePath(target);

    // 直接复用原匹配值，格式兼容新版JSON
    ruleNode.put(JSON_NODE_MATCHES, match);

    if (targetPath.getLeafName().matches(MATCHER_USER)) {
      // 叶子为%user，对应策略user（放置到用户名同名队列）
      ruleNode.put(JSON_NODE_POLICY, "user");

      // 如果存在父队列，添加parentQueue字段
      if (targetPath.hasParent()) {
        ruleNode.put(JSON_NODE_PARENT_QUEUE, targetPath.getParent());
      }
    } else {
      // 静态固定路径，使用custom策略直接指定完整路径
      ruleNode.put(JSON_NODE_POLICY, "custom");
      ruleNode.put(JSON_NODE_CUSTOM_PLACEMENT, targetPath.getFullPath());
    }

    return ruleNode;
  }


  /**
   * 创建单条应用名称映射规则的JSON节点
   * @param match 待匹配的应用名称，或%application匹配所有应用
   * @param target 目标队列路径，支持动态变量%application
   * @return 表示该规则的JSON节点
   */
  private ObjectNode createApplicationNameMappingRule(
      String match, String target) {
    ObjectNode ruleNode = createDefaultRuleNode("application");
    QueuePath targetPath = new QueuePath(target);

    // 直接复用原匹配值，格式兼容新版JSON
    ruleNode.put(JSON_NODE_MATCHES, match);

    if (targetPath.getLeafName().matches(MATCHER_APPLICATION)) {
      // 叶子为%application，对应策略applicationName（放置到应用名同名队列）
      ruleNode.put(JSON_NODE_POLICY, "applicationName");

      // 如果存在父队列，添加parentQueue字段
      if (targetPath.hasParent()) {
        ruleNode.put(JSON_NODE_PARENT_QUEUE, targetPath.getParent());
      }
    } else {
      // 静态固定路径，使用custom策略直接指定完整路径
      ruleNode.put(JSON_NODE_POLICY, "custom");
      ruleNode.put(JSON_NODE_CUSTOM_PLACEMENT, targetPath.getFullPath());
    }

    return ruleNode;
  }
}