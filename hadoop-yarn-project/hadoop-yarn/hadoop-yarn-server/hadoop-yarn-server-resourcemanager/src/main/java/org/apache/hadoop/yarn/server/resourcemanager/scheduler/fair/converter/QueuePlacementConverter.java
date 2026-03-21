// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.placement.DefaultPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.FSPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PrimaryGroupPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.RejectPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SecondaryGroupExistingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SpecifiedPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.MappingRulesDescription;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.FallbackResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Policy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.placement.schema.Rule.Type;

/**
 * 公平调度器队列放置规则转换器，将公平调度器的放置规则转换为容量调度器的映射规则格式
 * 用于公平调度器配置向容量调度器配置的迁移转换流程
 */
class QueuePlacementConverter {
  private static final FallbackResult SKIP_RESULT = FallbackResult.SKIP;
  private static final String DEFAULT_QUEUE = "root.default";
  private static final String MATCH_ALL_USER = "*";
  // 需要将root作为父队列的策略集合
  private static final Set<Policy> NEED_ROOT_PARENT = Sets.newHashSet(
      Policy.USER,
      Policy.PRIMARY_GROUP,
      Policy.PRIMARY_GROUP_USER,
      Policy.SECONDARY_GROUP,
      Policy.SECONDARY_GROUP_USER);

  /**
   * 将公平调度器放置策略转换为容量调度器映射规则描述
   * @param placementManager 公平调度器放置规则管理器
   * @param ruleHandler 转换规则处理器，用于处理警告和冲突检查
   * @param convertedCSconfig 转换后的容量调度器配置
   * @param usePercentages 是否使用百分比资源模式
   * @return 容量调度器格式的映射规则描述
   */
  MappingRulesDescription convertPlacementPolicy(
      PlacementManager placementManager,
      FSConfigToCSConfigRuleHandler ruleHandler,
      CapacitySchedulerConfiguration convertedCSconfig,
      boolean usePercentages) {

    MappingRulesDescription desc = new MappingRulesDescription();
    List<Rule> rules = new ArrayList<>();

    // 遍历公平调度器的所有放置规则逐个转换
    for (final PlacementRule fsRule : placementManager.getPlacementRules()) {
      boolean create = ((FSPlacementRule)fsRule).getCreateFlag();

      if (fsRule instanceof UserPlacementRule) {
        UserPlacementRule userRule = (UserPlacementRule) fsRule;

        // 处理嵌套规则（父规则+用户子规则）
        if (userRule.getParentRule() != null) {
          handleNestedRule(rules,
              userRule,
              ruleHandler,
              create,
              convertedCSconfig,
              usePercentages);
        } else {
          // 直接创建用户放置规则
          rules.add(createRule(Policy.USER, create, ruleHandler,
              usePercentages));
        }
      } else if (fsRule instanceof SpecifiedPlacementRule) {
        // 转换用户指定队列放置规则
        rules.add(createRule(Policy.SPECIFIED, create, ruleHandler,
            usePercentages));
      } else if (fsRule instanceof PrimaryGroupPlacementRule) {
        // 转换主用户组放置规则
        rules.add(createRule(Policy.PRIMARY_GROUP, create, ruleHandler,
            usePercentages));
      } else if (fsRule instanceof DefaultPlacementRule) {
        // 转换默认队列放置规则
        DefaultPlacementRule defaultRule = (DefaultPlacementRule) fsRule;
        String defaultQueueName = defaultRule.defaultQueueName;

        Rule rule;
        if (DEFAULT_QUEUE.equals(defaultQueueName)) {
          // 使用默认队列策略
          rule = createRule(Policy.DEFAULT_QUEUE, create, ruleHandler,
              usePercentages);
        } else {
          // 使用自定义默认队列
          rule = createRule(Policy.CUSTOM, create, ruleHandler,
              usePercentages);
          rule.setCustomPlacement(defaultQueueName);
        }

        rules.add(rule);
      } else if (fsRule instanceof SecondaryGroupExistingPlacementRule) {
        // 转换辅助用户组放置规则
        Rule rule = createRule(Policy.SECONDARY_GROUP, create, ruleHandler,
            usePercentages);
        rules.add(rule);
      } else if (fsRule instanceof RejectPlacementRule) {
        // 转换拒绝放置规则
        rules.add(createRule(Policy.REJECT, false, ruleHandler,
            usePercentages));
      } else {
        throw new IllegalArgumentException("Unknown placement rule: " + fsRule);
      }
    }

    desc.setRules(rules);

    return desc;
  }

  /**
   * 处理嵌套放置规则（父规则+用户子规则结构）
   * @param rules 转换后的规则列表
   * @param userRule 用户放置规则
   * @param ruleHandler 转换规则处理器
   * @param create 是否允许自动创建队列
   * @param csConf 容量调度器配置
   * @param usePercentages 是否使用百分比资源模式
   */
  private void handleNestedRule(List<Rule> rules,
      UserPlacementRule userRule,
      FSConfigToCSConfigRuleHandler ruleHandler,
      boolean create,
      CapacitySchedulerConfiguration csConf,
      boolean usePercentages) {
    PlacementRule parentRule = userRule.getParentRule();
    boolean parentCreate = ((FSPlacementRule) parentRule).getCreateFlag();
    Policy policy;
    QueuePath queueName = null;

    if (parentRule instanceof PrimaryGroupPlacementRule) {
      // 主用户组下创建对应用户队列
      policy = Policy.PRIMARY_GROUP_USER;
    } else if (parentRule instanceof SecondaryGroupExistingPlacementRule) {
      // 辅助用户组下创建对应用户队列
      policy = Policy.SECONDARY_GROUP_USER;
    } else if (parentRule instanceof DefaultPlacementRule) {
      // 指定默认队列作为父队列创建用户队列
      DefaultPlacementRule defaultRule = (DefaultPlacementRule) parentRule;
      policy = Policy.USER;
      queueName = new QueuePath(defaultRule.defaultQueueName);
    } else {
      throw new IllegalArgumentException(
          "Unsupported parent nested rule: "
          + parentRule.getClass().getCanonicalName());
    }

    // 创建嵌套规则并添加到结果列表
    Rule rule = createNestedRule(policy,
        create,
        ruleHandler,
        parentCreate,
        queueName,
        csConf,
        usePercentages);
    rules.add(rule);
  }

  /**
   * 创建基础容量调度器映射规则
   * @param policy 放置策略
   * @param create 是否允许自动创建队列
   * @param ruleHandler 转换规则处理器
   * @param usePercentages 是否使用百分比资源模式
   * @return 创建完成的映射规则
   */
  private Rule createRule(Policy policy, boolean create,
      FSConfigToCSConfigRuleHandler ruleHandler, boolean usePercentages) {
    Rule rule = new Rule();
    rule.setPolicy(policy);
    rule.setCreate(create);
    rule.setMatches(MATCH_ALL_USER);
    rule.setFallbackResult(SKIP_RESULT);
    rule.setType(Type.USER);

    // 百分比模式下，自动创建根下队列不被支持，需要发出警告
    if (usePercentages && create) {
      if (policy == Policy.PRIMARY_GROUP
          || policy == Policy.PRIMARY_GROUP_USER) {
        ruleHandler.handleRuleAutoCreateFlag("root.<primaryGroup>");
      } else if (policy == Policy.SECONDARY_GROUP
          || policy == Policy.SECONDARY_GROUP_USER) {
        ruleHandler.handleRuleAutoCreateFlag("root.<secondaryGroup>");
      }
    }

    // 非百分比（权重）模式下，需要设置父队列为root
    // 百分比模式不设置，避免根下自动创建导致验证失败
    if (!usePercentages &&
        NEED_ROOT_PARENT.contains(policy)) {
      rule.setParentQueue("root");
    }

    return rule;
  }

  /**
   * 创建嵌套结构的容量调度器映射规则
   * @param policy 放置策略
   * @param create 子规则是否允许自动创建
   * @param ruleHandler 转换规则处理器
   * @param fsParentCreate 父规则是否允许自动创建
   * @param parentQueue 父队列路径（可为null）
   * @param csConf 容量调度器配置
   * @param usePercentages 是否使用百分比资源模式
   * @return 创建完成的嵌套映射规则
   */
  private Rule createNestedRule(Policy policy,
      boolean create,
      FSConfigToCSConfigRuleHandler ruleHandler,
      boolean fsParentCreate,
      QueuePath parentQueue,
      CapacitySchedulerConfiguration csConf,
      boolean usePercentages) {

    Rule rule = createRule(policy, create, ruleHandler, usePercentages);

    // 如果指定了自定义父队列，覆盖默认的root父队列
    if (parentQueue != null) {
      rule.setParentQueue(parentQueue.getFullPath());
    }

    if (usePercentages) {
      // 百分比模式下不支持父规则自动创建，需要发出警告
      if (fsParentCreate) {
        if (policy == Policy.PRIMARY_GROUP_USER) {
          ruleHandler.handleFSParentCreateFlag("root.<primaryGroup>");
        } else if (policy == Policy.SECONDARY_GROUP_USER) {
          ruleHandler.handleFSParentCreateFlag("root.<secondaryGroup>");
        } else {
          ruleHandler.handleFSParentCreateFlag(parentQueue.getFullPath());
        }
      }

      // 检查父队列下是否已存在静态队列，静态动态混排需要发出警告
      if (create && policy == Policy.USER) {
        ruleHandler.handleRuleAutoCreateFlag(parentQueue.getFullPath());
        checkStaticDynamicConflict(parentQueue, csConf, ruleHandler);
      }
    } else {
      // 权重模式下，父或子任意一方允许创建则最终允许创建
      rule.setCreate(fsParentCreate || create);

      // 父和子创建标识不一致时，发出警告告知当前不支持该配置
      if (fsParentCreate ^ create) {
        ruleHandler.handleFSParentAndChildCreateFlagDiff(policy);
      }
    }

    return rule;
  }

  /**
   * 检查父队列下是否同时存在静态配置队列和动态自动创建队列，冲突则发出警告
   * @param parentPath 父队列路径
   * @param csConf 容量调度器配置
   * @param ruleHandler 转换规则处理器
   */
  private void checkStaticDynamicConflict(QueuePath parentPath,
      CapacitySchedulerConfiguration csConf,
      FSConfigToCSConfigRuleHandler ruleHandler) {
    List<String> childQueues = csConf.getQueues(parentPath);

    // 父队列下已有静态子队列，同时又配置了动态创建，发出警告
    if (childQueues != null && childQueues.size() > 0) {
      ruleHandler.handleChildStaticDynamicConflict(parentPath.getFullPath());
    }
  }
}