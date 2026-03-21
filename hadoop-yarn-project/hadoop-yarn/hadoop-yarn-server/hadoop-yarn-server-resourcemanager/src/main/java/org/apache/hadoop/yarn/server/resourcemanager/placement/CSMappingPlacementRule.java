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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.csmappingrule.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * 容量调度器映射规则应用放置器，根据配置的规则集将提交的应用分配到对应队列。
 * 整合了原UserGroupMappingPlacementRule和AppNameMappingPlacementRule的全部功能，
 * 同时添加了公平调度器放置策略的特性，缩小两种调度器的功能差距。
 */
public class CSMappingPlacementRule extends PlacementRule {
  private static final Logger LOG = LoggerFactory
      .getLogger(CSMappingPlacementRule.class);
  private static final String DOT = ".";
  private static final String DOT_REPLACEMENT = "_dot_";

  private CapacitySchedulerQueueManager queueManager;
  private List<MappingRule> mappingRules;

  /**
   * 具有特殊含义的不可变变量集合，每个变量上下文的值固定不变。
   */
  private ImmutableSet<String> immutableVariables = ImmutableSet.of(
      "%user",
      "%primary_group",
      "%secondary_group",
      "%application",
      "%specified"
      );

  private Groups groups;
  private boolean overrideWithQueueMappings;
  private boolean failOnConfigError = true;

  @VisibleForTesting
  public void setGroups(Groups groups) {
    this.groups = groups;
  }

  @VisibleForTesting
  public void setFailOnConfigError(boolean failOnConfigError) {
    this.failOnConfigError = failOnConfigError;
  }

  /**
   * 构建映射规则验证上下文，注册系统内置变量。
   * @return 初始化完成的验证上下文
   * @throws IOException 初始化失败时抛出异常
   */
  private MappingRuleValidationContext buildValidationContext()
      throws IOException {
    Preconditions.checkNotNull(queueManager, "Queue manager must be " +
        "initialized before building validation a context!");

    MappingRuleValidationContext validationContext =
        new MappingRuleValidationContextImpl(queueManager);

    // 将所有不可变系统变量添加到已知变量列表
    for (String var : immutableVariables) {
      try {
        validationContext.addImmutableVariable(var);
      } catch (YarnException e) {
        LOG.error("Error initializing placement variables, unable to register" +
            " '{}': {}", var, e.getMessage());
        throw new IOException(e);
      }
    }
    // 不可变变量 + %default 是官方仅支持的系统变量
    // 使用这些变量初始化上下文，允许自定义规则扩展变量列表
    try {
      validationContext.addVariable("%default");
    } catch (YarnException e) {
      LOG.error("Error initializing placement variables, unable to register" +
          " '%default': " + e.getMessage());
      throw new IOException(e);
    }

    return validationContext;
  }

  @Override
  public boolean initialize(ResourceScheduler scheduler) throws IOException {
    if (!(scheduler instanceof CapacityScheduler)) {
      throw new IOException(
        "CSMappingPlacementRule can be only used with CapacityScheduler");
    }
    LOG.info("Initializing {} queue mapping manager.",
        getClass().getSimpleName());

    CapacitySchedulerContext csContext = (CapacitySchedulerContext) scheduler;
    queueManager = csContext.getCapacitySchedulerQueueManager();

    CapacitySchedulerConfiguration conf = csContext.getConfiguration();
    overrideWithQueueMappings = conf.getOverrideWithQueueMappings();

    if (groups == null) {
      groups = Groups.getUserToGroupsMappingService(csContext.getConf());
    }

    MappingRuleValidationContext validationContext = buildValidationContext();

    // 获取并验证所有映射规则
    mappingRules = conf.getMappingRules();
    for (MappingRule rule : mappingRules) {
      try {
        rule.validate(validationContext);
      } catch (YarnException e) {
        LOG.error("Error initializing queue mappings, rule '{}' " +
            "has encountered a validation error: {}", rule, e.getMessage());
        if (failOnConfigError) {
          throw new IOException(e);
        }
      }
    }

    LOG.info("Initialized queue mappings, can override user specified " +
        "queues: {}  number of rules: {} mapping rules: {}",
        overrideWithQueueMappings, mappingRules.size(), mappingRules);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Initialized with the following mapping rules:");
      mappingRules.forEach(rule -> LOG.debug(rule.toString()));
    }

    return mappingRules.size() > 0;
  }

  /**
   * 为变量上下文设置用户组相关数据。
   * 主组是getGroups返回的第一个组；
   * 遍历所有其他组寻找存在对应队列的组作为次组；
   * 同时为组匹配添加完整组集合数据集。
   * @param vctx 待更新的变量上下文
   * @param user 用户名
   * @throws IOException 获取用户组信息失败时抛出
   */
  private void setupGroupsForVariableContext(VariableContext vctx, String user)
      throws IOException {
    if (groups == null) {
      LOG.warn(
          "Group provider hasn't been set, cannot query groups for user {}",
          user);
      // 强制设置为空字符串而非null，避免被识别为未定义变量，最终生成'%primary_group'队列名
      vctx.put("%primary_group", "");
      vctx.put("%secondary_group", "");
      return;
    }
    Set<String> groupsSet = groups.getGroupsSet(user);
    if (groupsSet.isEmpty()) {
      LOG.warn("There are no groups for user {}", user);
      vctx.putExtraDataset("groups", groupsSet);
      return;
    }
    Iterator<String> it = groupsSet.iterator();
    String primaryGroup = cleanName(it.next());

    ArrayList<String> secondaryGroupList = new ArrayList<>();

    while (it.hasNext()) {
      String groupName = cleanName(it.next());
      secondaryGroupList.add(groupName);
    }

    if (secondaryGroupList.size() == 0) {
      // 没有次组时直接注册为空值，加快规则评估
      vctx.put("%secondary_group", "");
      if (LOG.isDebugEnabled()) {
        LOG.debug("User {} does not have any potential Secondary group", user);
      }
    } else {
      // 有次组时注册条件变量，规则评估时会依次尝试匹配存在的队列
      vctx.putConditional(
          MappingRuleConditionalVariables.SecondaryGroupVariable.VARIABLE_NAME,
          new MappingRuleConditionalVariables.SecondaryGroupVariable(
              this.queueManager,
              secondaryGroupList
              ));
    }

    vctx.put("%primary_group", primaryGroup);
    vctx.putExtraDataset("groups", groupsSet);
  }

  /**
   * 创建应用放置变量上下文，填充所有系统变量的值。
   * @param asc 应用提交上下文
   * @param user 提交用户
   * @return 填充完成的变量上下文
   */
  private VariableContext createVariableContext(
      ApplicationSubmissionContext asc, String user) {
    VariableContext vctx = new VariableContext();

    String cleanedName = cleanName(user);
    if (!user.equals(cleanedName)) {
      vctx.putOriginal("%user", user);
    }
    vctx.put("%user", cleanedName);
    // 如果指定队列等于default，说明用户未指定队列，ClientRMService会在无队列时设置为default
    // 用户如果需要显式放置到default队列，必须使用root.default
    if (!asc.getQueue().equals(YarnConfiguration.DEFAULT_QUEUE_NAME)) {
      vctx.put("%specified", asc.getQueue());
    } else {
      // 未指定队列时设置为空，避免匹配到名为%specified的队列
      // 路径验证会拒绝空路径，最终会命中规则的回退操作
      vctx.put("%specified", "");
    }

    vctx.put("%application", asc.getApplicationName());
    vctx.put("%default", "root.default");
    try {
      setupGroupsForVariableContext(vctx, user);
    } catch (IOException e) {
      LOG.warn("Unable to setup groups: {}", e.getMessage());
    }

    vctx.setImmutables(immutableVariables);
    return vctx;
  }

  /**
   * 验证并规范化队列路径，检查队列是否存在/允许创建，确保目标是叶子队列。
   * @param queueName 待验证队列名
   * @param allowCreate 是否允许自动创建队列
   * @return 规范化后的完整队列路径
   * @throws YarnException 验证失败时抛出异常
   */
  private String validateAndNormalizeQueue(
      String queueName, boolean allowCreate) throws YarnException {
    QueuePath path = new QueuePath(queueName);

    if (path.hasEmptyPart()) {
      throw new YarnException("Invalid path returned by rule: '" +
          queueName + "'");
    }

    String leaf = path.getLeafName();
    String parent = path.getParent();

    String normalizedName;
    if (parent != null) {
      normalizedName = validateAndNormalizeQueueWithParent(
          parent, leaf, allowCreate);
    } else {
      normalizedName = validateAndNormalizeQueueWithNoParent(leaf);
    }

    CSQueue queue = queueManager.getQueueByFullName(normalizedName);
    if (queue != null && !(queue instanceof AbstractLeafQueue)) {
      throw new YarnException("Mapping rule returned a non-leaf queue '" +
          normalizedName + "', cannot place application in it.");
    }

    return normalizedName;
  }

  /**
   * 验证带父队列的目标队列，处理自动创建场景。
   * @param parent 父队列路径
   * @param leaf 叶子队列名
   * @param allowCreate 是否允许自动创建
   * @return 规范化后的完整队列路径
   * @throws YarnException 验证失败时抛出异常
   */
  private String validateAndNormalizeQueueWithParent(
      String parent, String leaf, boolean allowCreate) throws YarnException {
    String normalizedPath =
        MappingRuleValidationHelper.normalizeQueuePathRoot(
            queueManager, parent + DOT + leaf);
    MappingRuleValidationHelper.ValidationResult validity =
        MappingRuleValidationHelper.validateQueuePathAutoCreation(
            queueManager, normalizedPath);

    switch (validity) {
    case AMBIGUOUS_PARENT:
      throw new YarnException("Mapping rule specified a parent queue '" +
          parent + "', but it is ambiguous.");
    case AMBIGUOUS_QUEUE:
      throw new YarnException("Mapping rule specified a target queue '" +
          normalizedPath + "', but it is ambiguous.");
    case EMPTY_PATH:
      throw new YarnException("Mapping rule did not specify a target queue.");
    case NO_PARENT_PROVIDED:
      throw new YarnException("Mapping rule did not specify an existing queue" +
          " nor a dynamic parent queue.");
    case NO_DYNAMIC_PARENT:
      throw new YarnException("Mapping rule specified a parent queue '" +
          parent + "', but it is not a dynamic parent queue, " +
          "and no queue exists with name '" + leaf + "' under it.");
    case QUEUE_EXISTS:
      break;
    case CREATABLE:
      if (!allowCreate) {
        throw new YarnException("Mapping rule doesn't allow auto-creation of " +
            "the queue '" + normalizedPath + "'.");
      }
      break;
    default:
      // 可能是新增了未处理的验证结果
      throw new YarnException("Unknown queue path validation result. '" +
          validity + "'.");
    }

    // 此时要么队列已存在，要么父队列允许动态创建，直接返回规范化路径
    return normalizedPath;
  }

  /**
   * 验证无父队列的目标队列，要求队列必须已存在。
   * @param leaf 叶子队列名
   * @return 规范化后的完整队列路径
   * @throws YarnException 队列不存在或不明确时抛出异常
   */
  private String validateAndNormalizeQueueWithNoParent(String leaf)
      throws YarnException {
    // 未指定父队列时要求队列必须已存在，否则映射无效
    CSQueue queue = queueManager.getQueue(leaf);
    if (queue == null) {
      if (queueManager.isAmbiguous(leaf)) {
        throw new YarnException("Queue '" + leaf + "' specified in mapping" +
            " rule is ambiguous");
      } else {
        throw new YarnException("Queue '" + leaf + "' specified in mapping" +
            " rule does not exist.");
      }
    }

    // 规范化队列路径
    return queue.getQueuePath();
  }

  /**
   * 评估单个映射规则，处理验证失败时回退到规则的回退策略。
   * @param rule 待评估的映射规则
   * @param variables 变量上下文
   * @return 评估结果
   */
  private MappingRuleResult evaluateRule(
      MappingRule rule, VariableContext variables) {
    MappingRuleResult result = rule.evaluate(variables);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Evaluated rule '{}' with result: '{}'", rule, result);
    }

    if (result.getResult() == MappingRuleResultType.PLACE) {
      try {
        result.updateNormalizedQueue(validateAndNormalizeQueue(
            result.getQueue(), result.isCreateAllowed()));
      } catch (Exception e) {
        result = rule.getFallback();
        LOG.info("Cannot place to queue '{}' returned by mapping rule. " +
            "Reason: '{}' Fallback operation: '{}'",
            result.getQueue(), e.getMessage(), result);
      }
    }

    return result;
  }

  /**
   * 根据完整队列路径创建应用放置上下文，拆分父队列和叶子队列。
   * @param queueName 完整队列路径
   * @return 应用放置上下文
   */
  private ApplicationPlacementContext createPlacementContext(String queueName) {
    int parentQueueNameEndIndex = queueName.lastIndexOf(DOT);
    if (parentQueueNameEndIndex > -1) {
      String parent = queueName.substring(0, parentQueueNameEndIndex).trim();
      String leaf = queueName.substring(parentQueueNameEndIndex + 1).trim();
      return new ApplicationPlacementContext(leaf, parent);
    }

    // 仅为未来扩展性和一致性保留此分支
    // 当前没有不带父队列的有效叶子队列，所有路径都会被规范化为root.xxx格式
    // 只有root本身没有父队列，但root不是叶子队列，不会被用于放置应用
    return new ApplicationPlacementContext(queueName);
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    return getPlacementForApp(asc, user, false);
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user, boolean recovery)
        throws YarnException {
    // 仅在以下情况使用映射规则：
    // 1. 开启了覆盖用户指定队列配置
    // 2. 应用提交到了default队列（说明用户未指定队列）
    // 3. 应用恢复场景
    String appQueue = asc.getQueue();
    LOG.debug("Looking placement for app '{}' originally submitted to queue " +
        "'{}', with override enabled '{}'",
        asc.getApplicationName(), appQueue, overrideWithQueueMappings);
    if (appQueue != null &&
        !appQueue.equals(YarnConfiguration.DEFAULT_QUEUE_NAME) &&