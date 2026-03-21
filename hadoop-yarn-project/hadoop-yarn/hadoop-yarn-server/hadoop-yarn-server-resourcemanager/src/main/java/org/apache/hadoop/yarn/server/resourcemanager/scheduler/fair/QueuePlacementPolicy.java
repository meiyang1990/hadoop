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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.placement.DefaultPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.FSPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PrimaryGroupPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.RejectPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SecondaryGroupExistingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.SpecifiedPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserPlacementRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementFactory.getPlacementRule;

/**
 * 公平调度器基于规则的应用队列放置策略实现。
 * 它解析配置并生成有序的{@link PlacementRule}规则列表，更新PlacementManager用于应用队列分配。
 */
@Private
@Unstable
final class QueuePlacementPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueuePlacementPolicy.class);

  // 用于简化规则名称到实现类和终止状态映射的内部辅助类
  private static final class RuleMap {
    private final Class<? extends PlacementRule> ruleClass;
    private final String terminal;

    private RuleMap(Class<? extends PlacementRule> clazz, String terminate) {
      this.ruleClass = clazz;
      this.terminal = terminate;
    }
  }

  // 已知规则映射表：key是配置中使用的规则名称，
  // value包含规则实现类和终止状态配置标识
  private static final Map<String, RuleMap> RULES;
  static {
    Map<String, RuleMap> map = new HashMap<>();
    map.put("user", new RuleMap(UserPlacementRule.class, "create"));
    map.put("primaryGroup",
        new RuleMap(PrimaryGroupPlacementRule.class, "create"));
    map.put("secondaryGroupExistingQueue",
        new RuleMap(SecondaryGroupExistingPlacementRule.class, "false"));
    map.put("specified", new RuleMap(SpecifiedPlacementRule.class, "false"));
    map.put("nestedUserQueue", new RuleMap(UserPlacementRule.class, "create"));
    map.put("default", new RuleMap(DefaultPlacementRule.class, "create"));
    map.put("reject", new RuleMap(RejectPlacementRule.class, "true"));
    RULES = Collections.unmodifiableMap(map);
  }

  private QueuePlacementPolicy() {
  }

  /**
   * 验证并更新调度器PlacementManager中的规则集合。
   * @param newRules 要设置的新规则列表
   * @param newTerminalState 对应规则的终止状态列表
   * @param fs 公平调度器引用，用于规则初始化
   * @throws AllocationConfigurationException 配置错误时抛出
   */
  private static void updateRuleSet(List<PlacementRule> newRules,
                                    List<Boolean> newTerminalState,
                                    FairScheduler fs)
      throws AllocationConfigurationException {
    if (newRules.isEmpty()) {
      LOG.debug("Empty rule set defined, ignoring update");
      return;
    }
    LOG.debug("Placement rule order check");
    // 遍历检查所有规则，确保终止规则后没有不可达规则
    for (int i = 0; i < newTerminalState.size()-1; i++) {
      if (newTerminalState.get(i)) {
        String errorMsg = "Rules after rule "
            + (i+1) + " in queue placement policy can never be reached";
        if (fs.isNoTerminalRuleCheck()) {
          LOG.warn(errorMsg);
        } else {
          throw new AllocationConfigurationException(errorMsg);
        }
      }
    }
    // 必须保证最后一条规则是终止规则，避免分配结束后没有队列
    if (!newTerminalState.get(newTerminalState.size()-1)) {
      throw new AllocationConfigurationException(
          "Could get past last queue placement rule without assigning");
    }
    // 初始化所有规则，注入调度器引用
    LOG.debug("Initialising new rule set");
    try {
      for (PlacementRule rule: newRules){
        rule.initialize(fs);
      }
    } catch (IOException ioe) {
      // We should never throw as we pass in a FS object, however we still
      // should consider any exception here a config error.
      throw new AllocationConfigurationException(
          "Rule initialisation failed with exception", ioe);
    }
    // 所有规则验证通过，更新PlacementManager中的规则列表
    // We only get here when all rules are OK.
    fs.getRMContext().getQueuePlacementManager().updateRules(newRules);
    LOG.debug("PlacementManager active with new rule set");
  }

  /**
   * 从XML配置元素解析并构建队列放置策略。
   * @param confElement 公平调度器分配配置中的放置策略XML片段
   * @param fs 公平调度器引用，用于规则初始化
   * @throws AllocationConfigurationException 配置错误时抛出
   */
  static void fromXml(Element confElement, FairScheduler fs)
      throws AllocationConfigurationException {
    LOG.debug("Reloading placement policy from allocation config");
    if (confElement == null || !confElement.hasChildNodes()) {
      throw new AllocationConfigurationException(
          "Empty configuration for QueuePlacementPolicy is not allowed");
    }
    List<PlacementRule> newRules = new ArrayList<>();
    List<Boolean> newTerminalState = new ArrayList<>();
    NodeList elements = confElement.getChildNodes();
    // 遍历所有子节点，处理每个rule配置
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element &&
          node.getNodeName().equalsIgnoreCase("rule")) {
        String name = ((Element) node).getAttribute("name");
        LOG.debug("Creating new rule: {}", name);
        // 根据XML节点创建规则实例
        PlacementRule rule = createRule((Element)node);

        // 获取嵌套的父规则定义（仅用于嵌套用户队列场景）
        PlacementRule parentRule = null;
        String parentName = null;
        Element child = getParentRuleElement(node);
        if (child != null) {
          parentName = child.getAttribute("name");
          parentRule = getParentRule(child, fs);
        }
        // 向后兼容性检查：nestedUserQueue必须配置父规则
        if (name.equalsIgnoreCase("nestedUserQueue") && parentRule == null) {
          throw new AllocationConfigurationException("Rule '" + name
              + "' must have a parent rule set");
        }
        newRules.add(rule);
        // 计算规则终止状态
        if (parentRule == null) {
          newTerminalState.add(
              getTerminal(RULES.get(name).terminal, rule));
        } else {
          ((FSPlacementRule)rule).setParentRule(parentRule);
          newTerminalState.add(
              getTerminal(RULES.get(name).terminal, rule) &&
              getTerminal(RULES.get(parentName).terminal, parentRule));
        }
      }
    }
    updateRuleSet(newRules, newTerminalState, fs);
  }

  /**
   * 从当前规则节点中查找嵌套定义的父规则节点。
   * @param node 当前规则XML节点
   * @return 父规则元素，没有则返回null
   * @throws AllocationConfigurationException 配置错误时抛出
   */
  private static Element getParentRuleElement(Node node)
      throws AllocationConfigurationException {
    Element parent = null;
    // 遍历查找子节点中的rule节点
    if (node.hasChildNodes()) {
      NodeList childList = node.getChildNodes();
      for (int j = 0; j < childList.getLength(); j++) {
        Node child = childList.item(j);
        if (child instanceof Element &&
            child.getNodeName().equalsIgnoreCase("rule")) {
          // 允许多个配置但只使用最后一个，输出警告
          if (parent != null) {
            LOG.warn("Rule '{}' has multiple parent rules defined, only the " +
                "last parent rule will be used",
                ((Element) node).getAttribute("name"));
          }
          parent = ((Element) child);
        }
      }
    }
    // 验证父规则合法性：reject和nestedUserQueue不能作为父规则
    if (parent != null) {
      String parentName = parent.getAttribute("name");
      if (parentName.equals("reject") ||
          parentName.equals("nestedUserQueue")) {
        throw new AllocationConfigurationException("Rule '"
            + parentName
            + "' is not allowed as a parent rule for any rule");
      }
    }
    return parent;
  }

  /**
   * 根据XML配置创建并初始化父规则实例。
   * @param parent 父规则XML元素
   * @param fs 公平调度器引用，用于规则初始化
   * @return 初始化完成的父规则实例
   * @throws AllocationConfigurationException 配置错误时抛出
   */
  private static PlacementRule getParentRule(Element parent,
                                             FairScheduler fs)
      throws AllocationConfigurationException {
    LOG.debug("Creating new parent rule: {}", parent.getAttribute("name"));
    PlacementRule parentRule = createRule(parent);
    // 直接初始化父规则，不加入顶层规则列表
    try {
      parentRule.initialize(fs);
    } catch (IOException ioe) {
      // We should never throw as we pass in a FS object, however we
      // still should consider any exception here a config error.
      throw new AllocationConfigurationException(
          "Parent Rule initialisation failed with exception", ioe);
    }
    return parentRule;
  }

  /**
   * 根据规则配置和创建标志计算规则是否为终止规则。
   * 终止规则表示匹配后不会继续执行后续规则。
   * @param terminal 终止状态配置值（true/false/create）
   * @param rule 规则实例
   * @return true表示是终止规则，否则false
   */
  private static Boolean getTerminal(String terminal, PlacementRule rule) {
    switch (terminal) {
    case "true":    // 始终是终止规则
      return true;
    case "false":   // 始终不是终止规则
      return false;
    default:        // 根据规则的创建标志决定：允许创建队列则为终止
      return ((FSPlacementRule)rule).getCreateFlag();
    }
  }

  /**
   * 根据XML元素创建规则实例。
   * @param element 规则XML元素
   * @return 创建完成的规则实例
   * @throws AllocationConfigurationException 配置错误时抛出
   */
  @SuppressWarnings("unchecked")
  private static PlacementRule createRule(Element element)
      throws AllocationConfigurationException {

    String ruleName = element.getAttribute("name");
    if ("".equals(ruleName)) {
      throw new AllocationConfigurationException("No name provided for a "
          + "rule element");
    }

    Class<? extends PlacementRule> ruleClass = null;
    if (RULES.containsKey(ruleName)) {
      ruleClass = RULES.get(ruleName).ruleClass;
    }
    if (ruleClass == null) {
      throw new AllocationConfigurationException("No rule class found for "
          + ruleName);
    }
    return getPlacementRule(ruleClass, element);
  }
    
  /**
   * 根据传统FairScheduler配置选项构建默认队列放置策略。
   * 兼容不使用规则配置的旧版本配置格式。
   * @param fs 公平调度器引用，用于规则初始化
   */
  static void fromConfiguration(FairScheduler fs) {
    LOG.debug("Creating base placement policy from config");
    Configuration conf = fs.getConfig();

    boolean create = conf.getBoolean(
        FairSchedulerConfiguration.ALLOW_UNDECLARED_POOLS,
        FairSchedulerConfiguration.DEFAULT_ALLOW_UNDECLARED_POOLS);
    boolean userAsDefaultQueue = conf.getBoolean(
        FairSchedulerConfiguration.USER_AS_DEFAULT_QUEUE,
        FairSchedulerConfiguration.DEFAULT_USER_AS_DEFAULT_QUEUE);
    List<PlacementRule> newRules = new ArrayList<>();
    List<Boolean> newTerminalState = new ArrayList<>();
    Class<? extends PlacementRule> clazz =
        RULES.get("specified").ruleClass;
    newRules.add(getPlacementRule(clazz, create));
    newTerminalState.add(false);
    if (userAsDefaultQueue) {
      clazz = RULES.get("user").ruleClass;
      newRules.add(getPlacementRule(clazz, create));
      newTerminalState.add(create);
    }
    if (!userAsDefaultQueue || !create) {
      clazz = RULES.get("default").ruleClass;
      newRules.add(getPlacementRule(clazz, true));
      newTerminalState.add(true);
    }
    try {
      updateRuleSet(newRules, newTerminalState, fs);
    } catch (AllocationConfigurationException ex) {
      throw new RuntimeException("Should never hit exception when loading" +
          "placement policy from conf", ex);
    }
  }
}