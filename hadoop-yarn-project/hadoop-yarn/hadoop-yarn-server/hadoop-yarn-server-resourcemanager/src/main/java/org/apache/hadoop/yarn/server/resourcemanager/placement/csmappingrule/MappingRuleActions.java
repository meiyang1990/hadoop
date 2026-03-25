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
 * YARN容量调度器队列映射规则动作工厂类，定义了所有映射规则支持的动作类型，并提供创建动作实例的工具方法。
 * 该类属于YARN ResourceManager容量调度器应用 placement 模块，负责处理应用提交后队列映射的动作定义。
 */
public final class MappingRuleActions {
  /** 默认队列变量名，用于表示使用默认队列 */
  public static final String DEFAULT_QUEUE_VARIABLE = "%default";

  /**
   * 工具类，隐藏构造方法。
   */
  private MappingRuleActions() {}

  /**
   * 放置到指定队列动作，将应用放置到匹配的目标队列，支持队列名称模式变量替换。
   */
  public static class PlaceToQueueAction extends MappingRuleActionBase {
    /** 目标队列名称模式，可包含需要替换的变量占位符 */
    private String queuePattern;

    /** 标识如果目标队列不存在，是否允许自动创建 */
    private boolean allowCreate;

    /**
     * 构造方法。
     * @param queuePattern 应用放置的目标队列模式，可包含变量，例如 root.%primary_group.%user
     * @param allowCreate 是否允许在目标队列不存在时自动创建
     */
    PlaceToQueueAction(String queuePattern, boolean allowCreate) {
      this.allowCreate = allowCreate;
      this.queuePattern = queuePattern == null ? "" : queuePattern;
    }

    /**
     * 执行放置动作：替换队列模式中的变量为上下文实际值，返回最终放置结果。
     *
     * @param variables 变量上下文，包含所有可替换的变量值
     * @return 放置动作结果，包含最终队列名称
     */
    @Override
    public MappingRuleResult execute(VariableContext variables) {
        String substituted = variables.replacePathVariables(queuePattern);
        return MappingRuleResult.createPlacementResult(
            substituted, allowCreate);
    }

    /**
     * 验证队列路径模式的合法性，委托验证上下文进行队列结构校验。
     * @param ctx 验证上下文，包含队列结构信息和验证辅助方法
     * @throws YarnException 验证失败时抛出异常
     */
    @Override
    public void validate(MappingRuleValidationContext ctx)
        throws YarnException {
      ctx.validateQueuePath(this.queuePattern);
    }

    @Override
    public String toString() {
      return "PlaceToQueueAction{" +
          "queueName='" + queuePattern + "'," +
          "allowCreate=" + allowCreate +
          "}";
    }
  }

  /**
   * 拒绝应用提交动作，当规则匹配时拒绝当前应用的提交。
   */
  public static class RejectAction extends MappingRuleActionBase {
    /**
     * 执行拒绝动作，无条件返回拒绝结果。
     * @param variables 变量上下文
     * @return 固定返回拒绝结果
     */
    @Override
    public MappingRuleResult execute(VariableContext variables) {
      return MappingRuleResult.createRejectResult();
    }

    /**
     * 拒绝动作本身总是合法，无需额外验证，提供空实现。
     * @param ctx 验证上下文
     * @throws YarnException 验证失败时抛出异常
     */
    @Override
    public void validate(MappingRuleValidationContext ctx) throws
        YarnException {}

    @Override
    public String toString() {
      return "RejectAction";
    }
  }

  /**
   * 更新变量上下文动作，修改变量上下文中的可变变量值，不直接改变应用放置结果，
   * 用于后续规则中使用修改后的变量，例如修改默认队列或定义自定义变量。
   */
  public static class VariableUpdateAction extends MappingRuleActionBase {
    /** 需要更新的变量全名，例如 %custom */
    private final String variableName;
    /** 变量的新值模式，可包含其他变量，执行时会先解析替换 */
    private final String variableValue;

    /**
     * 构造方法。
     * @param variableName 需要更新的变量名称
     * @param variableValue 变量的新值模式
     */
    VariableUpdateAction(String variableName, String variableValue) {
      this.variableName = variableName;
      this.variableValue = variableValue;
    }

    /**
     * 执行变量更新：先解析替换值模式中的变量，然后更新变量上下文，
     * 更新完成后返回跳过结果，让规则匹配继续执行后续规则。
     * @param variables 变量上下文
     * @return 固定返回跳过结果，继续后续规则匹配
     */
    @Override
    public MappingRuleResult execute(VariableContext variables) {
      variables.put(variableName, variables.replaceVariables(variableValue));
      return MappingRuleResult.createSkipResult();
    }

    /**
     * 验证变量更新动作：将当前变量注册到验证上下文，如果变量不可修改则抛出异常。
     * @param ctx 验证上下文
     * @throws YarnException 变量无法添加时抛出异常（例如变量已被定义为不可变）
     */
    @Override
    public void validate(MappingRuleValidationContext ctx)
        throws YarnException {
      ctx.addVariable(this.variableName);
    }

    @Override
    public String toString() {
      return "VariableUpdateAction{" +
          "variableName='" + variableName + '\'' +
          ", variableValue='" + variableValue + '\'' +
          '}';
    }
  }

  /**
   * 创建更新默认队列变量的动作，修改默认队列的值。
   * @param queue 新的默认队列名称
   * @return 变量更新动作实例，执行时会修改默认队列变量
   */
  public static MappingRuleAction createUpdateDefaultAction(String queue) {
    return new VariableUpdateAction(DEFAULT_QUEUE_VARIABLE, queue);
  }

  /**
   * 创建放置应用到指定队列的动作。
   * @param queue 目标队列名称模式
   * @param allowCreate 是否允许自动创建不存在的队列
   * @return 放置到队列动作实例
   */
  public static MappingRuleAction createPlaceToQueueAction(
      String queue, boolean allowCreate) {
    return new PlaceToQueueAction(queue, allowCreate);
  }

  /**
   * 创建放置应用到默认队列的动作。
   * @return 放置到默认队列动作实例
   */
  public static MappingRuleAction createPlaceToDefaultAction() {
    return createPlaceToQueueAction(DEFAULT_QUEUE_VARIABLE, false);
  }

  /**
   * 创建拒绝应用提交的动作。
   * @return 拒绝动作实例，执行时会拒绝应用提交
   */
  public static MappingRuleAction createRejectAction() {
    return new RejectAction();
  }
}