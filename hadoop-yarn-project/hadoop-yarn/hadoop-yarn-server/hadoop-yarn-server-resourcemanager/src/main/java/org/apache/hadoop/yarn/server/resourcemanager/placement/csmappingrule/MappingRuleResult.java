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

/**
 * 容量调度器应用放置规则匹配结果封装类，用于表示一条映射规则执行后的结果
 */
public final class MappingRuleResult {
  /**
   * 目标队列名称，仅当结果类型为PLACE时有效
   */
  private final String queue;

  /**
   * 标记如果目标队列不存在时是否允许自动创建，仅当结果类型为PLACE时有效
   */
  private boolean allowCreate = true;

  /**
   * 标准化后的队列完整路径，容量调度器允许用户仅引用叶子节点名称，需要标准化为完整路径
   */
  private String normalizedQueue;

  /**
   * 结果类型枚举
   */
  private MappingRuleResultType result;

  /**
   * 拒绝结果单例，重复使用该实例避免重复创建对象
   */
  private static final MappingRuleResult RESULT_REJECT
      = new MappingRuleResult(null, MappingRuleResultType.REJECT);

  /**
   * 跳过结果单例，重复使用该实例避免重复创建对象
   */
  private static final MappingRuleResult RESULT_SKIP
      = new MappingRuleResult(null, MappingRuleResultType.SKIP);

  /**
   * 默认放置结果单例，重复使用该实例避免重复创建对象
   */
  private static final MappingRuleResult RESULT_DEFAULT_PLACEMENT
      = new MappingRuleResult(null, MappingRuleResultType.PLACE_TO_DEFAULT);

  /**
   * 私有构造方法，强制使用工厂方法创建实例避免状态不一致
   * @param queue 应用要放置到的队列名称，仅结果类型为PLACE时有效，否则为null
   * @param result 结果类型
   */
  private MappingRuleResult(String queue, MappingRuleResultType result) {
    this.queue = queue;
    this.normalizedQueue = queue;
    this.result = result;
  }

  /**
   * 私有构造方法，强制使用工厂方法创建实例避免状态不一致
   * @param queue 应用要放置到的队列名称，仅结果类型为PLACE时有效，否则为null
   * @param result 结果类型
   * @param allowCreate 标记不存在时是否允许创建目标队列
   */
  private MappingRuleResult(
      String queue, MappingRuleResultType result, boolean allowCreate) {
    this.queue = queue;
    this.normalizedQueue = queue;
    this.result = result;
    this.allowCreate = allowCreate;
  }

  /**
   * 获取目标队列名称，仅结果类型为PLACE时有意义
   * @return 目标队列名称
   */
  public String getQueue() {
    return queue;
  }

  /**
   * 获取是否允许创建不存在的目标队列
   * @return true表示允许创建不存在的队列
   */
  public boolean isCreateAllowed() {
    return allowCreate;
  }

  /**
   * 更新标准化后的队列完整路径，该类本身不负责标准化，仅提供存储能力
   * @param normalizedQueueName 标准化后的队列完整路径
   */
  public void updateNormalizedQueue(String normalizedQueueName) {
    this.normalizedQueue = normalizedQueueName;
  }

  /**
   * 获取标准化后的队列完整路径，仅结果类型为PLACE时有意义。
   * 标准化名称需要外部设置，本类仅提供存储能力
   * @return 标准化后的队列完整路径
   */
  public String getNormalizedQueue() {
    return normalizedQueue;
  }

  /**
   * 获取结果类型
   * @return 结果类型枚举值
   */
  public MappingRuleResultType getResult() {
    return result;
  }

  /**
   * 创建放置到指定队列的结果对象
   * @param queue 目标队列名称
   * @param allowCreate 是否允许创建不存在的目标队列
   * @return 映射规则结果对象
   */
  public static MappingRuleResult createPlacementResult(
      String queue, boolean allowCreate) {
    return new MappingRuleResult(
        queue, MappingRuleResultType.PLACE, allowCreate);
  }

  /**
   * 获取拒绝应用提交的结果单例
   * @return 拒绝结果单例
   */
  public static MappingRuleResult createRejectResult() {
    return RESULT_REJECT;
  }

  /**
   * 获取跳过当前规则的结果单例，继续匹配下一条规则
   * @return 跳过结果单例
   */
  public static MappingRuleResult createSkipResult() {
    return RESULT_SKIP;
  }

  /**
   * 获取使用默认放置策略的结果单例，使用默认队列放置应用
   * @return 默认放置结果单例
   */
  public static MappingRuleResult createDefaultPlacementResult() {
    return RESULT_DEFAULT_PLACEMENT;
  }

  @Override
  public String toString() {
    if (result == MappingRuleResultType.PLACE) {
      return result.name() + ": '" + normalizedQueue + "' ('" + queue + "')";
    } else {
      return result.name();
    }
  }
}