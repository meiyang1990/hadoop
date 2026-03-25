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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FairSharePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.FifoPolicy;


import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 公平调度器的调度策略抽象基类，用于计算队列的公平份额、稳定公平份额以及可用资源余量，
 * 决定同一队列内运行应用之间的资源分配方式。
 * <p>
 * 每个队列（包括父队列和子队列）都配置有独立的调度策略，子队列未指定时继承父队列策略。
 * 子队列策略必须与父队列策略兼容，部分组合不被允许，
 * 具体兼容规则见 {@link SchedulingPolicy#isChildPolicyAllowed(SchedulingPolicy)}。
 * 队列策略在公平调度器配置文件中通过<i>schedulingPolicy</i>属性指定，
 * 未指定时默认使用 {@link FairSharePolicy}。
 */
@Public
@Evolving
public abstract class SchedulingPolicy {
  // 策略实例缓存，保证每个策略类全局只有一个单例
  private static final ConcurrentHashMap<Class<? extends SchedulingPolicy>, SchedulingPolicy> instances =
      new ConcurrentHashMap<Class<? extends SchedulingPolicy>, SchedulingPolicy>();

  // 默认调度策略实例
  public static final SchedulingPolicy DEFAULT_POLICY =
      getInstance(FairSharePolicy.class);

  /**
   * 根据策略类获取单例实例，缓存已创建的实例保证全局唯一。
   *
   * @param clazz 继承SchedulingPolicy的策略类
   * @return 对应策略的单例实例
   */
  public static SchedulingPolicy getInstance(
      Class<? extends SchedulingPolicy> clazz) {
    SchedulingPolicy policy = ReflectionUtils.newInstance(clazz, null);
    SchedulingPolicy policyRet = instances.putIfAbsent(clazz, policy);
    if(policyRet != null) {
      return policyRet;
    }
    return policy;
  }

  /**
   * 从配置字符串解析出调度策略实例，支持别名和自定义全类名两种写法。
   * 别名："fair"对应FairSharePolicy、"fifo"对应FifoPolicy、"drf"对应DominantResourceFairnessPolicy；
   * 自定义策略填写完整类名即可，类必须在RM的类路径下。
   * 
   * @param policy 策略名称别名或自定义策略的完整类名
   * @return 解析得到的调度策略单例实例
   * @throws AllocationConfigurationException 解析出错（类不存在或不是SchedulingPolicy子类）时抛出
   *
   */
  @SuppressWarnings("unchecked")
  public static SchedulingPolicy parse(String policy)
      throws AllocationConfigurationException {
    @SuppressWarnings("rawtypes")
    Class clazz;
    // 转小写统一匹配格式
    String text = StringUtils.toLowerCase(policy);
    if (text.equalsIgnoreCase(FairSharePolicy.NAME)) {
      clazz = FairSharePolicy.class;
    } else if (text.equalsIgnoreCase(FifoPolicy.NAME)) {
      clazz = FifoPolicy.class;
    } else if (text.equalsIgnoreCase(DominantResourceFairnessPolicy.NAME)) {
      clazz = DominantResourceFairnessPolicy.class;
    } else {
      // 自定义策略尝试加载类
      try {
        clazz = Class.forName(policy);
      } catch (ClassNotFoundException cnfe) {
        throw new AllocationConfigurationException(policy
            + " SchedulingPolicy class not found!");
      }
    }
    // 检查类继承关系
    if (!SchedulingPolicy.class.isAssignableFrom(clazz)) {
      throw new AllocationConfigurationException(policy
          + " does not extend SchedulingPolicy");
    }
    return getInstance(clazz);
  }

  /**
   * 使用集群总资源初始化调度策略。
   * @deprecated 该方法无法跟踪集群资源变化，已被 {@link #initialize(FSContext)} 替代。
   *
   * @param clusterCapacity 集群总资源
   */
  @Deprecated
  public void initialize(Resource clusterCapacity) {}

  /**
   * 使用公平调度器上下文对象初始化调度策略，上下文包含集群资源等信息。
   *
   * @param fsContext 公平调度器上下文，持有集群资源等信息
   */
  public void initialize(FSContext fsContext) {}

  /**
   * 获取当前策略使用的资源计算器，所有资源计算都应使用该计算器。
   *
   * @return 当前策略的资源计算器实例
   */
  public abstract ResourceCalculator getResourceCalculator();

  /**
   * @return 获取当前调度策略的名称
   */
  public abstract String getName();

  /**
   * 获取队列内部可调度对象（应用/子队列）的排序比较器，用于调度前排序。
   * 
   * @return 排序使用的比较器
   */
  public abstract Comparator<Schedulable> getComparator();

  /**
   * 根据当前调度策略计算并更新所有可调度对象的瞬时公平份额，
   * 计算仅考虑正在运行应用的队列，结果用于后续调度决策。
   * 
   * @param schedulables 需要更新份额的可调度对象集合
   * @param totalResources 集群总资源
   */
  public abstract void computeShares(
      Collection<? extends Schedulable> schedulables, Resource totalResources);

  /**
   * 根据当前调度策略计算并更新所有队列的稳定公平份额，
   * 稳定份额不区分队列是否有运行应用，仅用于Web UI展示，不参与实际调度。
   *
   * @param queues 需要更新稳定份额的队列集合
   * @param totalResources 集群总资源
   */
  public abstract void computeSteadyShares(
      Collection<? extends FSQueue> queues, Resource totalResources);

  /**
   * 检查当前策略下资源使用量是否超过分配的公平份额。
   *
   * @param usage 实际资源使用量
   * @param fairShare 分配的公平份额
   * @return 使用量超过公平份额返回true，否则返回false
   */
  public abstract boolean checkIfUsageOverFairShare(
      Resource usage, Resource fairShare);

  /**
   * 计算队列的可用资源余量（headroom），计算逻辑为取集群可分配给当前队列的最大资源
   * 与（公平份额 - 当前已使用资源）每个维度资源的最小值，不支持的资源维度保持集群可用值不变。
   *
   * @param queueFairShare 队列的公平份额
   * @param queueUsage 队列当前已使用资源
   * @param maxAvailable 集群可分配给该队列的最大可用资源
   * @return 计算得到的队列可用资源余量
   */
  public abstract Resource getHeadroom(Resource queueFairShare,
      Resource queueUsage, Resource maxAvailable);

  /**
   * 检查当前父策略是否允许子队列使用指定的子策略。
   *
   * @param childPolicy 子队列拟使用的调度策略
   * @return 允许则返回true，否则返回false
   */
  public boolean isChildPolicyAllowed(SchedulingPolicy childPolicy) {
    return true;
  }
}