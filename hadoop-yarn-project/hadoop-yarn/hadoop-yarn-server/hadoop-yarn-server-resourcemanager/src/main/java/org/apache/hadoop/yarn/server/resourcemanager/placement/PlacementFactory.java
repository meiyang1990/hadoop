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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 文件级说明：应用队列放置规则工厂，YARN容量调度器中负责根据配置创建不同类型的应用放置规则实例，
 * 用于将提交的应用分配到对应的队列中。
 * Factory class for creating instances of {@link PlacementRule}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class PlacementFactory {

  /** 日志记录器 */
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementFactory.class);

  private PlacementFactory() {
    // Unused.
  }

  /**
   * 根据类名字符串创建放置规则实例，调度器在预解析阶段未预先加载类时使用该方法。
   * Create a new {@link PlacementRule} based on the rule class from the
   * configuration. This is used to instantiate rules by the scheduler which
   * does not resolve the class before this call.
   * @param ruleStr 要实例化的规则类全限定名
   * @param conf 规则使用的配置对象
   * @return 创建完成的规则实例
   * @throws ClassNotFoundException
   * no definition for the class with the specified name could be found.
   */
  public static PlacementRule getPlacementRule(String ruleStr,
      Configuration conf)
      throws ClassNotFoundException {
    // 加载类并转换为PlacementRule子类
    Class<? extends PlacementRule> ruleClass = Class.forName(ruleStr)
        .asSubclass(PlacementRule.class);
    LOG.info("Using PlacementRule implementation - " + ruleClass);
    // 通过反射工具创建实例并传入配置
    return ReflectionUtils.newInstance(ruleClass, conf);
  }

  /**
   * 根据已加载的类对象创建放置规则实例，调度器预先解析完类后使用该方法。
   * Create a new {@link PlacementRule} based on the rule class from the
   * configuration. This is used to instantiate rules by the scheduler which
   * resolve the class before this call.
   * @param ruleClass 已加载的规则类引用
   * @param initArg 初始化配置参数
   * @return 创建完成的规则实例
   */
  public static PlacementRule getPlacementRule(
      Class<? extends PlacementRule> ruleClass, Object initArg) {
    LOG.info("Creating PlacementRule implementation: " + ruleClass);
    // 反射创建规则实例
    PlacementRule rule = ReflectionUtils.newInstance(ruleClass, null);
    // 为规则设置配置参数
    rule.setConfig(initArg);
    return rule;
  }
}