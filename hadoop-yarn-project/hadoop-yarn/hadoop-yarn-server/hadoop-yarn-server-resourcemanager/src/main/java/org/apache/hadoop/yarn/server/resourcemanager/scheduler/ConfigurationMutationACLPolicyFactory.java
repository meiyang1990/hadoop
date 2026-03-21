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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * 配置修改ACL策略工厂类，用于创建 {@link ConfigurationMutationACLPolicy} 实例。
 * 负责从YARN配置中加载指定的ACL策略实现类并实例化。
 */
public final class ConfigurationMutationACLPolicyFactory {

  /** 日志记录器 */
  private static final Logger LOG = LoggerFactory.getLogger(
      ConfigurationMutationACLPolicyFactory.class);

  private ConfigurationMutationACLPolicyFactory() {
    // Unused.
  }

  /**
   * 根据YARN配置创建并返回配置修改ACL策略实例。
   * @param conf YARN配置对象
   * @return 配置修改ACL策略实例
   */
  public static ConfigurationMutationACLPolicy getPolicy(Configuration conf) {
    // 从配置中读取ACL策略实现类，默认使用DefaultConfigurationMutationACLPolicy
    Class<? extends ConfigurationMutationACLPolicy> policyClass =
        conf.getClass(YarnConfiguration.RM_SCHEDULER_MUTATION_ACL_POLICY_CLASS,
            DefaultConfigurationMutationACLPolicy.class,
            ConfigurationMutationACLPolicy.class);
    LOG.info("Using ConfigurationMutationACLPolicy implementation - " +
        policyClass);
    // 通过反射实例化策略对象
    return ReflectionUtils.newInstance(policyClass, conf);
  }
}