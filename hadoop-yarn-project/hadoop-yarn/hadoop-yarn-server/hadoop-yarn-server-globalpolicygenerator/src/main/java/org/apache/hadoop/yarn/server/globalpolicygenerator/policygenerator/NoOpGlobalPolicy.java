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
package org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator;

import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import java.util.Map;

/**
 * 空操作全局策略实现，不更新任何策略配置，用作默认实现。
 */
public class NoOpGlobalPolicy extends GlobalPolicy{

  /**
   * 空实现的策略更新方法，不执行任何实际更新操作。
   * @param queueName 队列名称
   * @param clusterInfo 子集群信息映射表
   * @param manager 原联邦策略管理器实例
   * @return 始终返回null，表示不修改原有策略
   */
  @Override
  public FederationPolicyManager updatePolicy(String queueName,
      Map<SubClusterId, Map<Class, Object>> clusterInfo,
      FederationPolicyManager manager) {
    return null;
  }
}