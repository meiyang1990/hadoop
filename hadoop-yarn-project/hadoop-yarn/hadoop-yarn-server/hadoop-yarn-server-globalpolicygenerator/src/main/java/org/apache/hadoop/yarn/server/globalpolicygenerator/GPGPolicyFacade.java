// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * 全局策略生成器(GPG)的策略门面工具类，负责将策略读写到联邦状态存储，封装了策略构造、初始化和序列化逻辑。
 * 
 * 仅对外暴露两个核心方法:
 *
 * {@link #getPolicyManager(String)}
 * 根据队列名获取对应的策略管理器。如果指定队列未配置策略则返回null。
 * 获取到的策略管理器可用于提取{@link FederationRouterPolicy}和{@link FederationAMRMProxyPolicy}
 * 以及其他策略相关参数。
 *
 * {@link #setPolicyManager(FederationPolicyManager)}
 * 设置策略管理器。如果策略配置未发生变化则不执行写入，否则更新本地缓存并将新配置写入联邦状态存储。
 *
 * 本类假设GPG是唯一写入策略的服务，因此仅在第一次获取队列策略时从联邦状态存储读取，
 * 之后GPG仅向联邦状态存储写入策略。
 *
 * 本类维护策略管理器缓存和子集群策略配置缓存，主要作用是提供读取缓存，
 * 并识别策略是否发生变更，避免不必要的联邦状态存储写入操作。
 */

public class GPGPolicyFacade {

  private static final Logger LOG =
      LoggerFactory.getLogger(GPGPolicyFacade.class);

  // 联邦状态存储门面，用于读写策略配置
  private FederationStateStoreFacade stateStore;

  // 队列名 -> 策略管理器 缓存
  private Map<String, FederationPolicyManager> policyManagerMap;
  // 队列名 -> 子集群策略配置 缓存
  private Map<String, SubClusterPolicyConfiguration> policyConfMap;

  // 是否为只读模式，只读模式下不会写入状态存储
  private boolean readOnly;

  /**
   * 构造GPG策略门面。
   * @param stateStore 联邦状态存储门面
   * @param conf Hadoop配置
   */
  public GPGPolicyFacade(FederationStateStoreFacade stateStore,
      Configuration conf) {
    this.stateStore = stateStore;
    this.policyManagerMap = new HashMap<>();
    this.policyConfMap = new HashMap<>();
    this.readOnly =
        conf.getBoolean(YarnConfiguration.GPG_POLICY_GENERATOR_READONLY,
            YarnConfiguration.DEFAULT_GPG_POLICY_GENERATOR_READONLY);
  }

  /**
   * 从联邦状态存储读取指定队列的策略管理器。
   * 由于GPG是唯一更新策略的组件，本实现不需要重复初始化策略。
   *
   * @param queueName 目标队列名称
   * @return 对应队列的策略管理器，如果不存在则返回null
   * @throws YarnException YARN服务异常
   */
  public FederationPolicyManager getPolicyManager(String queueName)
      throws YarnException {
    FederationPolicyManager policyManager = policyManagerMap.get(queueName);

    // 如果缓存中不存在策略管理器，从联邦状态存储拉取配置创建并缓存
    if (policyManager == null) {
      try {

        // 如果缓存中没有配置，从状态存储拉取
        SubClusterPolicyConfiguration conf = policyConfMap.get(queueName);

        if (conf == null) {
          conf = stateStore.getPolicyConfiguration(queueName);
        }

        // 如果配置仍为null，说明联邦状态存储中不存在该队列策略
        if (conf == null) {
          LOG.info("Read null policy for queue {}.", queueName);
          return null;
        }

        // 根据策略管理器类型实例化对象
        String policyManagerType = conf.getType();
        policyManager = FederationPolicyUtils.instantiatePolicyManager(policyManagerType);
        policyManager.setQueue(queueName);

        // 如果策略管理器支持权重策略信息，需要反序列化并设置参数，用于路由和容器分配
        if (policyManager.isSupportWeightedPolicyInfo()) {
          ByteBuffer weightedPolicyInfoParams = conf.getParams();
          if (weightedPolicyInfoParams == null) {
            LOG.warn("Warning: Queue = {}, FederationPolicyManager {} WeightedPolicyInfo is empty.",
                queueName, policyManagerType);
            return null;
          }
          WeightedPolicyInfo weightedPolicyInfo =
              WeightedPolicyInfo.fromByteBuffer(conf.getParams());
          policyManager.setWeightedPolicyInfo(weightedPolicyInfo);
        } else {
          LOG.warn("Warning: FederationPolicyManager of unsupported WeightedPolicyInfo type {}, " +
              "initialization may be incomplete.", policyManager.getClass());
        }

        // 更新缓存
        policyManagerMap.put(queueName, policyManager);
        policyConfMap.put(queueName, conf);
      } catch (YarnException e) {
        LOG.error("Error reading SubClusterPolicyConfiguration from state "
            + "store for queue: {}", queueName);
        throw e;
      }
    }
    return policyManager;
  }

  /**
   * 将策略管理器写入联邦状态存储。门面会维护缓存，仅当策略配置变更时才写入。
   *
   * @param policyManager 要更新的策略管理器，包含策略信息和目标队列名
   * @throws YarnException YARN服务异常
   */
  public void setPolicyManager(FederationPolicyManager policyManager)
      throws YarnException {
    if (policyManager == null) {
      LOG.warn("Attempting to set null policy manager");
      return;
    }
    // 从策略管理器中提取配置
    String queue = policyManager.getQueue();
    SubClusterPolicyConfiguration conf;
    try {
      conf = policyManager.serializeConf();
    } catch (FederationPolicyInitializationException e) {
      LOG.warn("Error serializing policy for queue {}", queue);
      throw e;
    }
    if (conf == null) {
      // 状态存储当前不支持将策略设置为null，因为需要从策略中读取队列名
      LOG.warn("Skip setting policy to null for queue {} into state store",
          queue);
      return;
    }
    // 与缓存配置比较，如果不同则写入存储并更新缓存
    if (!confCacheEqual(queue, conf)) {
      try {
        if (readOnly) {
          LOG.info("[read-only] Skipping policy update for queue {}", queue);
          return;
        }
        LOG.info("Updating policy for queue {} into state store", queue);
        stateStore.setPolicyConfiguration(conf);
        policyConfMap.put(queue, conf);
        policyManagerMap.put(queue, policyManager);
      } catch (YarnException e) {
        LOG.warn("Error writing SubClusterPolicyConfiguration to state "
            + "store for queue: {}", queue);
        throw e;
      }
    } else {
      LOG.info("Setting unchanged policy - state store write skipped");
    }
  }

  /**
   * 检查新配置与缓存配置是否一致。
   * @param queue 目标队列名
   * @param conf 新的策略配置
   * @return 配置是否相等
   */
  private boolean confCacheEqual(String queue,
      SubClusterPolicyConfiguration conf) {
    SubClusterPolicyConfiguration cachedConf = policyConfMap.get(queue);
    if (conf == null && cachedConf == null) {
      return true;
    } else if (conf != null && cachedConf != null) {
      if (conf.equals(cachedConf)) {
        return true;
      }
    }
    return false;
  }
}