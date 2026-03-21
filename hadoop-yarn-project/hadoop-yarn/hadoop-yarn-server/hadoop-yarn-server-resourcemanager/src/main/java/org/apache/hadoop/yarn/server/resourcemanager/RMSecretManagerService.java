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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * YARN ResourceManager 密钥管理器服务，统一管理RM中各类安全令牌的生成、轮换与生命周期
 * 负责创建并初始化所有类型的令牌密钥管理器，并注册到RM上下文供全局使用
 */
public class RMSecretManagerService extends AbstractService {

  // AM-RM 交互令牌密钥管理器
  AMRMTokenSecretManager amRmTokenSecretManager;
  // NodeManager 令牌密钥管理器
  NMTokenSecretManagerInRM nmTokenSecretManager;
  // 客户端到ApplicationMaster 令牌密钥管理器
  ClientToAMTokenSecretManagerInRM clientToAMSecretManager;
  // Container 令牌密钥管理器
  RMContainerTokenSecretManager containerTokenSecretManager;
  // RM 委托令牌密钥管理器，用于跨节点身份认证
  RMDelegationTokenSecretManager rmDTSecretManager;

  // RM上下文引用
  RMContextImpl rmContext;

  /**
   * 构造RM密钥管理器服务，创建各类令牌管理器并注册到RM上下文
   * @param conf YARN配置
   * @param rmContext ResourceManager上下文
   */
  public RMSecretManagerService(Configuration conf, RMContextImpl rmContext) {
    super(RMSecretManagerService.class.getName());
    this.rmContext = rmContext;

    // To initialize correctly, these managers should be created before
    // being called serviceInit().
    // 创建NM令牌管理器
    nmTokenSecretManager = createNMTokenSecretManager(conf);
    // 注册到RM上下文
    rmContext.setNMTokenSecretManager(nmTokenSecretManager);

    // 创建Container令牌管理器
    containerTokenSecretManager = createContainerTokenSecretManager(conf);
    // 注册到RM上下文
    rmContext.setContainerTokenSecretManager(containerTokenSecretManager);

    // 创建客户端到AM令牌管理器
    clientToAMSecretManager = createClientToAMTokenSecretManager();
    // 注册到RM上下文
    rmContext.setClientToAMTokenSecretManager(clientToAMSecretManager);

    // 创建AM-RM交互令牌管理器
    amRmTokenSecretManager = createAMRMTokenSecretManager(conf, this.rmContext);
    // 注册到RM上下文
    rmContext.setAMRMTokenSecretManager(amRmTokenSecretManager);

    // 创建RM委托令牌管理器
    rmDTSecretManager =
        createRMDelegationTokenSecretManager(conf, rmContext);
    // 注册到RM上下文
    rmContext.setRMDelegationTokenSecretManager(rmDTSecretManager);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    // 启动AM-RM令牌管理器
    amRmTokenSecretManager.start();
    // 启动Container令牌管理器
    containerTokenSecretManager.start();
    // 启动NM令牌管理器
    nmTokenSecretManager.start();

    try {
      // 启动RM委托令牌管理器后台线程
      rmDTSecretManager.startThreads();
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to start secret manager threads", ie);
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    // 停止RM委托令牌管理器线程
    if (rmDTSecretManager != null) {
      rmDTSecretManager.stopThreads();
    }
    // 停止AM-RM令牌管理器
    if (amRmTokenSecretManager != null) {
      amRmTokenSecretManager.stop();
    }
    // 停止Container令牌管理器
    if (containerTokenSecretManager != null) {
      containerTokenSecretManager.stop();
    }
    // 停止NM令牌管理器
    if(nmTokenSecretManager != null) {
      nmTokenSecretManager.stop();
    }
    super.serviceStop();
  }

  /**
   * 创建Container令牌密钥管理器
   * @param conf 配置
   * @return Container令牌密钥管理器实例
   */
  protected RMContainerTokenSecretManager createContainerTokenSecretManager(
      Configuration conf) {
    return new RMContainerTokenSecretManager(conf);
  }

  /**
   * 创建NodeManager令牌密钥管理器
   * @param conf 配置
   * @return NM令牌密钥管理器实例
   */
  protected NMTokenSecretManagerInRM createNMTokenSecretManager(
      Configuration conf) {
    return new NMTokenSecretManagerInRM(conf);
  }

  /**
   * 创建AM-RM交互令牌密钥管理器
   * @param conf 配置
   * @param rmContext RM上下文
   * @return AM-RM令牌密钥管理器实例
   */
  protected AMRMTokenSecretManager createAMRMTokenSecretManager(
      Configuration conf, RMContext rmContext) {
    return new AMRMTokenSecretManager(conf, rmContext);
  }

  /**
   * 创建客户端到AM令牌密钥管理器
   * @return 客户端到AM令牌密钥管理器实例
   */
  protected ClientToAMTokenSecretManagerInRM createClientToAMTokenSecretManager() {
    return new ClientToAMTokenSecretManagerInRM();
  }

  @VisibleForTesting
  /**
   * 创建RM委托令牌密钥管理器，从配置读取各类生命周期参数
   * @param conf 配置
   * @param rmContext RM上下文
   * @return RM委托令牌密钥管理器实例
   */
  protected RMDelegationTokenSecretManager createRMDelegationTokenSecretManager(
      Configuration conf, RMContext rmContext) {
    // 获取密钥更新间隔
    long secretKeyInterval =
        conf.getLong(YarnConfiguration.RM_DELEGATION_KEY_UPDATE_INTERVAL_KEY,
            YarnConfiguration.RM_DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    // 获取令牌最大存活时间
    long tokenMaxLifetime =
        conf.getLong(YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            YarnConfiguration.RM_DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    // 获取令牌续订间隔
    long tokenRenewInterval =
        conf.getLong(YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
            YarnConfiguration.RM_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
    // 获取过期令牌扫描间隔
    long removeScanInterval =
        conf.getTimeDuration(YarnConfiguration.RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_KEY,
        YarnConfiguration.RM_DELEGATION_TOKEN_REMOVE_SCAN_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    return new RMDelegationTokenSecretManager(secretKeyInterval,
        tokenMaxLifetime, tokenRenewInterval, removeScanInterval, rmContext);
  }

}