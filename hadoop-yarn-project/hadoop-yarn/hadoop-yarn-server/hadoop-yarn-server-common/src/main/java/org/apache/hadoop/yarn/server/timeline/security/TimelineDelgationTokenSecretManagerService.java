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

package org.apache.hadoop.yarn.server.timeline.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

/**
 * 为不同版本的时间线服务提供委托令牌密钥管理器服务的抽象基类
 * 封装了时间线服务委托令牌管理的通用逻辑，具体实现由子类完成
 */
public abstract class TimelineDelgationTokenSecretManagerService extends
    AbstractService {

  /**
   * 构造函数，初始化服务名称
   * @param name 服务名称
   */
  public TimelineDelgationTokenSecretManagerService(String name) {
    super(name);
  }

  // 过期委托令牌扫描间隔，默认1小时（单位：毫秒）
  private static long delegationTokenRemovalScanInterval = 3600000L;

  // 时间线服务委托令牌密钥管理器实例
  private AbstractDelegationTokenSecretManager
      <TimelineDelegationTokenIdentifier> secretManager = null;

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置读取密钥更新间隔，使用默认值兜底
    long secretKeyInterval =
        conf.getLong(YarnConfiguration.TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL,
            YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_KEY_UPDATE_INTERVAL);
    // 从配置读取令牌最大生命周期，使用默认值兜底
    long tokenMaxLifetime =
        conf.getLong(YarnConfiguration.TIMELINE_DELEGATION_TOKEN_MAX_LIFETIME,
            YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_TOKEN_MAX_LIFETIME);
    // 从配置读取令牌更新间隔，使用默认值兜底
    long tokenRenewInterval =
        conf.getLong(YarnConfiguration.TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL,
            YarnConfiguration.DEFAULT_TIMELINE_DELEGATION_TOKEN_RENEW_INTERVAL);
    // 调用子类方法创建具体的委托令牌密钥管理器实例
    secretManager = createTimelineDelegationTokenSecretManager(
        secretKeyInterval, tokenMaxLifetime, tokenRenewInterval,
        delegationTokenRemovalScanInterval);
    super.init(conf);
  }

  /**
   * 创建时间线服务委托令牌密钥管理器的抽象方法
   * 由不同版本的时间线服务提供具体实现
   * @param secretKeyInterval 密钥更新间隔
   * @param tokenMaxLifetime 令牌最大生命周期
   * @param tokenRenewInterval 令牌更新间隔
   * @param tokenRemovalScanInterval 过期令牌扫描间隔
   * @return 委托令牌密钥管理器实例
   */
  protected abstract
      AbstractDelegationTokenSecretManager<TimelineDelegationTokenIdentifier>
          createTimelineDelegationTokenSecretManager(long secretKeyInterval,
          long tokenMaxLifetime, long tokenRenewInterval,
          long tokenRemovalScanInterval);

  @Override
  protected void serviceStart() throws Exception {
    // 启动密钥管理器的后台线程
    secretManager.startThreads();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 停止密钥管理器的后台线程
    secretManager.stopThreads();
    super.stop();
  }

  /**
   * 获取当前服务持有的时间线委托令牌密钥管理器实例
   * @return 委托令牌密钥管理器实例
   */
  public AbstractDelegationTokenSecretManager
      <TimelineDelegationTokenIdentifier>
          getTimelineDelegationTokenSecretManager() {
    return secretManager;
  }
}