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

package org.apache.hadoop.yarn.server.federation.failover;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.client.AMRMClientUtils;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.RMFailoverProxyProvider;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN联邦环境下的代理提供者工具类，为指定协议创建支持联邦感知的代理对象。
 * 核心能力是创建能从联邦状态存储获取当前活跃ResourceManager信息的故障切换代理。
 */
@Private
@Unstable
public final class FederationProxyProviderUtil {

  /** 日志记录器 */
  public static final Logger LOG =
      LoggerFactory.getLogger(FederationProxyProviderUtil.class);

  // 禁止实例化工具类
  private FederationProxyProviderUtil() {
  }

  /**
   * 在联邦环境中为指定子集群创建ResourceManager代理对象。
   * 非HA模式下直接连接指定地址，HA模式下自动处理ResourceManager故障切换。
   *
   * @param configuration 生成代理使用的配置对象
   * @param protocol 代理需要实现的协议接口
   * @param subClusterId 目标子集群的唯一标识
   * @param user 创建代理所代表的用户身份
   * @param <T> 代理对象的类型
   * @return 目标ResourceManager的代理对象
   * @throws IOException 创建代理失败时抛出异常
   */
  @Public
  @Unstable
  public static <T> T createRMProxy(Configuration configuration,
      Class<T> protocol, SubClusterId subClusterId, UserGroupInformation user)
      throws IOException {
    return createRMProxy(configuration, protocol, subClusterId, user, null);
  }

  /**
   * 在联邦环境中为指定子集群创建带认证令牌的ResourceManager代理对象。
   * 非HA模式下直接连接指定地址，HA模式下自动处理ResourceManager故障切换。
   *
   * @param configuration 生成代理使用的配置对象
   * @param protocol 代理需要实现的协议接口
   * @param subClusterId 目标子集群的唯一标识
   * @param user 创建代理所代表的用户身份
   * @param token 连接使用的认证令牌
   * @param <T> 代理对象的类型
   * @return 目标ResourceManager的代理对象
   * @throws IOException 创建代理失败时抛出异常
   */
  @Public
  @Unstable
  public static <T> T createRMProxy(Configuration configuration,
      final Class<T> protocol, SubClusterId subClusterId,
      UserGroupInformation user, Token<? extends TokenIdentifier> token)
      throws IOException {
    // 基于传入配置创建YarnConfiguration实例
    final YarnConfiguration config = new YarnConfiguration(configuration);
    // 更新配置，适配联邦环境下指定子集群的连接需求
    updateConfForFederation(config, subClusterId.getId());
    // 调用通用工具创建RM代理对象
    return AMRMClientUtils.createRMProxy(config, protocol, user, token);
  }

  /**
   * 更新配置对象，适配联邦环境下访问指定子集群的需求。
   * 核心修改：替换故障切换代理为联邦感知实现，适配传统HA场景切换。
   *
   * @param conf 需要修改的配置对象
   * @param subClusterId 目标子集群ID
   */
  public static void updateConfForFederation(Configuration conf,
      String subClusterId) {
    // 设置目标子集群ID到配置
    conf.set(YarnConfiguration.RM_CLUSTER_ID, subClusterId);
    /*
     * In a Federation setting, we will connect to not just the local cluster RM
     * but also multiple external RMs. The membership information of all the RMs
     * that are currently participating in Federation is available in the
     * central FederationStateStore. So we will: 1. obtain the RM service
     * addresses from FederationStateStore using the
     * FederationRMFailoverProxyProvider. 2. disable traditional HA as that
     * depends on local configuration lookup for RMs using indexes. 3. we will
     * enable federation failover IF traditional HA is enabled so that the
     * appropriate failover RetryPolicy is initialized.
     */
    // 标记启用联邦模式
    conf.setBoolean(YarnConfiguration.FEDERATION_ENABLED, true);
    // 设置故障切换代理为联邦感知实现
    conf.setClass(YarnConfiguration.CLIENT_FAILOVER_PROXY_PROVIDER,
        FederationRMFailoverProxyProvider.class, RMFailoverProxyProvider.class);
    // 如果原配置开启了传统HA，适配切换为联邦故障切换模式
    if (HAUtil.isHAEnabled(conf)) {
      // 启用联邦故障切换能力，初始化对应重试策略
      conf.setBoolean(YarnConfiguration.FEDERATION_FAILOVER_ENABLED, true);
      // 关闭传统RM HA模式，避免从本地配置读取RM地址
      conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, false);
    }
  }

}