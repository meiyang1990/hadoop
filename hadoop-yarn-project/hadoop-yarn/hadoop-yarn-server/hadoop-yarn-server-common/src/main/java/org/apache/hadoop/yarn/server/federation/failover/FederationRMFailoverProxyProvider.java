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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.client.RMFailoverProxyProvider;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 联邦场景下的RM故障转移代理提供者实现，基于联邦状态存储获取目标RM地址，
 * 支持HA和普通模式，可通过配置开关控制。
 */
@Private
@Unstable
public class FederationRMFailoverProxyProvider<T>
    implements RMFailoverProxyProvider<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRMFailoverProxyProvider.class);

  private RMProxy<T> rmProxy;
  private Class<T> protocol;
  private T current;
  private YarnConfiguration conf;
  private FederationStateStoreFacade facade;
  private SubClusterId subClusterId;
  private UserGroupInformation originalUser;
  private boolean federationFailoverEnabled;
  private boolean flushFacadeCacheForYarnRMAddr;

  @Override
  public void init(Configuration configuration, RMProxy<T> proxy,
      Class<T> proto) {
    this.rmProxy = proxy;
    this.protocol = proto;
    this.rmProxy.checkAllowedProtocols(this.protocol);
    // 从配置获取当前子集群ID
    String clusterId = configuration.get(YarnConfiguration.RM_CLUSTER_ID);
    Preconditions.checkNotNull(clusterId, "Missing RM ClusterId");
    this.subClusterId = SubClusterId.newInstance(clusterId);
    // 获取联邦状态存储门面实例
    this.facade = FederationStateStoreFacade.getInstance(configuration);
    if (configuration instanceof YarnConfiguration) {
      this.conf = (YarnConfiguration) configuration;
    }
    // 读取联邦故障转移功能开关配置
    federationFailoverEnabled =
        conf.getBoolean(YarnConfiguration.FEDERATION_FAILOVER_ENABLED,
            YarnConfiguration.DEFAULT_FEDERATION_FAILOVER_ENABLED);
    // 读取故障转移时是否刷新RM地址缓存配置
    flushFacadeCacheForYarnRMAddr =
        conf.getBoolean(YarnConfiguration.FEDERATION_FLUSH_CACHE_FOR_RM_ADDR,
            YarnConfiguration.DEFAULT_FEDERATION_FLUSH_CACHE_FOR_RM_ADDR);

    // 设置IPC客户端最大连接重试次数
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES));

    // 设置socket超时情况下的最大连接重试次数
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(
            YarnConfiguration.CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS));

    // 保存请求发起用户信息，用于后续代理创建
    try {
      this.originalUser = UserGroupInformation.getCurrentUser();
      LOG.info("Initialized Federation proxy for user: {}",
          this.originalUser.getUserName());
    } catch (IOException e) {
      LOG.warn("Could not get information of requester, ignoring for now.");
      this.originalUser = null;
    }

  }

  /**
   * 根据指定RM地址创建RM代理对象，仅供测试使用。
   * @param rmAddress RM服务地址
   * @return 新建的RM代理对象
   * @throws IOException 创建失败抛出IO异常
   */
  @VisibleForTesting
  protected T createRMProxy(InetSocketAddress rmAddress) throws IOException {
    return rmProxy.getProxy(conf, protocol, rmAddress);
  }

  /**
   * 内部获取RM代理方法，支持故障转移场景下刷新缓存。
   * @param isFailover 是否为故障转移触发的代理获取
   * @return 新建的RM代理对象
   */
  private T getProxyInternal(boolean isFailover) {
    SubClusterInfo subClusterInfo;
    // 使用现有代理作为后备，获取新代理失败时回退
    T proxy = this.current;
    try {
      LOG.info("Failing over to the ResourceManager for SubClusterId: {}",
          subClusterId);
      // 从联邦状态存储获取子集群信息，故障转移时按需刷新缓存
      subClusterInfo = facade.getSubCluster(subClusterId,
          this.flushFacadeCacheForYarnRMAddr && isFailover);
      // 更新配置中的RM地址，后续代理创建基于新地址
      updateRMAddress(subClusterInfo);
      if (this.originalUser == null) {
        // 无原始用户信息，直接创建代理
        InetSocketAddress rmAddress = rmProxy.getRMAddress(conf, protocol);
        LOG.info(
            "Connecting to {} subClusterId {} with protocol {}"
                + " without a proxy user",
            rmAddress, subClusterId, protocol.getSimpleName());
        proxy = createRMProxy(rmAddress);
      } else {
        // 存在原始用户信息，使用原始用户UGI创建代理以保留最新AMRMToken
        proxy = this.originalUser.doAs(new PrivilegedExceptionAction<T>() {
          @Override
          public T run() throws IOException {
            InetSocketAddress rmAddress = rmProxy.getRMAddress(conf, protocol);
            LOG.info(
                "Connecting to {} subClusterId {} with protocol {} as user {}",
                rmAddress, subClusterId, protocol.getSimpleName(),
                originalUser);
            return createRMProxy(rmAddress);
          }
        });
      }
    } catch (Exception e) {
      LOG.error("Exception while trying to create proxy to the ResourceManager"
          + " for SubClusterId: {}", subClusterId, e);
      // 无后备代理时抛出异常，否则返回原有代理
      if (proxy == null) {
        throw new YarnRuntimeException(
            String.format("Create initial proxy to the ResourceManager for"
                + " SubClusterId %s failed", subClusterId),
            e);
      }
    }
    return proxy;
  }

  /**
   * 根据子集群信息更新对应协议的RM地址到配置中。
   * @param subClusterInfo 子集群信息
   */
  private void updateRMAddress(SubClusterInfo subClusterInfo) {
    if (subClusterInfo != null) {
      // 根据协议类型更新对应RM服务地址配置
      if (protocol == ApplicationClientProtocol.class) {
        conf.set(YarnConfiguration.RM_ADDRESS,
            subClusterInfo.getClientRMServiceAddress());
      } else if (protocol == ApplicationMasterProtocol.class) {
        conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS,
            subClusterInfo.getAMRMServiceAddress());
      } else if (protocol == ResourceManagerAdministrationProtocol.class) {
        conf.set(YarnConfiguration.RM_ADMIN_ADDRESS,
            subClusterInfo.getRMAdminServiceAddress());
      }
    }
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    // 首次调用时创建初始代理
    if (current == null) {
      current = getProxyInternal(false);
    }
    return new ProxyInfo<T>(current, subClusterId.getId());
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    // 执行故障转移，获取新代理
    current = getProxyInternal(federationFailoverEnabled);
    // 关闭旧代理连接
    if (current != currentProxy) {
      closeInternal(currentProxy);
    }
  }

  @Override
  public Class<T> getInterface() {
    return protocol;
  }

  /**
   * 内部关闭代理对象方法，适配不同关闭接口。
   * @param currentProxy 需要关闭的代理对象
   */
  private void closeInternal(T currentProxy) {
    if (currentProxy != null) {
      // 如果实现Closeable接口使用close方法，否则使用RPC.stopProxy
      if (currentProxy instanceof Closeable) {
        try {
          ((Closeable) currentProxy).close();
        } catch (IOException e) {
          LOG.warn("Exception while trying to close proxy", e);
        }
      } else {
        RPC.stopProxy(currentProxy);
      }
    }
  }

  /**
   * 关闭当前代理对象，释放资源。
   */
  @Override
  public synchronized void close() throws IOException {
    closeInternal(current);
  }

}