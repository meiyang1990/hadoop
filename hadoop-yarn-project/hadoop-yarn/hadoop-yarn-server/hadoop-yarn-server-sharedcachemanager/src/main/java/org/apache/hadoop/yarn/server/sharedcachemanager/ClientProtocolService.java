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

package org.apache.hadoop.yarn.server.sharedcachemanager;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.sharedcache.SharedCacheUtil;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.ClientSCMMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SharedCacheResourceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 这个文件已经全部加上中文注释
// 客户端协议服务，处理所有来自客户端的RPC调用到共享缓存管理器
/**
 * 文件说明：共享缓存管理器(SCM)客户端协议服务端实现，处理所有客户端发来的RPC请求，
 * 负责资源引用的申请与释放管理，维护共享缓存资源的引用计数。
 * This service handles all rpc calls from the client to the shared cache
 * manager.
 */
@Private
@Evolving
public class ClientProtocolService extends AbstractService implements
    ClientSCMProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClientProtocolService.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private Server server;
  InetSocketAddress clientBindAddress;
  private final SCMStore store;
  private int cacheDepth;
  private String cacheRoot;
  private ClientSCMMetrics metrics;

  /**
   * 构造客户端协议服务，注入共享缓存存储实例
   * @param store 共享缓存存储对象
   */
  public ClientProtocolService(SCMStore store) {
    super(ClientProtocolService.class.getName());
    this.store = store;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 获取服务绑定地址
    this.clientBindAddress = getBindAddress(conf);

    // 从配置获取共享缓存目录分层深度
    this.cacheDepth = SharedCacheUtil.getCacheDepth(conf);

    // 从配置获取共享缓存根目录路径
    this.cacheRoot =
        conf.get(YarnConfiguration.SHARED_CACHE_ROOT,
            YarnConfiguration.DEFAULT_SHARED_CACHE_ROOT);

    super.serviceInit(conf);
  }

  /**
   * 从配置解析客户端服务绑定地址
   * @param conf 配置对象
   * @return 服务绑定地址
   */
  InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.SCM_CLIENT_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_PORT);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    // 初始化客户端指标统计对象
    this.metrics = ClientSCMMetrics.getInstance();

    // 创建YARN RPC实例
    YarnRPC rpc = YarnRPC.create(conf);
    // 创建RPC服务端，绑定协议和地址
    this.server =
        rpc.getServer(ClientSCMProtocol.class, this,
            clientBindAddress,
            conf, null, // Secret manager null for now (security not supported)
            conf.getInt(YarnConfiguration.SCM_CLIENT_SERVER_THREAD_COUNT,
                YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_THREAD_COUNT));

    // TODO (YARN-2774): Enable service authorization

    // 启动RPC服务端
    this.server.start();
    // 更新绑定地址（处理端口自动分配情况）
    clientBindAddress =
        conf.updateConnectAddr(YarnConfiguration.SCM_CLIENT_SERVER_ADDRESS,
            server.getListenerAddress());

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 停止RPC服务端
    if (this.server != null) {
      this.server.stop();
    }

    super.serviceStop();
  }

  @Override
  /**
   * 处理客户端使用共享缓存资源的请求，添加资源引用计数，命中缓存则返回资源路径
   */
  public UseSharedCacheResourceResponse use(
      UseSharedCacheResourceRequest request) throws YarnException,
      IOException {

    // 创建响应对象
    UseSharedCacheResourceResponse response =
        recordFactory.newRecordInstance(UseSharedCacheResourceResponse.class);

    UserGroupInformation callerUGI;
    try {
      // 获取当前请求用户信息
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    // 向存储添加资源引用，返回资源文件名（不存在返回null）
    String fileName =
        this.store.addResourceReference(request.getResourceKey(),
            new SharedCacheResourceReference(request.getAppId(),
                callerUGI.getShortUserName()));

    if (fileName != null) {
      // 缓存命中，设置响应路径并更新指标
      response
          .setPath(getCacheEntryFilePath(request.getResourceKey(), fileName));
      this.metrics.incCacheHitCount();
    } else {
      // 缓存未命中，更新指标
      this.metrics.incCacheMissCount();
    }

    return response;
  }

  @Override
  /**
   * 处理客户端释放共享缓存资源的请求，移除资源引用计数
   */
  public ReleaseSharedCacheResourceResponse release(
      ReleaseSharedCacheResourceRequest request) throws YarnException,
      IOException {

    // 创建响应对象
    ReleaseSharedCacheResourceResponse response =
        recordFactory
            .newRecordInstance(ReleaseSharedCacheResourceResponse.class);

    UserGroupInformation callerUGI;
    try {
      // 获取当前请求用户信息
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    // 向存储移除资源引用，返回是否成功移除
    boolean removed =
        this.store.removeResourceReference(
            request.getResourceKey(),
            new SharedCacheResourceReference(request.getAppId(), callerUGI
                .getShortUserName()), true);

    if (removed) {
      // 更新释放指标
      this.metrics.incCacheRelease();
    }

    return response;
  }

  /**
   * 根据资源校验和和文件名生成完整的缓存资源文件路径
   * @param checksum 资源校验和
   * @param filename 资源文件名
   * @return 完整的HDFS路径字符串
   */
  private String getCacheEntryFilePath(String checksum, String filename) {
    return SharedCacheUtil.getCacheEntryPath(this.cacheDepth,
        this.cacheRoot, checksum) + Path.SEPARATOR_CHAR + filename;
  }
}