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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderCanUploadResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SCMUploaderNotifyResponse;
import org.apache.hadoop.yarn.server.sharedcachemanager.metrics.SharedCacheUploaderMetrics;
import org.apache.hadoop.yarn.server.sharedcachemanager.store.SCMStore;

// 这个文件已经全部加上中文注释
// 共享缓存上传服务，处理来自NodeManager上传器的所有RPC调用到共享缓存管理器
/**
 * 文件: 共享缓存管理器上传服务
 * 功能: 处理来自NodeManager上传器的所有RPC请求，对接共享缓存存储，处理资源上传校验和通知逻辑
 * 属于: YARN共享缓存管理器服务端核心模块
 */
/**
 * This service handles all rpc calls from the NodeManager uploader to the
 * shared cache manager.
 */
public class SharedCacheUploaderService extends AbstractService
    implements SCMUploaderProtocol {
  // 记录工厂，用于创建协议记录对象
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  // RPC服务端实例
  private Server server;
  // 服务绑定地址
  InetSocketAddress bindAddress;
  // 共享缓存存储引用
  private final SCMStore store;
  // 上传指标统计
  private SharedCacheUploaderMetrics metrics;

  /**
   * 构造共享缓存上传服务，依赖共享缓存存储
   * @param store 共享缓存存储实例
   */
  public SharedCacheUploaderService(SCMStore store) {
    super(SharedCacheUploaderService.class.getName());
    this.store = store;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置获取服务绑定地址
    this.bindAddress = getBindAddress(conf);

    super.serviceInit(conf);
  }

  /**
   * 从配置中解析获取上传服务绑定地址
   * @param conf 配置对象
   * @return 解析后的绑定地址
   */
  InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.SCM_UPLOADER_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_PORT);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    // 获取上传指标统计实例
    this.metrics = SharedCacheUploaderMetrics.getInstance();

    // 创建YARN RPC实例
    YarnRPC rpc = YarnRPC.create(conf);
    // 创建RPC服务端，绑定协议和地址
    this.server =
        rpc.getServer(SCMUploaderProtocol.class, this, bindAddress,
            conf, null, // Secret manager null for now (security not supported)
            conf.getInt(YarnConfiguration.SCM_UPLOADER_SERVER_THREAD_COUNT,
                YarnConfiguration.DEFAULT_SCM_UPLOADER_SERVER_THREAD_COUNT));

    // TODO (YARN-2774): Enable service authorization

    // 启动RPC服务
    this.server.start();
    // 更新绑定地址（处理自动端口分配场景）
    bindAddress =
        conf.updateConnectAddr(YarnConfiguration.SCM_UPLOADER_SERVER_ADDRESS,
            server.getListenerAddress());

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 停止RPC服务
    if (this.server != null) {
      this.server.stop();
      this.server = null;
    }

    super.serviceStop();
  }

  @Override
  /**
   * 处理NodeManager上传完成通知，将资源信息存入共享缓存
   */
  public SCMUploaderNotifyResponse notify(SCMUploaderNotifyRequest request)
      throws YarnException, IOException {
    // 创建响应对象
    SCMUploaderNotifyResponse response =
        recordFactory.newRecordInstance(SCMUploaderNotifyResponse.class);

    // TODO (YARN-2774): proper security/authorization needs to be implemented

    // 将资源信息存入共享缓存存储，获取实际存储文件名
    String filename =
        store.addResource(request.getResourceKey(), request.getFileName());

    // 判断上传是否被接受：文件名一致说明是新资源接受上传，不一致说明已有该资源，拒绝重复上传
    boolean accepted = filename.equals(request.getFileName());

    // 更新指标统计
    if (accepted) {
      this.metrics.incAcceptedUploads();
    } else {
      this.metrics.incRejectedUploads();
    }

    // 设置响应结果
    response.setAccepted(accepted);

    return response;
  }

  @Override
  /**
   * 处理NodeManager上传权限查询，判断当前资源是否可以上传
   */
  public SCMUploaderCanUploadResponse canUpload(
      SCMUploaderCanUploadRequest request) throws YarnException, IOException {
    // TODO (YARN-2781): we may want to have a more flexible policy of
    // instructing the node manager to upload only if it meets a certain
    // criteria
    // until then we return true for now
    // 创建响应对象，默认允许所有上传
    SCMUploaderCanUploadResponse response =
        recordFactory.newRecordInstance(SCMUploaderCanUploadResponse.class);
    response.setUploadable(true);
    return response;
  }
}