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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.api.SCMAdminProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RunSharedCacheCleanerTaskResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：共享缓存管理器(SCM)管理协议RPC服务端实现
 * 
 * 该服务处理管理员向共享缓存管理器发起的所有SCMAdminProtocol RPC调用，
 * 提供管理接口，支持触发缓存清理任务等管理操作。
 */
@Private
@Unstable
public class SCMAdminProtocolService extends AbstractService implements
    SCMAdminProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMAdminProtocolService.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private Server server;
  InetSocketAddress clientBindAddress;
  private final CleanerService cleanerService;
  private YarnAuthorizationProvider authorizer;

  /**
   * 构造SCM管理协议服务
   * @param cleanerService 缓存清理服务实例
   */
  public SCMAdminProtocolService(CleanerService cleanerService) {
    super(SCMAdminProtocolService.class.getName());
    this.cleanerService = cleanerService;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置获取服务绑定地址
    this.clientBindAddress = getBindAddress(conf);
    // 初始化权限验证器
    authorizer = YarnAuthorizationProvider.getInstance(conf);
    super.serviceInit(conf);
  }

  /**
   * 从配置中解析获取SCM管理服务的绑定地址
   * @param conf YARN配置对象
   * @return 解析后的绑定地址
   */
  InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.SCM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_ADMIN_PORT);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    // 创建YARN RPC实例
    YarnRPC rpc = YarnRPC.create(conf);
    // 创建RPC服务端
    this.server =
        rpc.getServer(SCMAdminProtocol.class, this,
            clientBindAddress,
            conf, null, // Secret manager null for now (security not supported)
            conf.getInt(YarnConfiguration.SCM_ADMIN_CLIENT_THREAD_COUNT,
                YarnConfiguration.DEFAULT_SCM_ADMIN_CLIENT_THREAD_COUNT));

    // TODO: Enable service authorization (see YARN-2774)

    // 启动RPC服务端
    this.server.start();
    // 更新绑定地址（处理端口自动分配场景）
    clientBindAddress =
        conf.updateConnectAddr(YarnConfiguration.SCM_ADMIN_ADDRESS,
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

  /**
   * 验证当前用户是否拥有管理员权限执行指定操作
   * @param method 要执行的管理方法名
   * @throws YarnException 权限验证失败时抛出异常
   */
  private void checkAcls(String method) throws YarnException {
    UserGroupInformation user;
    try {
      // 获取当前请求用户
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      LOG.warn("Couldn't get current user", ioe);
      throw RPCUtil.getRemoteException(ioe);
    }

    // 检查用户是否为管理员
    if (!authorizer.isAdmin(user)) {
      LOG.warn("User " + user.getShortUserName() + " doesn't have permission" +
          " to call '" + method + "'");

      throw RPCUtil.getRemoteException(
          new AccessControlException("User " + user.getShortUserName() +
          " doesn't have permission" + " to call '" + method + "'"));
    }
    // 记录管理员操作日志
    LOG.info("SCM Admin: " + method + " invoked by user " +
        user.getShortUserName());
  }

  @Override
  public RunSharedCacheCleanerTaskResponse runCleanerTask(
      RunSharedCacheCleanerTaskRequest request) throws YarnException {
    // 验证管理员权限
    checkAcls("runCleanerTask");
    // 创建响应对象
    RunSharedCacheCleanerTaskResponse response =
        recordFactory.newRecordInstance(RunSharedCacheCleanerTaskResponse.class);
    // 触发清理服务执行清理任务
    this.cleanerService.runCleanerTask();
    // 提交成功，返回接受响应给客户端
    response.setAccepted(true);
    return response;
  }
}