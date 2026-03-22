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

package org.apache.hadoop.mapreduce.v2.hs.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogDeletionService;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserMappingsProtocolService;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetUserMappingsProtocolService;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.mapreduce.v2.api.HSAdminProtocol;
import org.apache.hadoop.mapreduce.v2.api.HSAdminRefreshProtocolPB;
import org.apache.hadoop.mapreduce.v2.app.security.authorize.ClientHSPolicyProvider;
import org.apache.hadoop.mapreduce.v2.hs.HSAuditLogger;
import org.apache.hadoop.mapreduce.v2.hs.HSAuditLogger.AuditConstants;
import org.apache.hadoop.mapreduce.v2.hs.JobHistory;
import org.apache.hadoop.mapreduce.v2.hs.proto.HSAdminRefreshProtocolProtos.HSAdminRefreshProtocolService;
import org.apache.hadoop.mapreduce.v2.hs.protocolPB.HSAdminRefreshProtocolServerSideTranslatorPB;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 历史服务器管理RPC服务端，提供对MapReduce历史服务器的管理操作接口
 * 负责处理各类管理类RPC请求，包括刷新用户映射、刷新ACL、刷新缓存、刷新 retention 设置等
 */
@Private
public class HSAdminServer extends AbstractService implements HSAdminProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(HSAdminServer.class);
  // 管理员访问控制列表
  private AccessControlList adminAcl;
  // 聚合日志删除服务实例
  private AggregatedLogDeletionService aggLogDelService = null;

  /** 监听客户端请求的RPC服务器 */
  protected RPC.Server clientRpcServer;
  // 客户端RPC服务监听地址
  protected InetSocketAddress clientRpcAddress;
  private static final String HISTORY_ADMIN_SERVER = "HSAdminServer";
  // 作业历史服务实例
  private JobHistory jobHistoryService = null;

  // 登录用户信息
  private UserGroupInformation loginUGI;

  /**
   * 构造HSAdminServer实例
   * @param aggLogDelService 聚合日志删除服务
   * @param jobHistoryService 作业历史服务
   */
  public HSAdminServer(AggregatedLogDeletionService aggLogDelService,
      JobHistory jobHistoryService) {
    super(HSAdminServer.class.getName());
    this.aggLogDelService = aggLogDelService;
    this.jobHistoryService = jobHistoryService;
  }

  /**
   * 服务初始化，注册所有管理协议并启动RPC服务器
   * @param conf 配置对象
   * @throws Exception 初始化过程中抛出的异常
   */
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    // 设置用户映射刷新协议的RPC引擎
    RPC.setProtocolEngine(conf, RefreshUserMappingsProtocolPB.class,
        ProtobufRpcEngine2.class);

    // 创建用户映射刷新协议的PB转换器
    RefreshUserMappingsProtocolServerSideTranslatorPB refreshUserMappingXlator = new RefreshUserMappingsProtocolServerSideTranslatorPB(
        this);
    // 创建用户映射刷新协议的阻塞服务
    BlockingService refreshUserMappingService = RefreshUserMappingsProtocolService
        .newReflectiveBlockingService(refreshUserMappingXlator);

    // 创建用户映射获取协议的PB转换器
    GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator = new GetUserMappingsProtocolServerSideTranslatorPB(
        this);
    // 创建用户映射获取协议的阻塞服务
    BlockingService getUserMappingService = GetUserMappingsProtocolService
        .newReflectiveBlockingService(getUserMappingXlator);

    // 创建HS管理刷新协议的PB转换器
    HSAdminRefreshProtocolServerSideTranslatorPB refreshHSAdminProtocolXlator = new HSAdminRefreshProtocolServerSideTranslatorPB(
        this);
    // 创建HS管理刷新协议的阻塞服务
    BlockingService refreshHSAdminProtocolService = HSAdminRefreshProtocolService
        .newReflectiveBlockingService(refreshHSAdminProtocolXlator);

    // 从配置中获取RPC服务绑定地址
    clientRpcAddress = conf.getSocketAddr(
        JHAdminConfig.MR_HISTORY_BIND_HOST,
        JHAdminConfig.JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ADDRESS,
        JHAdminConfig.DEFAULT_JHS_ADMIN_PORT);
    // 构建并初始化RPC服务器
    clientRpcServer = new RPC.Builder(conf)
        .setProtocol(RefreshUserMappingsProtocolPB.class)
        .setInstance(refreshUserMappingService)
        .setBindAddress(clientRpcAddress.getHostName())
        .setPort(clientRpcAddress.getPort()).setVerbose(false).build();

    // 添加其他协议到RPC服务器
    addProtocol(conf, GetUserMappingsProtocolPB.class, getUserMappingService);
    addProtocol(conf, HSAdminRefreshProtocolPB.class,
        refreshHSAdminProtocolService);

    // 如果开启服务授权，刷新服务ACL配置
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      clientRpcServer.refreshServiceAcl(conf, new ClientHSPolicyProvider());
    }

    // 从配置加载管理员ACL
    adminAcl = new AccessControlList(conf.get(JHAdminConfig.JHS_ADMIN_ACL,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ACL));

  }

  /**
   * 启动服务，获取登录用户信息并启动RPC服务器
   * @throws Exception 启动过程中抛出的异常
   */
  @Override
  protected void serviceStart() throws Exception {
    // 根据安全模式获取登录用户信息
    if (UserGroupInformation.isSecurityEnabled()) {
      loginUGI = UserGroupInformation.getLoginUser();
    } else {
      loginUGI = UserGroupInformation.getCurrentUser();
    }
    clientRpcServer.start();
  }

  @VisibleForTesting
  UserGroupInformation getLoginUGI() {
    return loginUGI;
  }

  @VisibleForTesting
  void setLoginUGI(UserGroupInformation ugi) {
    loginUGI = ugi;
  }

  /**
   * 停止服务，关闭RPC服务器
   * @throws Exception 停止过程中抛出的异常
   */
  @Override
  protected void serviceStop() throws Exception {
    if (clientRpcServer != null) {
      clientRpcServer.stop();
    }
  }

  /**
   * 向RPC服务器添加新的协议
   * @param conf 配置对象
   * @param protocol 协议接口类
   * @param blockingService 协议对应的PB阻塞服务
   * @throws IOException 添加协议失败时抛出IO异常
   */
  private void addProtocol(Configuration conf, Class<?> protocol,
      BlockingService blockingService) throws IOException {
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngine2.class);
    clientRpcServer.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol,
        blockingService);
  }

  /**
   * 检查当前用户是否拥有管理员权限，用于所有管理操作的权限校验
   * @param method 被调用的管理方法名
   * @return 校验通过的当前用户信息
   * @throws IOException 权限校验失败或获取用户信息失败时抛出异常
   */
  private UserGroupInformation checkAcls(String method) throws IOException {
    UserGroupInformation user;
    try {
      // 获取当前请求用户
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      LOG.warn("Couldn't get current user", ioe);
      // 记录审计日志：获取用户失败
      HSAuditLogger.logFailure("UNKNOWN", method, adminAcl.toString(),
          HISTORY_ADMIN_SERVER, "Couldn't get current user");
      throw ioe;
    }

    // 检查用户是否在管理员ACL中
    if (!adminAcl.isUserAllowed(user)) {
      LOG.warn("User " + user.getShortUserName() + " doesn't have permission"
          + " to call '" + method + "'");
      // 记录审计日志：用户未授权
      HSAuditLogger.logFailure(user.getShortUserName(), method,
          adminAcl.toString(), HISTORY_ADMIN_SERVER,
          AuditConstants.UNAUTHORIZED_USER);
      throw new AccessControlException("User " + user.getShortUserName()
          + " doesn't have permission" + " to call '" + method + "'");
    }
    LOG.info("HS Admin: " + method + " invoked by user "
        + user.getShortUserName());

    return user;
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }

  @Override
  public void refreshUserToGroupsMappings() throws IOException {
    // 权限校验
    UserGroupInformation user = checkAcls("refreshUserToGroupsMappings");
    // 刷新用户-组映射缓存
    Groups.getUserToGroupsMappingService().refresh();
    // 记录审计日志：操作成功
    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshUserToGroupsMappings", HISTORY_ADMIN_SERVER);
  }

  @Override
  public void refreshSuperUserGroupsConfiguration() throws IOException {
    // 权限校验
    UserGroupInformation user = checkAcls("refreshSuperUserGroupsConfiguration");
    // 刷新超级用户代理组配置
    ProxyUsers.refreshSuperUserGroupsConfiguration(createConf());
    // 记录审计日志：操作成功
    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshSuperUserGroupsConfiguration", HISTORY_ADMIN_SERVER);
  }

  protected Configuration createConf() {
    return new Configuration();
  }

  @Override
  public void refreshAdminAcls() throws IOException {
    // 权限校验
    UserGroupInformation user = checkAcls("refreshAdminAcls");
    // 重新从配置加载管理员ACL
    Configuration conf = createConf();
    adminAcl = new AccessControlList(conf.get(JHAdminConfig.JHS_ADMIN_ACL,
        JHAdminConfig.DEFAULT_JHS_ADMIN_ACL));
    // 记录审计日志：操作成功
    HSAuditLogger.logSuccess(user.getShortUserName(), "refreshAdminAcls",
        HISTORY_ADMIN_SERVER);
  }

  @Override
  public void refreshLoadedJobCache() throws IOException {
    // 权限校验
    UserGroupInformation user = checkAcls("refreshLoadedJobCache");
    try {
      // 刷新已加载作业缓存
      jobHistoryService.refreshLoadedJobCache();
    } catch (UnsupportedOperationException e) {
      // 记录审计日志：操作失败
      HSAuditLogger.logFailure(user.getShortUserName(),
          "refreshLoadedJobCache", adminAcl.toString(), HISTORY_ADMIN_SERVER,
          e.getMessage());
      throw e;
    }
    // 记录审计日志：操作成功
    HSAuditLogger.logSuccess(user.getShortUserName(), "refreshLoadedJobCache",
        HISTORY_ADMIN_SERVER);
  }

  @Override
  public void refreshLogRetentionSettings() throws IOException {
    // 权限校验
    UserGroupInformation user = checkAcls("refreshLogRetentionSettings");
    // 以登录用户身份执行刷新
    try {
      loginUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          aggLogDelService.refreshLogRetentionSettings();
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    // 记录审计日志：操作成功
    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshLogRetentionSettings", "HSAdminServer");
  }

  @Override
  public void refreshJobRetentionSettings() throws IOException {
    // 权限校验
    UserGroupInformation user = checkAcls("refreshJobRetentionSettings");
    // 以登录用户身份执行刷新
    try {
      loginUGI.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws IOException {
          jobHistoryService.refreshJobRetentionSettings();
          return null;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    // 记录审计日志：操作成功
    HSAuditLogger.logSuccess(user.getShortUserName(),
        "refreshJobRetentionSettings", HISTORY_ADMIN_SERVER);
  }
}