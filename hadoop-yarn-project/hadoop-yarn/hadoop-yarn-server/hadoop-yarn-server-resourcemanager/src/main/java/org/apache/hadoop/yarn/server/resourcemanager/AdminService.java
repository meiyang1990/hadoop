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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.security.ConfiguredYarnAuthorizer;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.QueryFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.QueryFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetSubClustersRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetSubClustersResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NodeLabelsUtils;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DynamicResourceConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;

/**
 * ResourceManager 管理服务，提供集群管理RPC接口实现和HA状态转换支持
 * 负责处理管理员对集群的各种配置刷新、节点管理、标签管理等操作
 */
public class AdminService extends CompositeService implements
    HAServiceProtocol, ResourceManagerAdministrationProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(AdminService.class);

  // 所属ResourceManager实例
  private final ResourceManager rm;
  private String rmId;

  // 是否启用自动故障转移
  private boolean autoFailoverEnabled;

  // RPC服务端实例
  private Server server;

  // 管理服务绑定地址
  private InetSocketAddress masterServiceBindAddress;

  // 权限验证器
  private YarnAuthorizationProvider authorizer;

  private final RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);

  // RM守护进程用户信息
  private UserGroupInformation daemonUser;

  @VisibleForTesting
  boolean isCentralizedNodeLabelConfiguration = true;

  /**
   * 构造函数，初始化AdminService
   * @param rm 所属ResourceManager实例
   */
  public AdminService(ResourceManager rm) {
    super(AdminService.class.getName());
    this.rm = rm;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    // 计算是否启用自动故障转移
    autoFailoverEnabled =
        rm.getRMContext().isHAEnabled()
            && HAUtil.isAutomaticFailoverEnabled(conf);

    // 获取管理服务绑定地址
    masterServiceBindAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
    // 获取当前守护进程用户
    daemonUser = UserGroupInformation.getCurrentUser();
    // 初始化权限验证器
    authorizer = YarnAuthorizationProvider.getInstance(conf);
    // 设置管理员ACL
    authorizer.setAdmins(getAdminAclList(conf), daemonUser);
    // 获取RM标识ID
    rmId = conf.get(YarnConfiguration.RM_HA_ID);

    // 读取是否启用集中式节点标签配置
    isCentralizedNodeLabelConfiguration =
        YarnConfiguration.isCentralizedNodeLabelConfiguration(conf);

    super.serviceInit(conf);
  }

  /**
   * 从配置中读取管理员ACL并添加守护进程用户
   * @param conf 配置对象
   * @return 构建好的管理员ACL
   */
  private AccessControlList getAdminAclList(Configuration conf) {
    AccessControlList aclList = new AccessControlList(conf.get(
        YarnConfiguration.YARN_ADMIN_ACL,
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    aclList.addUser(daemonUser.getShortUserName());
    return aclList;
  }

  @Override
  protected void serviceStart() throws Exception {
    startServer();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopServer();
    super.serviceStop();
  }

  /**
   * 启动管理RPC服务
   * @throws Exception 启动失败时抛出异常
   */
  protected void startServer() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    // 创建管理协议RPC服务端
    this.server = (Server) rpc.getServer(
        ResourceManagerAdministrationProtocol.class, this, masterServiceBindAddress,
        conf, null,
        conf.getInt(YarnConfiguration.RM_ADMIN_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_ADMIN_CLIENT_THREAD_COUNT));

    // 如果启用服务授权，刷新服务ACL配置
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      refreshServiceAcls(
          getConfiguration(conf,
              YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE),
          RMPolicyProvider.getInstance());
    }

    // 如果启用HA，添加HA服务协议实现
    if (rm.getRMContext().isHAEnabled()) {
      RPC.setProtocolEngine(conf, HAServiceProtocolPB.class,
          ProtobufRpcEngine2.class);

      HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator =
          new HAServiceProtocolServerSideTranslatorPB(this);
      BlockingService haPbService =
          HAServiceProtocolProtos.HAServiceProtocolService
              .newReflectiveBlockingService(haServiceProtocolXlator);
      server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
          HAServiceProtocol.class, haPbService);
    }

    this.server.start();
    // 更新连接地址到配置
    conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
                           YarnConfiguration.RM_ADMIN_ADDRESS,
                           YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
                           server.getListenerAddress());
  }

  /**
   * 停止管理RPC服务
   * @throws Exception 停止失败抛出异常
   */
  protected void stopServer() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
  }

  /**
   * 验证管理员访问权限
   * @param method 调用的方法名
   * @return 验证通过的用户信息
   * @throws IOException 权限验证失败抛出异常
   */
  private UserGroupInformation checkAccess(String method) throws IOException {
    return RMServerUtils.verifyAdminAccess(authorizer, method, LOG);
  }

  /**
   * 验证管理员访问权限并包装异常为YarnException
   * @param method 调用的方法名
   * @return 验证通过的用户信息
   * @throws YarnException 权限验证失败抛出异常
   */
  private UserGroupInformation checkAcls(String method) throws YarnException {
    try {
      return checkAccess(method);
    } catch (IOException ioe) {
      throw RPCUtil.getRemoteException(ioe);
    }
  }

  /**
   * 检查HA状态变更请求是否合法
   * 根据自动故障转移配置拒绝非法手动请求
   * @param req 状态变更请求信息
   * @throws AccessControlException 请求不合法抛出异常
   */
  private void checkHaStateChange(StateChangeRequestInfo req)
      throws AccessControlException {
    switch (req.getSource()) {
      case REQUEST_BY_USER:
        if (autoFailoverEnabled) {
          throw new AccessControlException(
              "Manual failover for this ResourceManager is disallowed, " +
                  "because automatic failover is enabled.");
        }
        break;
      case REQUEST_BY_USER_FORCED:
        if (autoFailoverEnabled) {
          LOG.warn("Allowing manual failover from " +
              org.apache.hadoop.ipc.Server.getRemoteAddress() +
              " even though automatic failover is enabled, because the user " +
              "specified the force flag");
        }
        break;
      case REQUEST_BY_ZKFC:
        if (!autoFailoverEnabled) {
          throw new AccessControlException(
              "Request from ZK failover controller at " +
                  org.apache.hadoop.ipc.Server.getRemoteAddress() + " denied " +
                  "since automatic failover is not enabled");
        }
        break;
    }
  }

  /**
   * 检查当前RM是否处于Active状态
   * @return 是否Active
   */
  private synchronized boolean isRMActive() {
    return HAServiceState.ACTIVE == rm.getRMContext().getHAServiceState();
  }

  /**
   * 抛出Standby异常，表示当前RM不是Active无法处理请求
   * @throws StandbyException 固定抛出Standby异常
   */
  private void throwStandbyException() throws StandbyException {
    throw new StandbyException("ResourceManager " + rmId + " is not Active!");
  }

  @Override
  public synchronized void monitorHealth()
      throws IOException {
    checkAccess("monitorHealth");
    // Active状态但服务未启动，健康检查失败
    if (isRMActive() && !rm.areActiveServicesRunning()) {
      throw new HealthCheckFailedException(
          "Active ResourceManager services are not running!");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void transitionToActive(
      HAServiceProtocol.StateChangeRequestInfo reqInfo) throws IOException {
    // 已经是Active直接返回
    if (isRMActive()) {
      return;
    }
    // HA状态转换前刷新管理员ACL，同步之前Active RM的更新
    try {
      refreshAdminAcls(false);
    } catch (YarnException ex) {
      throw new ServiceFailedException("Can not execute refreshAdminAcls", ex);
    }

    UserGroupInformation user = checkAccess("transitionToActive");
    checkHaStateChange(reqInfo);

    try {
      // 刷新所有配置，获取最新配置后切换Active
      refreshAll();
    } catch (Exception e) {
      // 刷新失败发布致命事件，终止转换
      rm.getRMContext()
          .getDispatcher()
          .getEventHandler()
          .handle(
              new RMFatalEvent(RMFatalEventType.TRANSITION_TO_ACTIVE_FAILED,
                  e, "failure to refresh configuration settings"));
      throw new ServiceFailedException(
          "Error on refreshAll during transition to Active", e);
    }

    try {
      // 调用RM完成Active状态转换
      rm.transitionToActive();
    } catch (Exception e) {
      RMAuditLogger.logFailure(user.getShortUserName(), "transitionToActive",
          "", "RM",
          "Exception transitioning to active");
      throw new ServiceFailedException(
          "Error when transitioning to Active mode", e);
    }

    RMAuditLogger.logSuccess(user.getShortUserName(), "transitionToActive",
        "RM");
  }

  @Override
  public synchronized void transitionToStandby(
      HAServiceProtocol.StateChangeRequestInfo reqInfo) throws IOException {
    // HA状态转换前刷新管理员ACL
    try {
      refreshAdminAcls(false);
    } catch (YarnException ex) {
      throw new ServiceFailedException("Can not execute refreshAdminAcls", ex);
    }
    UserGroupInformation user = checkAccess("transitionToStandby");
    checkHaStateChange(reqInfo);
    try {
      // 调用RM完成Standby状态转换
      rm.transitionToStandby(true);
      RMAuditLogger.logSuccess(user.getShortUserName(),
          "transitionToStandby", "RM");
    } catch (Exception e) {
      RMAuditLogger.log