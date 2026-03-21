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

package org.apache.hadoop.yarn.server.router.rmadmin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
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
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.security.authorize.RouterPolicyProvider;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * RouterRMAdminService 是运行在 Router 上的服务，负责拦截处理客户端发往ResourceManager的
 * ResourceManagerAdministrationProtocol 管理协议请求。它为每个用户创建请求拦截管道，
 * 管道中的拦截器可以按需检查、修改请求和响应，是YARN联邦架构中路由RM管理请求的核心服务。
 */
public class RouterRMAdminService extends AbstractService
    implements ResourceManagerAdministrationProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterRMAdminService.class);

  private Server server;
  private InetSocketAddress listenerEndpoint;

  // 存储每个用户对应的请求拦截管道
  // 使用LRU缓存，只保留最近使用的管道，淘汰最久未使用的，控制内存占用
  private Map<String, RequestInterceptorChainWrapper> userPipelineMap;

  public RouterRMAdminService() {
    super(RouterRMAdminService.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting Router RMAdmin Service.");
    Configuration conf = getConfig();
    // 创建YARN RPC实例
    YarnRPC rpc = YarnRPC.create(conf);
    // 设置安全配置
    UserGroupInformation.setConfiguration(conf);

    // 从配置获取RMAdmin服务监听地址
    this.listenerEndpoint =
        conf.getSocketAddr(YarnConfiguration.ROUTER_BIND_HOST,
            YarnConfiguration.ROUTER_RMADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_RMADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_RMADMIN_PORT);

    // 从配置获取管道缓存最大容量
    int maxCacheSize =
        conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
            YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    // 初始化线程安全的LRU缓存存储用户管道
    this.userPipelineMap = Collections.synchronizedMap(new LRUCacheHashMap<>(maxCacheSize, true));

    // 创建服务端配置副本
    Configuration serverConf = new Configuration(conf);

    // 从配置获取工作线程数
    int numWorkerThreads =
        serverConf.getInt(YarnConfiguration.RM_ADMIN_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_ADMIN_CLIENT_THREAD_COUNT);

    // 创建RPC服务端，绑定本服务实例到监听地址
    this.server = rpc.getServer(ResourceManagerAdministrationProtocol.class,
        this, listenerEndpoint, serverConf, null, numWorkerThreads);

    // 如果开启了安全授权，刷新服务访问控制列表
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      refreshServiceAcls(conf, RouterPolicyProvider.getInstance());
    }

    // 启动RPC服务
    this.server.start();
    LOG.info("Router RMAdminService listening on address: {}.", this.server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping Router RMAdminService.");
    if (this.server != null) {
      // 停止RPC服务
      this.server.stop();
    }
    // 清空管道缓存
    userPipelineMap.clear();
    super.serviceStop();
  }

  void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }

  @VisibleForTesting
  public RequestInterceptorChainWrapper getInterceptorChain()
      throws IOException {
    // 获取当前请求用户
    String user = UserGroupInformation.getCurrentUser().getUserName();
    // 从缓存获取用户管道
    RequestInterceptorChainWrapper chain = userPipelineMap.get(user);
    if (chain != null && chain.getRootInterceptor() != null) {
      return chain;
    }
    // 缓存不存在则初始化新管道
    return initializePipeline(user);
  }

  /**
   * 获取所有用户的请求拦截管道缓存。
   *
   * @return 所有用户的请求拦截管道映射
   */
  @VisibleForTesting
  protected Map<String, RequestInterceptorChainWrapper> getPipelines() {
    return this.userPipelineMap;
  }

  /**
   * 根据配置创建并初始化拦截器责任链，返回链首拦截器。
   *
   * @return 责任链的第一个拦截器实例
   */
  @VisibleForTesting
  protected RMAdminRequestInterceptor createRequestInterceptorChain() {
    Configuration conf = getConfig();
    // 调用工具方法按配置创建拦截器链
    return RouterServerUtil.createRequestInterceptorChain(conf,
        YarnConfiguration.ROUTER_RMADMIN_INTERCEPTOR_CLASS_PIPELINE,
        YarnConfiguration.DEFAULT_ROUTER_RMADMIN_INTERCEPTOR_CLASS,
        RMAdminRequestInterceptor.class);
  }

  /**
   * 为指定用户初始化请求拦截管道，存入缓存。
   *
   * @param user 用户名
   * @return 初始化完成的拦截管道包装器
   */
  private RequestInterceptorChainWrapper initializePipeline(String user) {
    synchronized (this.userPipelineMap) {
      // 双重检查避免重复初始化
      if (this.userPipelineMap.containsKey(user)) {
        LOG.info("Request to start an already existing user: {}"
            + " was received, so ignoring.", user);
        return userPipelineMap.get(user);
      }

      RequestInterceptorChainWrapper chainWrapper =
          new RequestInterceptorChainWrapper();
      try {
        // 先初始化再加入缓存，保证线程安全
        LOG.info("Initializing request processing pipeline for user: {}.", user);

        RMAdminRequestInterceptor interceptorChain =
            this.createRequestInterceptorChain();
        interceptorChain.init(user);
        chainWrapper.init(interceptorChain);
      } catch (Exception e) {
        LOG.error("Init RMAdminRequestInterceptor error for user: {}.", user, e);
        throw e;
      }

      this.userPipelineMap.put(user, chainWrapper);
      return chainWrapper;
    }
  }

  /**
   * 封装用户请求拦截链的包装类，统一管理拦截器实例生命周期。
   *
   */
  @Private
  public static class RequestInterceptorChainWrapper {
    private RMAdminRequestInterceptor rootInterceptor;

    /**
     * 初始化包装器，设置链首拦截器。
     *
     * @param interceptor 责任链首拦截器
     */
    public synchronized void init(RMAdminRequestInterceptor interceptor) {
      this.rootInterceptor = interceptor;
    }

    /**
     * 获取责任链首拦截器。
     *
     * @return 责任链首拦截器
     */
    public synchronized RMAdminRequestInterceptor getRootInterceptor() {
      return rootInterceptor;
    }

    /**
     * 对象销毁时关闭拦截链，释放资源。
     */
    @Override
    protected void finalize() {
      rootInterceptor.shutdown();
    }
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getGroupsForUser(user);
  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshQueues(request);

  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshNodes(request);

  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor()
        .refreshSuperUserGroupsConfiguration(request);

  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshUserToGroupsMappings(request);

  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshAdminAcls(request);

  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshServiceAcls(request);

  }

  @Override
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().updateNodeResource(request);

  }

  @Override
  public RefreshNodesResourcesResponse refreshNodesResources(
      RefreshNodesResourcesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshNodesResources(request);

  }

  @Override
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
      AddToClusterNodeLabelsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().addToClusterNodeLabels(request);

  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().removeFromClusterNodeLabels(request);

  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().replaceLabelsOnNode(request);

  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor()
        .checkForDecommissioningNodes(checkForDecommissioningNodesRequest);
  }

  @Override
  public RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      RefreshClusterMaxPriorityRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshClusterMaxPriority(request);
  }

  @Override
  public NodesToAttributesMappingResponse mapAttributesToNodes(
      NodesToAttributesMappingRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().mapAttributesToNodes(request);
  }

  @Override
  public DeregisterSubClusterResponse deregisterSubCluster(
      DeregisterSubClusterRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().deregisterSubCluster(request);
  }

  @Override
  public SaveFederationQueuePolicyResponse saveFederationQueuePolicy(
      SaveFederationQueuePolicyRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().saveFederationQueuePolicy(request);
  }

  @Override
  public BatchSaveFederationQueuePoliciesResponse batchSaveFederationQueuePolicies(
      BatchSaveFederationQueuePoliciesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().batchSaveFederationQueuePolicies(request);
  }

  @Override
  public QueryFederationQueuePoliciesResponse listFederationQueuePolicies(
      QueryFederationQueuePoliciesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().listFederationQueuePolicies(request);
  }

  @Override
  public DeleteFederationApplicationResponse deleteFederationApplication(
      DeleteFederationApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRoot