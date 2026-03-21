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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusterResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeregisterSubClusters;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationQueueWeight;
import org.apache.hadoop.yarn.server.api.protocolrecords.FederationSubCluster;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.SaveFederationQueuePolicyResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.BatchSaveFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.QueryFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.QueryFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationQueuePoliciesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationQueuePoliciesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetSubClustersRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetSubClustersResponse;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.policies.manager.PriorityBroadcastPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedHomePolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Set;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.server.router.RouterServerUtil.checkPolicyManagerValid;

/**
 * YARN联邦模式下Router侧的ResourceManager管理请求拦截器，负责将管理请求转发到各个子集群RM，
 * 并统一处理联邦级别的管理操作，如队列策略管理、子集群下线、应用删除等。
 */
public class FederationRMAdminInterceptor extends AbstractRMAdminRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRMAdminInterceptor.class;

  private static final String COMMA = ",";
  private static final String COLON = ":";

  // 支持权重配置的策略管理器列表
  private static final List<String> SUPPORT_WEIGHT_MANAGERS =
      new ArrayList<>(Arrays.asList(WeightedLocalityPolicyManager.class.getName(),
      PriorityBroadcastPolicyManager.class.getName(), WeightedHomePolicyManager.class.getName()));

  // 缓存子集群RM管理代理连接
  private Map<SubClusterId, ResourceManagerAdministrationProtocol> adminRMProxies;
  // 联邦状态存储门面，提供对联邦元数据存储操作
  private FederationStateStoreFacade federationFacade;
  private final Clock clock = new MonotonicClock();
  // Router监控指标
  private RouterMetrics routerMetrics;
  // 并发转发请求线程池
  private ThreadPoolExecutor executorService;
  private Configuration conf;
  // 子集群心跳过期时间，超过该时间标记为丢失
  private long heartbeatExpirationMillis;

  @Override
  public void init(String userName) {
    super.init(userName);

    // 从配置读取线程池大小配置
    int numThreads = getConf().getInt(
        YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREADS_SIZE);
    // 创建命名线程工厂
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("RPC Router RMAdminClient-" + userName + "-%d ").build();

    // 从配置读取线程保活时间
    long keepAliveTime = getConf().getTimeDuration(
        YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_KEEP_ALIVE_TIME,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREAD_POOL_KEEP_ALIVE_TIME, TimeUnit.SECONDS);

    // 创建工作队列
    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    // 初始化线程池
    this.executorService = new ThreadPoolExecutor(numThreads, numThreads,
        keepAliveTime, TimeUnit.MILLISECONDS, workQueue, threadFactory);

    // 读取是否允许核心线程超时
    boolean allowCoreThreadTimeOut =  getConf().getBoolean(
        YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT);

    // 如果开启了核心线程超时且保活时间大于0，设置核心线程允许超时
    if (keepAliveTime > 0 && allowCoreThreadTimeOut) {
      this.executorService.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
    }

    // 获取联邦状态存储门面实例
    federationFacade = FederationStateStoreFacade.getInstance(this.getConf());
    this.conf = this.getConf();
    // 初始化代理缓存
    this.adminRMProxies = new ConcurrentHashMap<>();
    // 获取Router监控指标
    routerMetrics = RouterMetrics.getMetrics();

    // 读取子集群心跳过期配置
    this.heartbeatExpirationMillis = this.conf.getTimeDuration(
        YarnConfiguration.ROUTER_SUBCLUSTER_EXPIRATION_TIME,
        YarnConfiguration.DEFAULT_ROUTER_SUBCLUSTER_EXPIRATION_TIME, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  protected ResourceManagerAdministrationProtocol getAdminRMProxyForSubCluster(
      SubClusterId subClusterId) throws Exception {

    // 缓存命中直接返回
    if (adminRMProxies.containsKey(subClusterId)) {
      return adminRMProxies.get(subClusterId);
    }

    ResourceManagerAdministrationProtocol adminRMProxy = null;
    try {
      // 获取服务鉴权配置
      boolean serviceAuthEnabled = this.conf.getBoolean(
          CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
      UserGroupInformation realUser = user;
      // 如果开启服务鉴权，创建代理用户
      if (serviceAuthEnabled) {
        realUser = UserGroupInformation.createProxyUser(
            user.getShortUserName(), UserGroupInformation.getLoginUser());
      }
      // 创建RM管理代理
      adminRMProxy = FederationProxyProviderUtil.createRMProxy(getConf(),
          ResourceManagerAdministrationProtocol.class, subClusterId, realUser);
    } catch (Exception e) {
      RouterServerUtil.logAndThrowException(e,
          "Unable to create the interface to reach the SubCluster %s", subClusterId);
    }
    // 缓存代理
    adminRMProxies.put(subClusterId, adminRMProxy);
    return adminRMProxy;
  }

  @Override
  public void setNextInterceptor(RMAdminRequestInterceptor next) {
    // 本拦截器必须是责任链最后一个，不允许设置下一个拦截器
    throw new YarnRuntimeException("setNextInterceptor is being called on "
       + "FederationRMAdminRequestInterceptor, which should be the last one "
       + "in the chain. Check if the interceptor pipeline configuration "
       + "is correct");
  }

  /**
   * Refresh queue requests.
   *
   * The Router supports refreshing all SubCluster queues at once,
   * and also supports refreshing queues by SubCluster.
   *
   * @param request RefreshQueuesRequest, If subClusterId is not empty,
   * it means that we want to refresh the queue of the specified subClusterId.
   * If subClusterId is empty, it means we want to refresh all queues.
   *
   * @return RefreshQueuesResponse, There is no specific information in the response,
   * as long as it is not empty, it means that the request is successful.
   *
   * @throws StandbyException exception thrown by non-active server.
   * @throws YarnException indicates exceptions from yarn servers.
   * @throws IOException io error occurs.
   */
  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {

    // 参数校验
    if (request == null) {
      routerMetrics.incrRefreshQueuesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing RefreshQueues request.", null);
    }

    // 并发调用所有活跃子集群的refreshQueues
    try {
      long startTime = clock.getTime();
      RMAdminProtocolMethod remoteMethod = new RMAdminProtocolMethod(
           new Class[] {RefreshQueuesRequest.class}, new Object[] {request});

      String subClusterId = request.getSubClusterId();
      Collection<RefreshQueuesResponse> refreshQueueResps =
          remoteMethod.invokeConcurrent(this, RefreshQueuesResponse.class, subClusterId);

      // 只要成功获取响应说明调用成功，构造返回结果
      if (CollectionUtils.isNotEmpty(refreshQueueResps)) {
        long stopTime = clock.getTime();
        routerMetrics.succeededRefreshQueuesRetrieved(stopTime - startTime);
        return RefreshQueuesResponse.newInstance();
      }
    } catch (YarnException e) {
      routerMetrics.incrRefreshQueuesFailedRetrieved();
      RouterServerUtil.logAndThrowException(e,
          "Unable to refreshQueue due to exception. " + e.getMessage());
    }

    routerMetrics.incrRefreshQueuesFailedRetrieved();
    throw new YarnException("Unable to refreshQueue.");
  }

  /**
   * Refresh node requests.
   *
   * The Router supports refreshing all SubCluster nodes at once,
   * and also supports refreshing node by SubCluster.
   *
   * @param request RefreshNodesRequest, If subClusterId is not empty,
   * it means that we want to refresh the node of the specified subClusterId.
   * If subClusterId is empty, it means we want to refresh all nodes.
   *
   * @return RefreshNodesResponse, There is no specific information in the response,
   * as long as it is not empty, it means that the request is successful.
   *
   * @throws StandbyException exception thrown by non-active server.
   * @throws YarnException indicates exceptions from yarn servers.
   * @throws IOException io error occurs.
   */
  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {

    // 参数校验
    // We will not check whether the DecommissionType is empty,
    // because this parameter has a default value at the proto level.
    if (request == null) {
      routerMetrics.incrRefreshNodesFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing RefreshNodes request.", null);
    }

    // 并发调用所有活跃子集群的refreshNodes
    try {
      long startTime = clock.getTime();
      RMAdminProtocolMethod remoteMethod = new RMAdminProtocolMethod(
          new Class[] {RefreshNodesRequest.class}, new Object[] {request});

      String subClusterId = request.getSubClusterId();
      Collection<RefreshNodesResponse> refreshNodesResps =
          remoteMethod.invokeConcurrent(this, RefreshNodesResponse.class, subClusterId);

      if (CollectionUtils.isNotEmpty(refreshNodesResps)) {
        long stopTime = clock.getTime();
        routerMetrics.succeededRefreshNodesRetrieved(stopTime - startTime);
        return RefreshNodesResponse.newInstance();
      }
    } catch (YarnException e) {
      routerMetrics.incrRefreshNodesFailedRetrieved();
      RouterServerUtil.logAndThrowException(e,
          "Unable to refreshNodes due to exception. " + e.getMessage());
    }

    routerMetrics.incrRefreshNodesFailedRetrieved();
    throw new YarnException("Unable to refreshNodes due to exception.");
  }

  /**
   * Refresh SuperUserGroupsConfiguration requests.
   *
   * The Router supports refreshing all subCluster SuperUserGroupsConfiguration at once,
   * and also supports refreshing SuperUserGroupsConfiguration by SubCluster.
   *
   * @param request RefreshSuperUserGroupsConfigurationRequest,
   * If subClusterId is not empty, it means that we want to
   * refresh the superuser groups configuration of the specified subClusterId.
   * If subClusterId is empty, it means we want to
   * refresh all subCluster superuser groups configuration.
   *
   * @return RefreshSuperUserGroupsConfigurationResponse,
   * There is no specific information in the response, as long as it is not empty,
   * it means that the request is successful.
   *
   * @throws StandbyException exception thrown by non-active server.
   * @throws YarnException indicates exceptions