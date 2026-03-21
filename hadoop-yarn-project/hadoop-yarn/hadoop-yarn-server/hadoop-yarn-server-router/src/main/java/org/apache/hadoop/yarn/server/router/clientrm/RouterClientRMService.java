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

package org.apache.hadoop.yarn.server.router.clientrm;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.router.security.authorize.RouterPolicyProvider;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * RouterClientRMService是运行在每个Router上的服务，负责拦截处理客户端发往ResourceManager的
 * ApplicationClientProtocol请求，基于请求拦截器责任链模式处理请求，支持对请求/响应进行检查和修改。
 * 在YARN联邦场景下，承担客户端请求代理和路由的核心功能。
 */
public class RouterClientRMService extends AbstractService
    implements ApplicationClientProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterClientRMService.class);

  /** RPC服务端实例 */
  private Server server;
  /** 服务监听地址 */
  private InetSocketAddress listenerEndpoint;

  // 按用户存储请求拦截器责任链，使用LRU缓存缓存最近使用的管道，淘汰最久未使用的提升性能
  private Map<String, RequestInterceptorChainWrapper> userPipelineMap;

  /** Web代理重定向地址 */
  private URL redirectURL;
  /** Router代理令牌管理服务 */
  private RouterDelegationTokenSecretManager routerDTSecretManager;

  public RouterClientRMService() {
    super(RouterClientRMService.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting Router ClientRMService.");
    Configuration conf = getConfig();
    // 创建YARN RPC实例
    YarnRPC rpc = YarnRPC.create(conf);
    // 设置UGI配置
    UserGroupInformation.setConfiguration(conf);

    // 从配置中获取服务绑定地址
    this.listenerEndpoint =
        conf.getSocketAddr(YarnConfiguration.ROUTER_BIND_HOST,
            YarnConfiguration.ROUTER_CLIENTRM_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_PORT);

    // 如果开启了Router Web代理，初始化重定向地址
    if (RouterServerUtil.isRouterWebProxyEnable(conf)) {
      redirectURL = getRedirectURL();
    }

    // 从配置获取管道缓存最大容量
    int maxCacheSize =
        conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
            YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    // 初始化线程安全的LRU缓存存储用户管道
    this.userPipelineMap = Collections.synchronizedMap(new LRUCacheHashMap<>(maxCacheSize, true));

    // 创建服务端配置副本
    Configuration serverConf = new Configuration(conf);

    // 获取RPC工作线程数配置
    int numWorkerThreads =
        serverConf.getInt(YarnConfiguration.RM_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_CLIENT_THREAD_COUNT);

    // 初始化Router代理令牌密钥管理器
    routerDTSecretManager = createRouterRMDelegationTokenSecretManager(conf);
    routerDTSecretManager.startThreads();

    // 创建RPC服务端，绑定到监听地址
    this.server = rpc.getServer(ApplicationClientProtocol.class, this,
        listenerEndpoint, serverConf, routerDTSecretManager, numWorkerThreads);

    // 如果开启了服务授权，刷新服务ACL
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      refreshServiceAcls(conf, RouterPolicyProvider.getInstance());
    }

    // 启动RPC服务端
    this.server.start();
    LOG.info("Router ClientRMService listening on address: {}.", this.server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping Router ClientRMService.");
    // 停止RPC服务端
    if (this.server != null) {
      this.server.stop();
    }
    // 清空管道缓存
    userPipelineMap.clear();
    super.serviceStop();
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getNewApplication(request);
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().submitApplication(request);
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().forceKillApplication(request);
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getClusterMetrics(request);
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getClusterNodes(request);
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getQueueInfo(request);
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getQueueUserAcls(request);
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().moveApplicationAcrossQueues(request);
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getNewReservation(request);
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().submitReservation(request);
  }

  @Override
  public ReservationListResponse listReservations(
      ReservationListRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().listReservations(request);
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().updateReservation(request);
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().deleteReservation(request);
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getNodeToLabels(request);
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getLabelsToNodes(request);
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getClusterNodeLabels(request);
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    GetApplicationReportResponse response = pipeline.getRootInterceptor()
        .getApplicationReport(request);
    // 如果开启Router Web代理，重定向应用跟踪URL到Router代理服务
    if (RouterServerUtil.isRouterWebProxyEnable(getConfig())) {
      URL url = new URL(response.getApplicationReport().getTrackingUrl());
      String redirectUrl = new URL(redirectURL.getProtocol(),
          redirectURL.getHost(), redirectURL.getPort(), url.getFile())
          .toString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("The tracking url of application {} is redirect from {} to {}",
            response.getApplicationReport().getApplicationId(), url, redirectUrl);
      }
      // 修改响应中的跟踪URL为Router代理地址
      response.getApplicationReport().setTrackingUrl(redirectUrl);
    }
    return response;
  }

  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getApplications(request);
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getApplicationAttemptReport(request);
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getApplicationAttempts(request);
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getContainerReport(request);
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getContainers(request);
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getDelegationToken(request);
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().renewDelegationToken(request);
  }

  @Override
  public CancelDelegationTokenResponse cancelDeleg