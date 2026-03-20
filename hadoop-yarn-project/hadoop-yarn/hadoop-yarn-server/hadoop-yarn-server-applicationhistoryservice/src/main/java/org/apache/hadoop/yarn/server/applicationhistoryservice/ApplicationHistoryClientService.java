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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.timeline.security.authorize.TimelinePolicyProvider;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 应用历史客户端服务，提供 RPC 接口供客户端查询应用、尝试和容器的历史数据。
 * 
 * 该服务作为应用历史服务器的入口点，实现了 ApplicationHistoryProtocol 协议，
 * 将客户端请求委托给底层 ApplicationHistoryManager 处理。
 */
public class ApplicationHistoryClientService extends AbstractService implements
    ApplicationHistoryProtocol {
  private static final Logger LOG =
          LoggerFactory.getLogger(ApplicationHistoryClientService.class);
  
  // 应用历史管理器，负责实际的历史数据查询
  private ApplicationHistoryManager history;
  // RPC 服务器，处理客户端请求
  private Server server;
  // 服务绑定的地址
  private InetSocketAddress bindAddress;

  /**
   * 构造函数，注入应用历史管理器
   */
  public ApplicationHistoryClientService(ApplicationHistoryManager history) {
    super("ApplicationHistoryClientService");
    this.history = history;
  }

  /**
   * 启动服务：创建并启动 RPC 服务器
   */
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    
    // 从配置中获取服务绑定地址
    InetSocketAddress address = conf.getSocketAddr(
        YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
        YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_PORT);

    // 校验处理线程数必须大于 0
    Preconditions.checkArgument(conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_HANDLER_THREAD_COUNT,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_THREAD_COUNT) > 0,
        "%s property value should be greater than zero",
        YarnConfiguration.TIMELINE_SERVICE_HANDLER_THREAD_COUNT);

    // 创建 RPC 服务器
    server =
        rpc.getServer(ApplicationHistoryProtocol.class, this,
          address, conf, null, conf.getInt(
            YarnConfiguration.TIMELINE_SERVICE_HANDLER_THREAD_COUNT,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_THREAD_COUNT));

    // 如果启用了服务授权，则刷新 ACL 配置
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      refreshServiceAcls(conf, new TimelinePolicyProvider());
    }

    server.start();
    
    // 更新实际绑定地址到配置中
    this.bindAddress =
        conf.updateConnectAddr(YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
                               YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
                               YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ADDRESS,
                               server.getListenerAddress());
    LOG.info("Instantiated ApplicationHistoryClientService at "
        + this.bindAddress);

    super.serviceStart();
  }

  /**
   * 停止服务：关闭 RPC 服务器
   */
  @Override
  protected void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
    }
    super.serviceStop();
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  /**
   * 刷新服务访问控制列表
   */
  private void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException, IOException {
    // TODO 尚未实现
    return null;
  }

  /**
   * 获取指定应用尝试的报告
   */
  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request) throws YarnException,
      IOException {
    ApplicationAttemptId appAttemptId = request.getApplicationAttemptId();
    try {
      GetApplicationAttemptReportResponse response =
          GetApplicationAttemptReportResponse.newInstance(history
            .getApplicationAttempt(appAttemptId));
      return response;
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw e;
    }
  }

  /**
   * 获取指定应用的所有尝试列表
   */
  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    GetApplicationAttemptsResponse response =
        GetApplicationAttemptsResponse
          .newInstance(new ArrayList<ApplicationAttemptReport>(history
            .getApplicationAttempts(request.getApplicationId()).values()));
    return response;
  }

  /**
   * 获取指定应用的报告
   */
  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {
    ApplicationId applicationId = request.getApplicationId();
    try {
      GetApplicationReportResponse response =
          GetApplicationReportResponse.newInstance(history
            .getApplication(applicationId));
      return response;
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw e;
    }
  }

  /**
   * 获取应用列表，支持按启动时间范围过滤
   */
  @Override
  public GetApplicationsResponse
      getApplications(GetApplicationsRequest request) throws YarnException,
          IOException {
    // 解析启动时间范围，未指定则使用默认的全范围
    long startedBegin =
        request.getStartRange() == null ? 0L : request.getStartRange()
          .getMinimum();
    long startedEnd =
        request.getStartRange() == null ? Long.MAX_VALUE : request
          .getStartRange().getMaximum();
    GetApplicationsResponse response =
        GetApplicationsResponse.newInstance(new ArrayList<ApplicationReport>(
          history.getApplications(request.getLimit(), startedBegin, startedEnd)
            .values()));
    return response;
  }

  /**
   * 获取指定容器的报告
   */
  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    ContainerId containerId = request.getContainerId();
    try {
      GetContainerReportResponse response =
          GetContainerReportResponse.newInstance(history
            .getContainer(containerId));
      return response;
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw e;
    }
  }

  /**
   * 获取指定应用尝试的所有容器列表
   */
  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    GetContainersResponse response =
        GetContainersResponse.newInstance(new ArrayList<ContainerReport>(
          history.getContainers(request.getApplicationAttemptId()).values()));
    return response;
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {
    return null;
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {
    return null;
  }
}
