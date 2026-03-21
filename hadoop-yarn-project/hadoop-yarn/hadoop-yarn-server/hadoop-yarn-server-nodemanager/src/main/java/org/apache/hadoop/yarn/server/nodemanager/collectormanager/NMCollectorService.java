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
package org.apache.hadoop.yarn.server.nodemanager.collectormanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReportNewCollectorInfoResponse;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;

/**
 * NM侧时间线收集器信息管理服务，仅在启用时间线服务v2时工作。
 * 负责接收RM下发的收集器地址信息，向本NM的发布器更新地址，
 * 同时响应收集器获取应用上下文信息的RPC请求。
 */
public class NMCollectorService extends CompositeService implements
    CollectorNodemanagerProtocol {

  private static final Logger LOG =
       LoggerFactory.getLogger(NMCollectorService.class);

  private final Context context;

  private Server server;

  /**
   * 构造NMCollectorService实例，关联NodeManager上下文
   * @param context NodeManager全局上下文
   */
  public NMCollectorService(Context context) {
    super(NMCollectorService.class.getName());
    this.context = context;
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    // 解析获取收集器服务绑定地址
    InetSocketAddress collectorServerAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_COLLECTOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_PORT);

    Configuration serverConf = new Configuration(conf);
    // 创建YARN RPC实例
    YarnRPC rpc = YarnRPC.create(conf);

    // 创建RPC服务器，绑定CollectorNodemanagerProtocol协议实现
    // 开启安全认证时使用Kerberos认证
    server =
        rpc.getServer(CollectorNodemanagerProtocol.class, this,
            collectorServerAddress, serverConf, null,
            conf.getInt(YarnConfiguration.NM_COLLECTOR_SERVICE_THREAD_COUNT,
                YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_THREAD_COUNT));
    // 若开启服务级授权，刷新ACL权限配置
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      server.refreshServiceAcl(conf, NMPolicyProvider.getInstance());
    }
    // 启动RPC服务器
    server.start();
    // 更新实际绑定的连接地址（处理端口自动分配场景）
    collectorServerAddress = conf.updateConnectAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_COLLECTOR_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_NM_COLLECTOR_SERVICE_ADDRESS,
        server.getListenerAddress());
    // 启动所有子服务
    super.serviceStart();
    LOG.info("NMCollectorService started at " + collectorServerAddress);
  }

  @Override
  public void serviceStop() throws Exception {
    // 停止RPC服务器
    if (server != null) {
      server.stop();
    }
    // TODO 未来需要清理本NM上运行的应用收集器
    super.serviceStop();
  }

  @Override
  /**
   * 处理RM上报的新收集器信息请求，更新本NM上下文和时间线发布器地址
   * @param request 包含新增应用收集器列表的请求
   * @return 空响应
   * @throws YarnException Yarn异常
   * @throws IOException IO异常
   */
  public ReportNewCollectorInfoResponse reportNewCollectorInfo(
      ReportNewCollectorInfoRequest request) throws YarnException, IOException {
    List<AppCollectorData> newCollectorsList = request.getAppCollectorsList();
    if (newCollectorsList != null && !newCollectorsList.isEmpty()) {
      // 将列表转为以应用ID为key的map
      Map<ApplicationId, AppCollectorData> newCollectorsMap =
          new HashMap<>();
      // 遍历处理每个新增收集器
      for (AppCollectorData collector : newCollectorsList) {
        ApplicationId appId = collector.getApplicationId();
        newCollectorsMap.put(appId, collector);
        // 更新NMTimelinePublisher中的收集器服务地址
        // TODO: 需要确认是否要等到RM确认后再执行更新
        NMTimelinePublisher nmTimelinePublisher =
            context.getNMTimelinePublisher();
        if (nmTimelinePublisher != null) {
          nmTimelinePublisher.setTimelineServiceAddress(appId,
              collector.getCollectorAddr());
        }
      }
      // 将新收集器信息存入上下文的注册中集合
      Map<ApplicationId, AppCollectorData> registeringCollectors
          = context.getRegisteringCollectors();
      if (registeringCollectors != null) {
        registeringCollectors.putAll(newCollectorsMap);
      } else {
        LOG.warn("collectors are added when the registered collectors are " +
            "initialized");
      }
    }

    return ReportNewCollectorInfoResponse.newInstance();
  }

  @Override
  /**
   * 处理收集器获取应用上下文信息（用户名、流信息）的请求
   * @param request 获取上下文请求，包含目标应用ID
   * @return 应用上下文响应
   * @throws YarnException Yarn异常（应用不存在时抛出）
   * @throws IOException IO异常
   */
  public GetTimelineCollectorContextResponse getTimelineCollectorContext(
      GetTimelineCollectorContextRequest request)
      throws YarnException, IOException {
    // 从NM上下文获取应用信息
    Application app = context.getApplications().get(request.getApplicationId());
    if (app == null) {
      throw new YarnException("Application " + request.getApplicationId() +
          " doesn't exist on NM.");
    }
    // 返回应用的用户、流名称、流版本、流运行ID信息
    return GetTimelineCollectorContextResponse.newInstance(
        app.getUser(), app.getFlowName(), app.getFlowVersion(),
        app.getFlowRunId());
  }
}