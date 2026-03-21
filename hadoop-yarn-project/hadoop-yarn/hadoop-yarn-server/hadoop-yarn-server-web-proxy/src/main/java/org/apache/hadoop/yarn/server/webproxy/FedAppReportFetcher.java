// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.webproxy;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * YARN联邦环境下的应用报告获取器，从对应子集群获取应用报告和Web页面地址
 * 继承通用AppReportFetcher，扩展支持多子集群场景
 */
public class FedAppReportFetcher extends AppReportFetcher {

  // 缓存已连接的子集群信息，key为子集群ID，value为子集群信息和对应的RM客户端代理
  private final Map<SubClusterId, Pair<SubClusterInfo, ApplicationClientProtocol>> subClusters;
  // 联邦状态存储门面，用于查询应用归属子集群和子集群信息
  private FederationStateStoreFacade federationFacade;

  /**
   * 创建联邦应用报告获取器实例，初始化子集群缓存和联邦状态存储连接
   *
   * @param conf YARN配置
   */
  public FedAppReportFetcher(Configuration conf) {
    super(conf);
    subClusters = new ConcurrentHashMap<>();
    federationFacade = FederationStateStoreFacade.getInstance(conf);
  }

  /**
   * 根据应用ID获取应用报告，先查询应用归属子集群，再从对应子集群RM获取报告
   *
   * @param appId 应用ID
   * @return 应用报告
   * @throws YarnException YARN异常
   * @throws IOException 连接/IO异常
   */
  @Override
  public FetchedAppReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    // 查询应用所属的子集群ID
    SubClusterId scid = federationFacade.getApplicationHomeSubCluster(appId);
    // 如果子集群未连接则创建连接
    createSubclusterIfAbsent(scid);
    // 获取对应子集群的RM客户端代理
    ApplicationClientProtocol applicationsManager = subClusters.get(scid).getRight();

    // 调用父类方法通过代理获取应用报告
    return super.getApplicationReport(applicationsManager, appId);
  }

  /**
   * 获取应用对应RM Web页面的基础URL
   *
   * @param appId 应用ID
   * @return Web页面基础URL
   * @throws IOException 连接/IO异常
   * @throws YarnException YARN异常
   */
  @Override
  public String getRmAppPageUrlBase(ApplicationId appId)
      throws IOException, YarnException {
    // 查询应用所属的子集群ID
    SubClusterId scid = federationFacade.getApplicationHomeSubCluster(appId);
    // 如果子集群未连接则创建连接
    createSubclusterIfAbsent(scid);

    // 获取子集群信息
    SubClusterInfo subClusterInfo = subClusters.get(scid).getLeft();
    // 获取HTTP协议前缀
    String scheme = WebAppUtils.getHttpSchemePrefix(getConf());
    // 拼接应用页面基础URL
    return StringHelper.pjoin(scheme + subClusterInfo.getRMWebServiceAddress(), "cluster", "app");
  }

  /**
   * 如果子集群未缓存，则创建对应RM客户端代理并缓存
   *
   * @param scId 子集群ID
   * @throws YarnException YARN异常
   * @throws IOException 连接/IO异常
   */
  private void createSubclusterIfAbsent(SubClusterId scId) throws YarnException, IOException {
    // 已缓存直接返回
    if (subClusters.containsKey(scId)) {
      return;
    }
    // 从联邦状态存储获取子集群信息
    SubClusterInfo subClusterInfo = federationFacade.getSubCluster(scId);
    // 基于全局配置创建子集群专属配置
    Configuration subClusterConf = new Configuration(getConf());
    // 更新配置适配联邦子集群环境
    FederationProxyProviderUtil
        .updateConfForFederation(subClusterConf, subClusterInfo.getSubClusterId().toString());
    // 创建对应子集群RM的客户端代理
    ApplicationClientProtocol proxy =
        ClientRMProxy.createRMProxy(subClusterConf, ApplicationClientProtocol.class);
    // 缓存子集群信息和代理
    subClusters.put(scId, Pair.of(subClusterInfo, proxy));
  }

  /**
   * 停止所有子集群连接，关闭所有RPC代理
   */
  public void stop() {
    super.stop();
    // 遍历所有子集群代理，停止RPC连接释放资源
    for (Pair pair : this.subClusters.values()) {
      RPC.stopProxy(pair.getRight());
    }
  }

  /**
   * 测试用方法：手动注册子集群信息和代理，用于单元测试
   *
   * @param info 子集群信息
   * @param proxy 应用客户端代理
   */
  @VisibleForTesting
  public void registerSubCluster(SubClusterInfo info, ApplicationClientProtocol proxy) {
    subClusters.put(info.getSubClusterId(), Pair.of(info, proxy));
  }
}