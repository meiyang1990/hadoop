// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.glassfish.jersey.jettison.JettisonJaxbContext;
import org.glassfish.jersey.jettison.JettisonMarshaller;

import javax.ws.rs.client.Client;
import java.io.StringWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

/**
 * YARN Router Web UI 基础区块抽象基类，提供联邦集群下多个子集群的页面构建基础能力
 * 为Router的各个Web页面区块提供通用工具方法和数据获取能力
 */
public abstract class RouterBlock extends HtmlBlock {

  private final Router router;
  private final ViewContext ctx;
  private final FederationStateStoreFacade facade;
  private final Configuration conf;

  public static final String ROUTER = "router";

  /**
   * 构造RouterBlock，初始化依赖组件
   * @param router Router服务实例
   * @param ctx 视图上下文
   */
  public RouterBlock(Router router, ViewContext ctx) {
    super(ctx);
    this.ctx = ctx;
    this.router = router;
    this.facade = FederationStateStoreFacade.getInstance(router.getConfig());
    this.conf = this.router.getConfig();
  }

  /**
   * 获取Router聚合后的集群指标信息
   * @return 聚合后的集群指标信息
   */
  protected ClusterMetricsInfo getRouterClusterMetricsInfo() {
    boolean isEnabled = isYarnFederationEnabled();
    String webAppAddress;
    if(isEnabled) {
      webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(conf);
    } else {
      webAppAddress = WebAppUtils.getRMWebAppURLWithScheme(conf);
    }
    return getClusterMetricsInfo(webAppAddress);
  }

  /**
   * 根据指定地址获取集群指标信息
   * @param webAppAddress 目标Web服务地址
   * @return 集群指标信息
   */
  protected ClusterMetricsInfo getClusterMetricsInfo(String webAppAddress) {
    // 如果地址为空直接返回null
    if (StringUtils.isBlank(webAppAddress)) {
      return null;
    }

    // 创建REST客户端调用指标接口
    Client client = RouterWebServiceUtil.createJerseyClient(conf);
    ClusterMetricsInfo metrics = RouterWebServiceUtil
        .genericForward(webAppAddress, null, ClusterMetricsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
        conf, client);
    client.close();
    return metrics;
  }

  /**
   * 获取所有子集群信息列表，按子集群ID排序
   * @return 所有子集群信息列表
   */
  protected List<SubClusterInfo> getSubClusterInfoList() {
    List<SubClusterInfo> subClusters = new ArrayList<>();
    try {
      Map<SubClusterId, SubClusterInfo> subClustersInfo = facade.getSubClusters(true);
      // 将所有子集群加入列表并按ID排序
      subClusters.addAll(subClustersInfo.values());
      Comparator<? super SubClusterInfo> cmp = Comparator.comparing(o -> o.getSubClusterId());
      Collections.sort(subClusters, cmp);

      return subClusters;
    } catch (YarnException e) {
      LOG.error("getSubClusterInfoList error.", e);
      return subClusters;
    }
  }

  /**
   * 检查YARN联邦模式是否启用
   * @return true表示已启用联邦，false表示未启用
   */
  protected boolean isYarnFederationEnabled() {
    boolean isEnabled = conf.getBoolean(
        YarnConfiguration.FEDERATION_ENABLED,
        YarnConfiguration.DEFAULT_FEDERATION_ENABLED);
    return isEnabled;
  }

  /**
   * 获取所有活跃子集群的ID列表
   * @return 活跃子集群ID列表
   */
  protected List<String> getActiveSubClusterIds() {
    List<String> result = new ArrayList<>();
    try {
      Map<SubClusterId, SubClusterInfo> subClustersInfo = facade.getSubClusters(true);
      subClustersInfo.values().stream().forEach(subClusterInfo -> {
        result.add(subClusterInfo.getSubClusterId().getId());
      });
    } catch (Exception e) {
      LOG.error("getActiveSubClusters error.", e);
    }
    return result;
  }

  /**
   * 初始化指定子集群的指标概览表格
   * @param html HTML块对象
   * @param subclusterId 目标子集群ID
   */
  protected void initSubClusterMetricsOverviewTable(Block html, String subclusterId) {
    MetricsOverviewTable metricsOverviewTable = new MetricsOverviewTable(this.router, this.ctx);
    metricsOverviewTable.render(html, subclusterId);
  }

  /**
   * 根据子集群ID获取对应子集群RM的指标信息
   * @param subclusterId 目标子集群ID
   * @return 子集群指标信息，获取失败返回null
   */
  protected ClusterMetricsInfo getClusterMetricsInfoBySubClusterId(String subclusterId) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subclusterId);
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);
      if (subClusterInfo != null) {
        Client client = RouterWebServiceUtil.createJerseyClient(this.conf);
        // 调用子集群RM接口获取指标信息
        String webAppAddress =  WebAppUtils.getHttpSchemePrefix(this.conf) +
            subClusterInfo.getRMWebServiceAddress();
        ClusterMetricsInfo metrics = RouterWebServiceUtil
            .genericForward(webAppAddress, null, ClusterMetricsInfo.class, HTTPMethods.GET,
            RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
            conf, client);
        client.close();
        return metrics;
      }
    } catch (Exception e) {
      LOG.error("getClusterMetricsInfoBySubClusterId subClusterId = {} error.", subclusterId, e);
    }
    return null;
  }

  /**
   * 根据子集群ID获取对应子集群信息
   * @param subclusterId 目标子集群ID
   * @return 仅包含目标子集群的单元素集合，获取失败返回null
   */
  protected Collection<SubClusterInfo> getSubClusterInfoList(String subclusterId) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subclusterId);
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);
      return Collections.singletonList(subClusterInfo);
    } catch (Exception e) {
      LOG.error("getSubClusterInfoList subClusterId = {} error.", subclusterId, e);
    }
    return null;
  }

  /**
   * 获取联邦状态存储门面实例
   * @return 联邦状态存储门面实例
   */
  public FederationStateStoreFacade getFacade() {
    return facade;
  }

  /**
   * 初始化导航栏中的Nodes菜单，联邦模式下按子集群分组展示
   * @param mainList 父级UL列表对象
   * @param subClusterIds 活跃子集群ID列表
   */
  protected void initNodesMenu(Hamlet.UL<Hamlet.DIV<Hamlet>> mainList,
      List<String> subClusterIds) {
    if (CollectionUtils.isNotEmpty(subClusterIds)) {
      Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>> nodesList =
          mainList.li().a(url("nodes"), "Nodes").ul().
          $style("padding:0.3em 1em 0.1em 2em");

      // 添加每个子集群的Nodes入口链接
      nodesList.li().__();
      for (String subClusterId : subClusterIds) {
        nodesList.li().a(url("nodes", subClusterId), subClusterId).__();
      }
      nodesList.__().__();
    } else {
      mainList.li().a(url("nodes"), "Nodes").__();
    }
  }

  /**
   * 初始化导航栏中的Applications菜单，联邦模式下按子集群+应用状态分组展示
   * @param mainList 父级UL列表对象
   * @param subClusterIds 活跃子集群ID列表
   */
  protected void initApplicationsMenu(Hamlet.UL<Hamlet.DIV<Hamlet>> mainList,
      List<String> subClusterIds) {
    if (CollectionUtils.isNotEmpty(subClusterIds)) {
      Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>> apps =
          mainList.li().a(url("apps"), "Applications").ul();
      apps.li().__();
      for (String subClusterId : subClusterIds) {
        Hamlet.LI<Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>>> subClusterList = apps.
            li().a(url("apps", subClusterId), subClusterId);
        Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>>>> subAppStates =
            subClusterList.ul().$style("padding:0.3em 1em 0.1em 2em");
        subAppStates.li().__();
        // 为每个应用状态添加筛选链接
        for (YarnApplicationState state : YarnApplicationState.values()) {
          subAppStates.
              li().a(url("apps", subClusterId, state.toString()), state.toString()).__();
        }
        subAppStates.li().__().__();
        subClusterList.__();
      }
      apps.__().__();
    } else {
      mainList.li().a(url("apps"), "Applications").__();
    }
  }

  /**
   * 初始化导航栏中的Node Labels菜单，联邦模式下按子集群分组展示
   * @param mainList 父级UL列表对象
   * @param subClusterIds 活跃子集群ID列表
   */
  protected void initNodeLabelsMenu(Hamlet.UL<Hamlet.DIV<Hamlet>> mainList,
      List<String> subClusterIds) {

    if (CollectionUtils.isNotEmpty(subClusterIds)) {
      Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Hamlet>>>> nodesList =
          mainList.li().a(url("nodelabels"), "Node Labels").ul().
          $style("padding:0.3em 1em 0.1em 2em");

      // 添加每个子集群的Node Labels入口链接
      nodesList.li().__();
      for (String subClusterId : subClusterIds) {
        nodesList.li().a(url("nodelabels", subClusterId), subClusterId).__();
      }
      nodesList.__().__();
    } else {
      mainList.li().a(url("nodelabels"), "Node Labels").__();
    }
  }

  /**
   * 根据本地集群信息生成SubClusterInfo对象，用于非联邦模式兼容
   * @param config 配置对象
   * @return 本地集群对应的SubClusterInfo，获取失败返回null
   */
  protected SubClusterInfo getSubClusterInfoByLocalCluster(Configuration config) {

    Client client = null;
    try {

      // 第一步：获取本地集群名称和指标信息
      String localClusterName = config.get(YarnConfiguration.RM_CLUSTER_ID, UNAVAILABLE);
      String webAppAddress = WebAppUtils.getRMWebAppURLWithScheme(config);
      String rmWebAppURLWithoutScheme = WebAppUtils.getRMWebAppURLWithoutScheme(config);
      client = RouterWebServiceUtil.createJerseyClient(config);
      ClusterMetricsInfo clusterMetricsInfos = RouterWebServiceUtil
          .genericForward(webAppAddress, null, ClusterMetricsInfo.class, HTTPMethods.GET,
          RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.METRICS, null, null,
           config, client);

      if (clusterMetricsInfos == null) {
        return null;
      }

      // 第二步：获取本地集群基本信息，获取启动时间
      ClusterInfo clusterInfo = RouterWebServiceUtil.genericForward(webAppAddress, null,
          ClusterInfo.class, HTTPMethods.GET, RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.INFO,
          null, null, config, client);

      if (clusterInfo == null) {
        return null;
      }

      // 第三步：将集群指标序列化为JSON字符串作为子集群能力描述
      JettisonJaxbContext jc = new JettisonJaxbContext(ClusterMetricsInfo.class);
      JettisonMarshaller marshaller = jc.createJsonMarshaller();
      StringWriter writer = new StringWriter();
      marshaller.marshallToJSON(clusterMetricsInfos, writer);
      String capability = writer.toString();

      // 第四步：构造SubClusterInfo对象返回
      SubClusterId subClusterId = SubClusterId.newInstance(localClusterName);
      SubClusterInfo subClusterInfo = SubClusterInfo.newInstance(subClusterId,
          rmWebAppURLWithoutScheme, SubClusterState.SC_RUNNING, clusterInfo.getStartedOn(),
          Time.now(), capability);

      return subClusterInfo;
    } catch (Exception e) {
      LOG.error("An error occurred while parsing the local YARN cluster.", e);
    } finally {
      if (client != null) {
        client.close();
      }
    }
    return null;
  }
}