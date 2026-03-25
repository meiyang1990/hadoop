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

import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerOverviewInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterClusterMetrics;
import org.apache.hadoop.yarn.server.router.webapp.dao.RouterSchedulerMetrics;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import javax.ws.rs.client.Client;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * YARN联邦Router WebUI指标概览表格渲染组件，负责聚合展示整个联邦集群或指定子集群的指标信息。
 * 包含应用指标、节点指标、调度器指标三类数据的聚合展示。
 */
public class MetricsOverviewTable extends RouterBlock {

  private final Router router;

  @Inject
  MetricsOverviewTable(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {
    // 初始化页面样式
    html.style(".metrics {margin-bottom:5px}");

    // 获取聚合后的Router集群指标信息
    ClusterMetricsInfo routerClusterMetricsInfo = getRouterClusterMetricsInfo();
    RouterClusterMetrics routerClusterMetrics = new RouterClusterMetrics(routerClusterMetricsInfo);

    // 创建指标容器div
    Hamlet.DIV<Hamlet> div = html.div().$class("metrics");
    try {
      // 初始化联邦集群应用指标表格
      initFederationClusterAppsMetrics(div, routerClusterMetrics);
      // 初始化联邦集群节点指标表格
      initFederationClusterNodesMetrics(div, routerClusterMetrics);
      // 获取所有可用子集群信息
      List<SubClusterInfo> subClusters = getSubClusterInfoList();
      // 初始化联邦集群调度器指标表格
      initFederationClusterSchedulersMetrics(div, routerClusterMetrics, subClusters);
    } catch (Exception e) {
      LOG.error("MetricsOverviewTable init error.", e);
    }
    div.__();
  }

  /**
   * 渲染指定子集群的指标概览表格。
   * @param html HTML块对象
   * @param subClusterId 目标子集群ID
   */
  protected void render(Block html, String subClusterId) {
    // 初始化页面样式
    html.style(".metrics {margin-bottom:5px}");

    // 获取指定子集群的指标信息
    ClusterMetricsInfo clusterMetricsInfo =
        getClusterMetricsInfoBySubClusterId(subClusterId);
    RouterClusterMetrics routerClusterMetrics =
        new RouterClusterMetrics(clusterMetricsInfo, subClusterId);

    // 创建指标容器div
    Hamlet.DIV<Hamlet> div = html.div().$class("metrics");
    try {
      // 初始化子集群应用指标表格
      initFederationClusterAppsMetrics(div, routerClusterMetrics);
      // 初始化子集群节点指标表格
      initFederationClusterNodesMetrics(div, routerClusterMetrics);
      // 获取指定子集群信息
      Collection<SubClusterInfo> subClusters = getSubClusterInfoList(subClusterId);
      // 初始化子集群调度器指标表格
      initFederationClusterSchedulersMetrics(div, routerClusterMetrics, subClusters);
    } catch (Exception e) {
      LOG.error("MetricsOverviewTable init error.", e);
    }
    div.__();
  }

  /**
   * 初始化联邦集群应用指标表格，包含应用数量、容器、资源使用等信息。
   *
   * @param div 数据展示容器div
   * @param metrics 聚合后的集群指标数据
   */
  private void initFederationClusterAppsMetrics(Hamlet.DIV<Hamlet> div,
      RouterClusterMetrics metrics) {
    div.h3(metrics.getWebPageTitlePrefix() + " Cluster Metrics").
        table("#metricsoverview").
        thead().$class("ui-widget-header").
        // 初始化表头信息
        tr().
        th().$class("ui-state-default").__("Apps Submitted").__().
        th().$class("ui-state-default").__("Apps Pending").__().
        th().$class("ui-state-default").__("Apps Running").__().
        th().$class("ui-state-default").__("Apps Completed").__().
        th().$class("ui-state-default").__("Containers Running").__().
        th().$class("ui-state-default").__("Used Resources").__().
        th().$class("ui-state-default").__("Total Resources").__().
        th().$class("ui-state-default").__("Reserved Resources").__().
        th().$class("ui-state-default").__("Physical Mem Used %").__().
        th().$class("ui-state-default").__("Physical VCores Used %").__().
        __().
        __().
        // 初始化表格数据
        tbody().$class("ui-widget-content").
        tr().
        td(metrics.getAppsSubmitted()).
        td(metrics.getAppsPending()).
        td(String.valueOf(metrics.getAppsRunning())).
        td(metrics.getAppsCompleted()).
        td(metrics.getAllocatedContainers()).
        td(metrics.getUsedResources()).
        td(metrics.getTotalResources()).
        td(metrics.getReservedResources()).
        td(metrics.getUtilizedMBPercent()).
        td(metrics.getUtilizedVirtualCoresPercent()).
        __().
        __().__();
  }

  /**
   * 初始化联邦集群节点指标表格，包含不同状态节点的数量统计。
   *
   * @param div 数据展示容器div
   * @param metrics 聚合后的集群指标数据
   */
  private void initFederationClusterNodesMetrics(Hamlet.DIV<Hamlet> div,
      RouterClusterMetrics metrics) {
    div.h3(metrics.getWebPageTitlePrefix() + " Cluster Nodes Metrics").
        table("#nodemetricsoverview").
        thead().$class("ui-widget-header").
        // 初始化表头信息
        tr().
        th().$class("ui-state-default").__("Active Nodes").__().
        th().$class("ui-state-default").__("Decommissioning Nodes").__().
        th().$class("ui-state-default").__("Decommissioned Nodes").__().
        th().$class("ui-state-default").__("Lost Nodes").__().
        th().$class("ui-state-default").__("Unhealthy Nodes").__().
        th().$class("ui-state-default").__("Rebooted Nodes").__().
        th().$class("ui-state-default").__("Shutdown Nodes").__().
        __().
        __().
        // 初始化表格数据
        tbody().$class("ui-widget-content").
        tr().
        td().a(url("nodes"), String.valueOf(metrics.getActiveNodes())).__().
        td().a(url("nodes/router/?node.state=decommissioning"),
            String.valueOf(metrics.getDecommissioningNodes())).__().
        td().a(url("nodes/router/?node.state=decommissioned"),
            String.valueOf(metrics.getDecommissionedNodes())).__().
        td().a(url("nodes/router/?node.state=lost"),
            String.valueOf(metrics.getLostNodes())).__().
        td().a(url("nodes/router/?node.state=unhealthy"),
            String.valueOf(metrics.getUnhealthyNodes())).__().
        td().a(url("nodes/router/?node.state=rebooted"),
            String.valueOf(metrics.getRebootedNodes())).__().
        td().a(url("nodes/router/?node.state=shutdown"),
            String.valueOf(metrics.getShutdownNodes())).__().
        __().
        __().__();
  }

  /**
   * 初始化联邦集群调度器指标表格，聚合展示各个子集群的调度器信息。
   *
   * @param div 数据展示容器div
   * @param metrics 聚合后的集群指标数据
   * @param subclusters 活跃子集群列表
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  private void initFederationClusterSchedulersMetrics(Hamlet.DIV<Hamlet> div,
      RouterClusterMetrics metrics, Collection<SubClusterInfo> subclusters)
      throws YarnException, IOException, InterruptedException {

    Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr =
        div.h3(metrics.getWebPageTitlePrefix() + " Scheduler Metrics").
        table("#schedulermetricsoverview").
        thead().$class("ui-widget-header").
        tr().
        th().$class("ui-state-default").__("SubCluster").__().
        th().$class("ui-state-default").__("Scheduler Type").__().
        th().$class("ui-state-default").__("Scheduling Resource Type").__().
        th().$class("ui-state-default").__("Minimum Allocation").__().
        th().$class("ui-state-default").__("Maximum Allocation").__().
        th().$class("ui-state-default").__("Maximum Cluster Application Priority").__().
        th().$class("ui-state-default").__("Scheduler Busy %").__().
        th().$class("ui-state-default").__("RM Dispatcher EventQueue Size").__().
        th().$class("ui-state-default")
        .__("Scheduler Dispatcher EventQueue Size").__().
        __().
        __().
        tbody().$class("ui-widget-content");

    // 检查YARN联邦是否启用
    boolean isEnabled = isYarnFederationEnabled();

    // 如果未启用联邦或没有可用子集群，显示N/A
    if (!isEnabled) {
      initLocalClusterOverViewTable(fsMetricsScheduleTr);
    } else if (subclusters != null && !subclusters.isEmpty()) {
      initSubClusterOverViewTable(metrics, fsMetricsScheduleTr, subclusters);
    } else {
      showRouterSchedulerMetricsData(UNAVAILABLE, fsMetricsScheduleTr);
    }

    fsMetricsScheduleTr.__().__();
  }

  /**
   * 初始化本地集群（非联邦模式）的调度器概览表格。
   *
   * @param fsMetricsScheduleTr 表格tbody对象
   */
  private void initLocalClusterOverViewTable(
      Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr) {
    // 获取配置
    Configuration config = this.router.getConfig();
    Client client = RouterWebServiceUtil.createJerseyClient(config);
    String webAppAddress = WebAppUtils.getRMWebAppURLWithScheme(config);

    // 获取本地集群名称
    String localClusterName = config.get(YarnConfiguration.RM_CLUSTER_ID, UNAVAILABLE);
    SchedulerOverviewInfo schedulerOverviewInfo =
        getSchedulerOverviewInfo(webAppAddress, config, client);
    if (schedulerOverviewInfo != null) {
      RouterSchedulerMetrics rsMetrics =
          new RouterSchedulerMetrics(localClusterName, schedulerOverviewInfo);
      // 展示调度器指标数据
      showRouterSchedulerMetricsData(rsMetrics, fsMetricsScheduleTr);
    } else {
      showRouterSchedulerMetricsData(localClusterName, fsMetricsScheduleTr);
    }
  }

  /**
   * 初始化联邦模式下多个子集群的调度器概览表格。
   *
   * @param metrics Router集群聚合指标
   * @param fsMetricsScheduleTr 表格tbody对象
   * @param subClusters 子集群列表
   */
  private void initSubClusterOverViewTable(RouterClusterMetrics metrics,
      Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr,
      Collection<SubClusterInfo> subClusters) {

    // 获取配置
    Configuration config = this.router.getConfig();

    Client client = RouterWebServiceUtil.createJerseyClient(config);

    // 遍历所有子集群获取调度信息
    for (SubClusterInfo subcluster : subClusters) {
      // 确保子集群ID不为空
      if (subcluster != null && subcluster.getSubClusterId() != null) {
        // 构造子集群RM Web服务地址
        String webAppAddress =  WebAppUtils.getHttpSchemePrefix(config) +
            subcluster.getRMWebServiceAddress();
        // 调用子集群RM接口获取调度概览信息
        SchedulerOverviewInfo schedulerOverviewInfo =
            getSchedulerOverviewInfo(webAppAddress, config, client);

        // 获取成功则展示该子集群调度信息
        if (schedulerOverviewInfo != null) {
          RouterSchedulerMetrics rsMetrics =
              new RouterSchedulerMetrics(subcluster, metrics, schedulerOverviewInfo);
          // 展示调度器指标数据
          showRouterSchedulerMetricsData(rsMetrics, fsMetricsScheduleTr);
        }
      }
    }

    client.close();
  }

  /**
   * 从指定RM Web地址获取调度器概览信息。
   *
   * @param webAppAddress RM Web服务地址
   * @param config 配置对象
   * @param client Jersey客户端
   * @return 调度器概览信息，失败返回null
   */
  private SchedulerOverviewInfo getSchedulerOverviewInfo(String webAppAddress,
      Configuration config, Client client) {
    try {
      SchedulerOverviewInfo schedulerOverviewInfo = RouterWebServiceUtil
          .genericForward(webAppAddress, null, SchedulerOverviewInfo.class, HTTPMethods.GET,
          RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.SCHEDULER_OVERVIEW, null, null,
           config, client);
      return schedulerOverviewInfo;
    } catch (Exception e) {
      LOG.error("get SchedulerOverviewInfo from webAppAddress = {} error.",
          webAppAddress, e);
      return null;
    }
  }

  /**
   * 在表格中添加一行调度器指标数据。
   *
   * @param rsMetrics 调度器指标数据
   * @param fsMetricsScheduleTr 表格tbody对象
   */
  private void showRouterSchedulerMetricsData(RouterSchedulerMetrics rsMetrics,
      Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr) {
    // 添加一行指标数据
    fsMetricsScheduleTr.tr().
        td(rsMetrics.getSubCluster()).
        td(rsMetrics.getSchedulerType()).
        td(rsMetrics.getSchedulingResourceType()).
        td(rsMetrics.getMinimumAllocation()).
        td(rsMetrics.getMaximumAllocation()).
        td(rsMetrics.getApplicationPriority()).
        td(rsMetrics.getSchedulerBusy()).
        td(rsMetrics.getRmDispatcherEventQueueSize()).
        td(rsMetrics.getSchedulerDispatcherEventQueueSize()).
        __();
  }

  /**
   * 在表格中添加一行占位数据（全部为N/A），用于数据不可用场景。
   *
   * @param subClusterId 子集群ID
   * @param fsMetricsScheduleTr 表格tbody对象
   */
  private void showRouterSchedulerMetricsData(String subClusterId,
      Hamlet.TBODY<Hamlet.TABLE<Hamlet.DIV<Hamlet>>> fsMetricsScheduleTr) {
    String subCluster = StringUtils.isNotBlank(subClusterId) ? subClusterId : UNAVAILABLE;
    fsMetricsScheduleTr.tr().
        td(subCluster).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE).
        td(UNAVAILABLE)
        .__();
  }
}