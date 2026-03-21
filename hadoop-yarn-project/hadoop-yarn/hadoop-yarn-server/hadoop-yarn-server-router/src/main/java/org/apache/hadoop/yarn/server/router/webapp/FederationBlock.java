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

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.inject.Inject;
import org.glassfish.jersey.jettison.JettisonJaxbContext;
import org.glassfish.jersey.jettison.JettisonUnmarshaller;

/**
 * YARN联邦Router WebUI的子集群信息展示块，负责渲染所有子集群的概览信息表格
 */
class FederationBlock extends RouterBlock {

  private final Router router;

  @Inject
  FederationBlock(ViewContext ctx, Router router) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  public void render(Block html) {
    // 检查YARN联邦是否启用
    boolean isEnabled = isYarnFederationEnabled();

    // 初始化联邦页面HTML
    initHtmlPageFederation(html, isEnabled);
  }

  /**
   * 从RM返回的指标JSON中解析出集群指标信息.
   *
   * @param capability 从RM获取的指标JSON字符串
   * @return 解析后的集群指标对象
   */
  protected ClusterMetricsInfo getClusterMetricsInfo(String capability) {
    try {
      if (capability != null && !capability.isEmpty()) {
        JettisonJaxbContext jettisonJaxbContext = new JettisonJaxbContext(ClusterMetricsInfo.class);
        JettisonUnmarshaller jsonMarshaller = jettisonJaxbContext.createJsonUnmarshaller();
        ClusterMetricsInfo clusterMetricsInfo = jsonMarshaller.unmarshalFromJSON(
            new StringReader(capability), ClusterMetricsInfo.class);
        return clusterMetricsInfo;
      }
    } catch (Exception e) {
      LOG.error("Cannot parse SubCluster info", e);
    }
    return null;
  }

  /**
   * 初始化联邦页面子集群详情展示所需的JavaScript.
   * 该JS脚本负责处理用户点击子集群ID时，展示/隐藏子集群详情信息的逻辑
   * 会将所有子集群的详细指标数据传递给前端JS用于展示
   *
   * @param html html对象
   * @param subClusterDetailMap 子集群详情数据列表
   */
  private void initFederationSubClusterDetailTableJs(Block html,
      List<Map<String, String>> subClusterDetailMap) {
    Gson gson = new Gson();
    // 将子集群详情数据转为JSON注入页面供前端使用
    html.script().$type("text/javascript").
        __(" var scTableData = " + gson.toJson(subClusterDetailMap) + "; ")
        .__();
    // 加载联邦页面前端处理脚本
    html.script(root_url("static/federation/federation.js"));
  }

  /**
   * 初始化联邦页面HTML结构.
   *
   * @param html html对象
   * @param isEnabled YARN联邦是否启用
   */
  private void initHtmlPageFederation(Block html, boolean isEnabled) {
    List<Map<String, String>> lists = new ArrayList<>();

    // 创建表格表头
    TBODY<TABLE<Hamlet>> tbody =
        html.table("#rms").$class("cell-border").$style("width:100%").thead().tr()
        .th(".id", "SubCluster")
        .th(".state", "State")
        .th(".lastStartTime", "LastStartTime")
        .th(".lastHeartBeat", "LastHeartBeat")
        .th(".resources", "Resources")
        .th(".nodes", "Nodes")
        .__().__().tbody();

    try {
      if (isEnabled) {
        // 联邦启用，加载所有子集群信息
        initSubClusterPage(tbody, lists);
      } else {
        // 联邦未启用，仅展示本集群信息
        initLocalClusterPage(tbody, lists);
      }
    } catch (Exception e) {
      LOG.error("Cannot render Router Federation.", e);
    }

    // 初始化前端交互JS
    initFederationSubClusterDetailTableJs(html, lists);

    // 添加提示信息
    tbody.__().__().div().p().$style("color:red")
        .__("*The application counts are local per subcluster").__().__();
  }

  /**
   * 初始化本集群的页面展示（联邦未启用场景）.
   *
   * @param tbody HTML表格body
   * @param lists 子集群页面数据列表
   */
  private void initLocalClusterPage(TBODY<TABLE<Hamlet>> tbody, List<Map<String, String>> lists) {
    Configuration config = this.router.getConfig();
    SubClusterInfo localCluster = getSubClusterInfoByLocalCluster(config);
    if (localCluster != null) {
      try {
        initSubClusterPageItem(tbody, localCluster, lists);
      } catch (Exception e) {
        LOG.error("init LocalCluster = {} page data error.", localCluster, e);
      }
    }
  }

  /**
   * 初始化所有子集群的页面展示（联邦启用场景）.
   *
   * @param tbody HTML表格body
   * @param lists 子集群页面数据列表
   */
  private void initSubClusterPage(TBODY<TABLE<Hamlet>> tbody, List<Map<String, String>> lists) {
    // 获取排序后的子集群列表
    List<SubClusterInfo> subClusters = getSubClusterInfoList();

    // 遍历所有子集群，逐个渲染数据，遇到异常跳过该子集群
    for (SubClusterInfo subCluster : subClusters) {
      try {
        initSubClusterPageItem(tbody, subCluster, lists);
      } catch (Exception e) {
        LOG.error("init subCluster = {} page data error.", subCluster, e);
      }
    }
  }

  /**
   * 初始化单个子集群的页面数据行.
   *
   * @param tbody HTML表格body
   * @param subClusterInfo 子集群信息
   * @param lists 用于记录需要传递给前端JS展示的数据
   */
  private void initSubClusterPageItem(TBODY<TABLE<Hamlet>> tbody,
      SubClusterInfo subClusterInfo, List<Map<String, String>> lists) {

    Map<String, String> subClusterMap = new HashMap<>();

    // 获取子集群ID
    SubClusterId subClusterId = subClusterInfo.getSubClusterId();
    String subClusterIdText = subClusterId.getId();

    // 构建子集群RM Web服务链接
    String webAppAddress = subClusterInfo.getRMWebServiceAddress();
    String herfWebAppAddress = "";
    if (webAppAddress != null && !webAppAddress.isEmpty()) {
      herfWebAppAddress =
          WebAppUtils.getHttpSchemePrefix(this.router.getConfig()) + webAppAddress;
    }

    // 解析子集群指标信息
    String capability = subClusterInfo.getCapability();
    ClusterMetricsInfo subClusterMetricsInfo = getClusterMetricsInfo(capability);

    if (subClusterMetricsInfo == null) {
      return;
    }

    // 格式化启动时间和最后心跳时间
    Date lastStartTime = new Date(subClusterInfo.getLastStartTime());
    Date lastHeartBeat = new Date(subClusterInfo.getLastHeartBeat());

    // 格式化总资源信息
    long totalMB = subClusterMetricsInfo.getTotalMB();
    String totalMBDesc = StringUtils.byteDesc(totalMB * BYTES_IN_MB);
    long totalVirtualCores = subClusterMetricsInfo.getTotalVirtualCores();
    String resources = String.format("<memory:%s, vCores:%s>", totalMBDesc, totalVirtualCores);

    // 格式化节点信息
    long totalNodes = subClusterMetricsInfo.getTotalNodes();
    long activeNodes = subClusterMetricsInfo.getActiveNodes();
    String nodes = String.format("<totalNodes:%s, activeNodes:%s>", totalNodes, activeNodes);

    // 根据子集群状态设置不同字体颜色（运行中绿色，异常红色）
    String stateStyle = "color:#dc3545;font-weight:bolder";
    SubClusterState state = subClusterInfo.getState();
    if (SubClusterState.SC_RUNNING == state) {
      stateStyle = "color:#28a745;font-weight:bolder";
    }

    // 添加表格行
    tbody.tr().$id(subClusterIdText)
        .td().$class("details-control").a(herfWebAppAddress, subClusterIdText).__()
        .td().$style(stateStyle).__(state.name()).__()
        .td().__(lastStartTime).__()
        .td().__(lastHeartBeat).__()
        .td(resources)
        .td(nodes)
        .__();

    // 格式化各类内存指标
    long allocatedMB = subClusterMetricsInfo.getAllocatedMB();
    String allocatedMBDesc = StringUtils.byteDesc(allocatedMB * BYTES_IN_MB);
    long availableMB = subClusterMetricsInfo.getAvailableMB();
    String availableMBDesc = StringUtils.byteDesc(availableMB * BYTES_IN_MB);
    long pendingMB = subClusterMetricsInfo.getPendingMB();
    String pendingMBDesc = StringUtils.byteDesc(pendingMB * BYTES_IN_MB);
    long reservedMB = subClusterMetricsInfo.getReservedMB();
    String reservedMBDesc = StringUtils.byteDesc(reservedMB * BYTES_IN_MB);

    // 将详细信息存入Map供前端JS展开详情使用
    subClusterMap.put("totalmemory", totalMBDesc);
    subClusterMap.put("allocatedmemory", allocatedMBDesc);
    subClusterMap.put("availablememory", availableMBDesc);
    subClusterMap.put("pendingmemory", pendingMBDesc);
    subClusterMap.put("reservedmemory", reservedMBDesc);
    subClusterMap.put("subcluster", subClusterId.getId());
    subClusterMap.put("capability", capability);
    lists.add(subClusterMap);
  }

}