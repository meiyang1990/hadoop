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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodesInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.inject.Inject;

import javax.ws.rs.client.Client;
import java.util.Date;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_SC;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_LABEL;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_STATE;

/**
 * 路由器Web UI的节点信息块，负责在联邦YARN集群页面渲染节点列表。
 */
public class NodesBlock extends RouterBlock {

  private final Router router;

  @Inject
  NodesBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {
    // 检查YARN联邦功能是否启用
    boolean isEnabled = isYarnFederationEnabled();

    // 获取请求参数中的子集群名称
    String subClusterName = $(NODE_SC);
    // 获取请求参数中的节点状态过滤条件
    String state = $(NODE_STATE);
    // 获取请求参数中的节点标签过滤条件
    String nodeLabel = $(NODE_LABEL);

    NodesInfo nodesInfo;
    // 如果指定了有效子集群，则查询该子集群的节点列表
    if (subClusterName != null && !subClusterName.isEmpty() &&
        !ROUTER.equalsIgnoreCase(subClusterName)) {
      // 初始化子集群指标概览表格
      initSubClusterMetricsOverviewTable(html, subClusterName);
      // 获取指定子集群的节点信息
      nodesInfo = getSubClusterNodesInfo(subClusterName);
    } else {
      // 渲染全局指标概览表格
      html.__(MetricsOverviewTable.class);
      // 获取整个YARN联邦集群的节点信息
      nodesInfo = getYarnFederationNodesInfo(isEnabled);
    }

    // 根据过滤条件渲染节点列表表格
    initYarnFederationNodesOfCluster(nodesInfo, html, state, nodeLabel);
  }

  /**
   * 获取YARN联邦集群所有节点汇总信息。
   * @param isEnabled YARN联邦是否启用
   * @return 节点信息汇总对象
   */
  private NodesInfo getYarnFederationNodesInfo(boolean isEnabled) {
    Configuration config = this.router.getConfig();
    String webAddress;
    if (isEnabled) {
      // 联邦启用时从Router获取汇总节点信息
      webAddress = WebAppUtils.getRouterWebAppURLWithScheme(this.router.getConfig());
    } else {
      // 联邦未启用时从本地RM获取节点信息
      webAddress = WebAppUtils.getRMWebAppURLWithScheme(config);
    }
    // 通过REST API获取节点信息
    return getSubClusterNodesInfoByWebAddress(webAddress);
  }

  /**
   * 从指定子集群获取节点信息。
   * @param subCluster 子集群ID
   * @return 子集群节点信息汇总，出错返回null
   */
  private NodesInfo getSubClusterNodesInfo(String subCluster) {
    try {
      // 构造子集群ID对象
      SubClusterId subClusterId = SubClusterId.newInstance(subCluster);
      // 获取联邦状态存储门面实例
      FederationStateStoreFacade facade =
          FederationStateStoreFacade.getInstance(this.router.getConfig());
      // 从状态存储查询子集群信息
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);

      if (subClusterInfo != null) {
        // 获取子集群RM的Web服务地址
        String webAddress = subClusterInfo.getRMWebServiceAddress();
        String herfWebAppAddress;
        if (webAddress != null && !webAddress.isEmpty()) {
          // 拼接完整HTTP地址
          herfWebAppAddress =
              WebAppUtils.getHttpSchemePrefix(this.router.getConfig()) + webAddress;
          // 调用子集群RM接口获取节点信息
          return getSubClusterNodesInfoByWebAddress(herfWebAppAddress);
        }
      }
    } catch (Exception e) {
      LOG.error("get NodesInfo From SubCluster = {} error.", subCluster, e);
    }
    return null;
  }

  /**
   * 通过指定Web地址调用REST API获取节点信息。
   * @param webAddress 目标Web服务地址
   * @return 节点信息汇总对象，请求失败返回null
   */
  private NodesInfo getSubClusterNodesInfoByWebAddress(String webAddress) {
    Configuration conf = this.router.getConfig();
    // 创建Jersey客户端实例
    Client client = RouterWebServiceUtil.createJerseyClient(conf);
    // 转发请求并解析响应
    NodesInfo nodes = RouterWebServiceUtil
        .genericForward(webAddress, null, NodesInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.NODES, null, null, conf,
        client);
    // 关闭客户端释放资源
    client.close();
    return nodes;
  }

  /**
   * 根据过滤条件渲染节点列表表格。
   * @param nodesInfo 节点信息汇总
   * @param html HTML块输出对象
   * @param filterState 节点状态过滤条件
   * @param filterLabel 节点标签过滤条件
   */
  private void initYarnFederationNodesOfCluster(NodesInfo nodesInfo, Block html,
      String filterState, String filterLabel) {
    // 创建节点表格并初始化表头
    TBODY<TABLE<Hamlet>> tbody = html.table("#nodes").thead().tr()
        .th(".nodelabels", "Node Labels")
        .th(".rack", "Rack")
        .th(".state", "Node State")
        .th(".nodeaddress", "Node Address")
        .th(".nodehttpaddress", "Node HTTP Address")
        .th(".lastHealthUpdate", "Last health-update")
        .th(".healthReport", "Health-report")
        .th(".containers", "Containers")
        .th(".mem", "Mem Used")
        .th(".mem", "Mem Avail")
        .th(".vcores", "VCores Used")
        .th(".vcores", "VCores Avail")
        .th(".nodeManagerVersion", "Version")
        .__().__().tbody();

    // 遍历所有节点生成表格行
    if (nodesInfo != null && CollectionUtils.isNotEmpty(nodesInfo.getNodes())) {
      for (NodeInfo info : nodesInfo.getNodes()) {
        // 按节点状态过滤，不匹配则跳过
        if (filterState != null && !filterState.isEmpty() && !filterState.equals(info.getState())) {
          continue;
        }

        // 按节点标签过滤，不匹配则跳过
        if (!filterLabel.equals(RMNodeLabelsManager.ANY)) {
          if (filterLabel.isEmpty()) {
            // 空标签过滤仅展示无标签节点
            if (!info.getNodeLabels().isEmpty()) {
              continue;
            }
          } else if (!info.getNodeLabels().contains(filterLabel)) {
            // 仅展示包含指定标签的节点
            continue;
          }
        }

        // 计算内存使用量
        int usedMemory = (int) info.getUsedMemory();
        int availableMemory = (int) info.getAvailableMemory();
        // 创建新表格行
        TR<TBODY<TABLE<Hamlet>>> row = tbody.tr();
        // 输出节点标签
        row.td().__(StringUtils.join(",", info.getNodeLabels())).__();
        // 输出机架信息
        row.td().__(info.getRack()).__();
        // 输出节点状态
        row.td().__(info.getState()).__();
        // 输出节点ID
        row.td().__(info.getNodeId()).__();
        boolean isInactive = false;
        if (isInactive) {
          row.td().__(UNAVAILABLE).__();
        } else {
          String httpAddress = info.getNodeHTTPAddress();
          String herfWebAppAddress = "";
          if (httpAddress != null && !httpAddress.isEmpty()) {
            // 拼接节点NM的完整HTTP地址
            herfWebAppAddress =
                WebAppUtils.getHttpSchemePrefix(this.router.getConfig()) + httpAddress;
          }
          // 输出带链接的节点HTTP地址
          row.td().a(herfWebAppAddress, httpAddress).__();
        }

        // 输出最后健康检查时间
        row.td().br().$title(String.valueOf(info.getLastHealthUpdate())).__()
            .__(new Date(info.getLastHealthUpdate())).__()
            // 输出健康检查报告
            .td(info.getHealthReport())
            // 输出容器数量
            .td(String.valueOf(info.getNumContainers())).td().br()
            .$title(String.valueOf(usedMemory)).__()
            // 输出已用内存容量
            .__(StringUtils.byteDesc(usedMemory * BYTES_IN_MB)).__().td().br()
            .$title(String.valueOf(availableMemory)).__()
            // 输出可用内存容量
            .__(StringUtils.byteDesc(availableMemory * BYTES_IN_MB)).__()
            // 输出已用vCore数量
            .td(String.valueOf(info.getUsedVirtualCores()))
            // 输出可用vCore数量
            .td(String.valueOf(info.getAvailableVirtualCores()))
            // 输出NodeManager版本
            .td(info.getVersion()).__();
      }
    }

    // 结束表格渲染
    tbody.__().__();
  }
}