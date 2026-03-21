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

import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.PartitionInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import javax.ws.rs.client.Client;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_SC;

/**
 *  Router Web UI 节点标签页面区块，负责渲染联邦集群下的节点标签列表
 */
public class NodeLabelsBlock extends RouterBlock {

  private Router router;

  @Inject
  public NodeLabelsBlock(Router router, ViewContext ctx) {
    super(router, ctx);
    this.router = router;
  }

  @Override
  protected void render(Block html) {
    // 检查YARN联邦模式是否启用
    boolean isEnabled = isYarnFederationEnabled();

    // 获取请求参数中的子集群名称
    String subClusterName = $(NODE_SC);

    NodeLabelsInfo nodeLabelsInfo = null;
    if (StringUtils.isNotEmpty(subClusterName)) {
      // 获取指定子集群的节点标签信息
      nodeLabelsInfo = getSubClusterNodeLabelsInfo(subClusterName);
    } else {
      // 根据联邦状态获取对应集群的节点标签信息
      nodeLabelsInfo = getYarnFederationNodeLabelsInfo(isEnabled);
    }

    // 渲染节点标签表格到页面
    initYarnFederationNodeLabelsOfCluster(nodeLabelsInfo, html);
  }

  /**
   * 根据指定子集群获取节点标签信息.
   * @param subCluster 子集群ID
   * @return 节点标签信息
   */
  private NodeLabelsInfo getSubClusterNodeLabelsInfo(String subCluster) {
    try {
      SubClusterId subClusterId = SubClusterId.newInstance(subCluster);
      FederationStateStoreFacade facade =
          FederationStateStoreFacade.getInstance(router.getConfig());
      // 从联邦状态存储获取子集群信息
      SubClusterInfo subClusterInfo = facade.getSubCluster(subClusterId);

      if (subClusterInfo != null) {
        // 构造子集群RM Web服务地址
        String webAddress = subClusterInfo.getRMWebServiceAddress();
        String herfWebAppAddress = "";
        if (webAddress != null && !webAddress.isEmpty()) {
          herfWebAppAddress =
              WebAppUtils.getHttpSchemePrefix(this.router.getConfig()) + webAddress;
          // 远程调用子集群RM获取节点标签
          return getSubClusterNodeLabelsByWebAddress(herfWebAppAddress);
        }
      }
    } catch (Exception e) {
      LOG.error("get NodeLabelsInfo From SubCluster = {} error.", subCluster, e);
    }
    return null;
  }

  /**
   * 获取节点标签信息，联邦模式开启则聚合多个子集群，否则获取本地集群节点标签.
   *
   * @param isEnabled 是否开启联邦模式，true为开启，false为关闭
   *
   * @return 节点标签信息
   */
  private NodeLabelsInfo getYarnFederationNodeLabelsInfo(boolean isEnabled) {
    Configuration config = this.router.getConfig();
    String webAddress;
    if (isEnabled) {
      // 联邦模式下获取Router自身的Web服务地址
      webAddress = WebAppUtils.getRouterWebAppURLWithScheme(config);
    } else {
      // 非联邦模式下获取本地RM的Web服务地址
      webAddress = WebAppUtils.getRMWebAppURLWithScheme(config);
    }
    // 从对应地址获取节点标签
    return getSubClusterNodeLabelsByWebAddress(webAddress);
  }

  /**
   * 根据指定Web地址远程获取节点标签信息.
   *
   * @param webAddress RM Web服务地址
   * @return 节点标签信息
   */
  private NodeLabelsInfo getSubClusterNodeLabelsByWebAddress(String webAddress) {
    Configuration conf = this.router.getConfig();
    // 创建Jersey REST客户端
    Client client = RouterWebServiceUtil.createJerseyClient(conf);
    // 转发请求到目标RM获取节点标签信息
    NodeLabelsInfo nodes = RouterWebServiceUtil
        .genericForward(webAddress, null, NodeLabelsInfo.class, HTTPMethods.GET,
        RMWSConsts.RM_WEB_SERVICE_PATH + RMWSConsts.GET_RM_NODE_LABELS, null, null, conf,
        client);
    // 关闭客户端释放资源
    client.close();
    return nodes;
  }

  /**
   * 初始化渲染页面节点标签表格.
   *
   * @param nodeLabelsInfo 节点标签信息
   * @param html 页面区块对象
   */
  private void initYarnFederationNodeLabelsOfCluster(NodeLabelsInfo nodeLabelsInfo, Block html) {
    // 创建表格并初始化表头
    Hamlet.TBODY<Hamlet.TABLE<Hamlet>> tbody = html.table("#nodelabels").
        thead().
        tr().
        th(".name", "Label Name").
        th(".type", "Label Type").
        th(".numOfActiveNMs", "Num Of Active NMs").
        th(".totalResource", "Total Resource").
        __().__().
        tbody();

    if (nodeLabelsInfo != null) {
      // 遍历所有节点标签生成表格行
      for (NodeLabelInfo info : nodeLabelsInfo.getNodeLabelsInfo()) {
        Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Hamlet>>> row =
            tbody.tr().td(info.getName().isEmpty() ?
            NodeLabel.DEFAULT_NODE_LABEL_PARTITION : info.getName());
        // 输出标签独占类型
        String type = (info.getExclusivity()) ? "Exclusive Partition" : "Non Exclusive Partition";
        row = row.td(type);
        int nActiveNMs = info.getActiveNMs();
        if (nActiveNMs > 0) {
          // 活跃节点数大于0时添加跳转到节点列表的链接
          row = row.td().a(url("nodes",
              "?" + YarnWebParams.NODE_LABEL + "=" + info.getName()), String.valueOf(nActiveNMs))
              .__();
        } else {
          row = row.td(String.valueOf(nActiveNMs));
        }

        // 输出分区可用资源信息
        PartitionInfo partitionInfo = info.getPartitionInfo();
        ResourceInfo available = partitionInfo.getResourceAvailable();
        row.td(available.toFormattedString()).__();
      }
    }

    // 结束表格渲染
    tbody.__().__();
  }
}