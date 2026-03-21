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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.google.inject.Inject;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.Collection;
import java.util.Map;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_LABEL;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

/**
 * YARN ResourceManager WebUI 节点列表页面，展示集群所有NodeManager节点信息，支持按节点状态和标签过滤
 */
class NodesPage extends RmView {

  /**
   * 节点列表表格内容渲染块，负责生成节点表格的HTML和数据
   */
  static class NodesBlock extends HtmlBlock {
    final ResourceManager rm;
    private static final long BYTES_IN_MB = 1024 * 1024;
    private static final long BYTES_IN_GB = 1024 * 1024 * 1024;
    private static boolean opportunisticContainersEnabled;

    @Inject
    NodesBlock(ResourceManager rm, ViewContext ctx) {
      super(ctx);
      this.rm = rm;
      // 从配置读取是否启用机会容器分配
      this.opportunisticContainersEnabled = YarnConfiguration
          .isOpportunisticContainerAllocationEnabled(
              this.rm.getRMContext().getYarnConfiguration());
    }

    @Override
    protected void render(Block html) {
      // 渲染指标概览表格
      html.__(MetricsOverviewTable.class);

      ResourceScheduler sched = rm.getResourceScheduler();

      // 获取请求参数中的节点状态过滤条件
      String type = $(NODE_STATE);
      // 获取请求参数中的节点标签过滤条件，默认不过滤
      String labelFilter = $(NODE_LABEL, CommonNodeLabelsManager.ANY).trim();
      // 构建表格表头，添加基础列
      Hamlet.TR<Hamlet.THEAD<TABLE<Hamlet>>> trbody =
          html.table("#nodes").thead().tr()
              .th(".nodelabels", "Node Labels")
              .th(".rack", "Rack")
              .th(".state", "Node State")
              .th(".nodeaddress", "Node Address")
              .th(".nodehttpaddress", "Node HTTP Address")
              .th(".lastHealthUpdate", "Last health-update")
              .th(".healthReport", "Health-report");

      // 根据是否启用机会容器，渲染不同的表头列
      if (!this.opportunisticContainersEnabled) {
        // 未启用机会容器，只展示常规资源列
        trbody.th(".containers", "Containers")
            .th(".allocationTags", "Allocation Tags")
            .th(".mem", "Mem Used")
            .th(".mem", "Mem Avail")
            .th(".mem", "Mem Total")
            .th(".mem", "Phys Mem Used %")
            .th(".vcores", "VCores Used")
            .th(".vcores", "VCores Avail")
            .th(".vcores", "VCores Total")
            .th(".vcores", "Phys VCores Used %");
      } else {
        // 启用机会容器，额外添加机会容器和排队容器列
        trbody.th(".containers", "Running Containers (G)")
            .th(".allocationTags", "Allocation Tags")
            .th(".mem", "Mem Used (G)")
            .th(".mem", "Mem Avail (G)")
            .th(".mem", "Mem Total")
            .th(".mem", "Phys Mem Used %")
            .th(".vcores", "VCores Used (G)")
            .th(".vcores", "VCores Avail (G)")
            .th(".vcores", "VCores Total")
            .th(".vcores", "Phys VCores Used %")
            .th(".containers", "Running Containers (O)")
            .th(".mem", "Mem Used (O)")
            .th(".vcores", "VCores Used (O)")
            .th(".containers", "Queued Containers");
      }

      // 为自定义资源类型添加表头列
      for (Map.Entry<String, Integer> integerEntry :
          ResourceUtils.getResourceTypeIndex().entrySet()) {
        // 内存和vCore已经处理过，跳过
        if (integerEntry.getKey().equals(ResourceInformation.MEMORY_URI)
            || integerEntry.getKey().equals(ResourceInformation.VCORES_URI)) {
          continue;
        }

        // 添加自定义资源已用列
        trbody.th("." + integerEntry.getKey(),
            integerEntry.getKey() + " " + "Used");

        // 添加自定义资源可用列
        trbody.th("." + integerEntry.getKey(),
            integerEntry.getKey() + " " + "Avail");
      }

      // 完成表头构建，开始准备表体
      TBODY<TABLE<Hamlet>> tbody =
          trbody.th(".nodeManagerVersion", "Version").__().__().tbody();

      NodeState stateFilter = null;
      // 解析节点状态过滤条件
      if (type != null && !type.isEmpty()) {
        stateFilter = NodeState.valueOf(StringUtils.toUpperCase(type));
      }
      // 默认获取所有活跃节点
      Collection<RMNode> rmNodes = this.rm.getRMContext().getRMNodes().values();
      boolean isInactive = false;
      // 根据过滤条件调整节点集合来源
      if (stateFilter != null) {
        switch (stateFilter) {
        case DECOMMISSIONED:
        case LOST:
        case REBOOTED:
        case SHUTDOWN:
          // 已下线类节点从非活跃节点集合获取
          rmNodes = this.rm.getRMContext().getInactiveRMNodes().values();
          isInactive = true;
          break;
        case DECOMMISSIONING:
          // 退役中节点仍然在活跃列表，不需要切换数据源
          break;
        default:
          LOG.debug("Unexpected state filter for inactive RM node");
        }
      }
      // 构建节点数据JSON数组，供前端DataTable渲染
      StringBuilder nodeTableData = new StringBuilder("[\n");
      // 遍历所有节点，按条件过滤并添加数据
      for (RMNode ni : rmNodes) {
        // 按节点状态过滤，不匹配则跳过
        if (stateFilter != null) {
          NodeState state = ni.getState();
          if (!stateFilter.equals(state)) {
            continue;
          }
        } else {
          // 无状态过滤时，默认不展示不健康节点
          if (ni.getState() == NodeState.UNHEALTHY) {
            continue;
          }
        }
        // 按节点标签过滤，不匹配则跳过
        if (!labelFilter.equals(RMNodeLabelsManager.ANY)) {
          if (labelFilter.isEmpty()) {
            // 空过滤条件只展示无标签节点
            if (!ni.getNodeLabels().isEmpty()) {
              continue;
            }
          } else if (!ni.getNodeLabels().contains(labelFilter)) {
            // 只展示包含指定标签的节点
            continue;
          }
        }
        // 封装节点信息为DAO对象
        NodeInfo info = new NodeInfo(ni, sched);
        int usedMemory = (int) info.getUsedMemory();
        int availableMemory = (int) info.getAvailableMemory();
        long totalMemory = info.getTotalResource().getMemorySize();
        int totalVcore = info.getTotalResource().getvCores();
        // 拼接节点基础信息到JSON数组
        nodeTableData.append("[\"")
            .append(StringUtils.join(",", info.getNodeLabels())).append("\",\"")
            .append(info.getRack()).append("\",\"").append(info.getState())
            .append("\",\"").append(info.getNodeId());
        if (isInactive) {
          // 非活跃节点无HTTP地址，显示N/A
          nodeTableData.append("\",\"").append("N/A").append("\",\"");
        } else {
          // 活跃节点生成HTTP地址链接
          String httpAddress = info.getNodeHTTPAddress();
          nodeTableData.append("\",\"<a ").append("href='" + "//" + httpAddress)
              .append("'>").append(httpAddress).append("</a>\",").append("\"");
        }

        // 添加健康检查时间和报告
        nodeTableData.append("<br title='")
            .append(String.valueOf(info.getLastHealthUpdate())).append("'>")
            .append(Times.format(info.getLastHealthUpdate())).append("\",\"")
            .append(StringEscapeUtils.escapeJava(info.getHealthReport())).append("\",\"")
            .append(String.valueOf(info.getNumContainers())).append("\",\"")
            .append(info.getAllocationTagsSummary()).append("\",\"")
            .append("<br title='").append(String.valueOf(usedMemory))
            .append("'>").append(StringUtils.byteDesc(usedMemory * BYTES_IN_MB))
            .append("\",\"").append("<br title='")
            .append(String.valueOf(availableMemory)).append("'>")
            .append(StringUtils.byteDesc(availableMemory * BYTES_IN_MB))
            .append("\",\"").append("<br title='").append(String.valueOf(totalMemory))
            .append("'>").append(StringUtils.byteDesc(totalMemory * BYTES_IN_MB))
            .append("\",\"")
            .append(String.valueOf((int) info.getMemUtilization()))
            .append("\",\"")
            .append(String.valueOf(info.getUsedVirtualCores()))
            .append("\",\"")
            .append(String.valueOf(info.getAvailableVirtualCores()))
            .append("\",\"")
            .append(String.valueOf(totalVcore))
            .append("\",\"")
            .append(String.valueOf((int) info.getVcoreUtilization()))
            .append("\",\"");

        // 如果启用机会容器，添加机会容器相关数据
        if (this.opportunisticContainersEnabled) {
          nodeTableData
              .append(String.valueOf(info.getNumRunningOpportContainers()))
              .append("\",\"").append("<br title='")
              .append(String.valueOf(info.getUsedMemoryOpportGB())).append("'>")
              .append(StringUtils.byteDesc(
                  info.getUsedMemoryOpportGB() * BYTES_IN_GB))
              .append("\",\"")
              .append(String.valueOf(info.getUsedVirtualCoresOpport()))
              .append("\",\"")
              .append(String.valueOf(info.getNumQueuedContainers()))
              .append("\",\"");
        }

        // 添加自定义资源数据
        for (Map.Entry<String, Integer> integerEntry :
            ResourceUtils.getResourceTypeIndex().entrySet()) {
          // 内存和vCore已经处理过，跳过
          if (integerEntry.getKey().equals(ResourceInformation.MEMORY_URI)
              || integerEntry.getKey().equals(ResourceInformation.VCORES_URI)) {
            continue;
          }

          long usedCustomResource = 0;
          long availableCustomResource = 0;

          String resourceName = integerEntry.getKey();
          Integer index = integerEntry.getValue();

          // 读取自定义资源已用和可用值
          if (index != null && info.getUsedResource() != null
              && info.getAvailableResource() != null) {
            usedCustomResource = info.getUsedResource().getResource()
                .getResourceValue(resourceName);
            availableCustomResource = info.getAvailableResource().getResource()
                .getResourceValue(resourceName);

            nodeTableData
                .append(usedCustomResource)
                .append("\",\"")
                .append(availableCustomResource)
                .append("\",\"");
          }
        }

        // 添加NodeManager版本信息，完成当前节点数据拼接
        nodeTableData.append(ni.getNodeManagerVersion())
            .append("\"],\n");
      }
      // 移除最后一个节点数据末尾多余的逗号
      if (nodeTableData.charAt(nodeTableData.length() - 2) == ',') {
        nodeTableData.delete(nodeTableData.length() - 2,
            nodeTableData.length() - 1);
      }
      // 完成JSON数组构建
      nodeTableData.append("]");
      // 将节点数据注入页面JavaScript变量
      html.script().$type("text/javascript")
          .__("var nodeTableData=" + nodeTableData).__();
      // 完成表体渲染
      tbody.__().__();
    }
  }

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    String type = $(NODE_STATE);
    String title = "Nodes of the cluster";
    // 按过滤条件设置页面标题
    if (type != null && !type.isEmpty()) {
      title = title + " (" + type + ")";
    }
    setTitle(title);
    // 配置DataTable插件参数
    set(DATATABLES_ID, "nodes");
    set(initID(DATATABLES, "nodes"), nodesTableInit());
    setTableStyles(html, "nodes", ".healthStatus {width:10em}",
        ".healthReport {width:10em}");
  }

  @Override
  protected Class<? extends SubView> content() {
    return NodesBlock.class;
  }

  /**
   * 生成节点表格DataTable初始化配置JSON
   * @return DataTable初始化配置字符串
   */
  private String nodesTableInit() {
    StringBuilder b = tableInit().append(", 'aaData': nodeTableData")
        .append(", bDeferRender: true").append(", bProcessing: true")
        .append(", aoColumnDefs: [")
        .append("{'bSearchable': false, 'aTargets': [ 7 ]}")
        .append(", {'sType': 'title-numeric', 'bSearchable': false, "
            + "'aTargets': [ 9, 10 ] }")
        .append(", {'sType': 'title-numeric', 'aTargets': [ 5 ]}")
        .append("]}");
    return b.toString();
  }
}