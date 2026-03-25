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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;

import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * YARN ResourceManager WebUI 节点标签页面，展示集群中所有节点标签信息
 */
public class NodeLabelsPage extends RmView {
  /**
   * 节点标签信息表格渲染块，负责生成节点标签列表的HTML内容
   */
  static class NodeLabelsBlock extends HtmlBlock {
    final ResourceManager rm;

    @Inject
    NodeLabelsBlock(ResourceManager rm, ViewContext ctx) {
      super(ctx);
      this.rm = rm;
    }

    @Override
    protected void render(Block html) {
      // 初始化表格表头，定义各列标题
      TBODY<TABLE<Hamlet>> tbody = html.table("#nodelabels").
          thead().
          tr().
          th(".name", "Label Name").
          th(".type", "Label Type").
          th(".numOfActiveNMs", "Num Of Active NMs").
          th(".totalResource", "Total Resource").
          __().__().
          tbody();
  
      // 获取ResourceManager节点标签管理器
      RMNodeLabelsManager nlm = rm.getRMContext().getNodeLabelManager();
      // 遍历所有节点标签信息，生成表格行
      for (RMNodeLabel info : nlm.pullRMNodeLabelsInfo()) {
        // 添加标签名称单元格，空名称显示默认分区
        TR<TBODY<TABLE<Hamlet>>> row =
            tbody.tr().td(info.getLabelName().isEmpty()
                ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : info.getLabelName());
        // 计算标签分区类型（独占/非独占）
        String type =
            (info.getIsExclusive()) ? "Exclusive Partition"
                : "Non Exclusive Partition";
        // 添加标签类型单元格
        row = row.td(type);
        // 获取该标签下活跃NodeManager数量
        int nActiveNMs = info.getNumActiveNMs();
        // 活跃节点数大于0时添加跳转到节点列表的链接
        if (nActiveNMs > 0) {
          row = row.td()
          .a(url("nodes",
              "?" + YarnWebParams.NODE_LABEL + "=" + info.getLabelName()),
              String.valueOf(nActiveNMs))
           .__();
        } else {
          // 无活跃节点直接显示数量
          row = row.td(String.valueOf(nActiveNMs));
        }
        // 添加该标签总资源单元格，关闭行
        row.td(info.getResource().toFormattedString()).__();
      }
      // 关闭表格标签
      tbody.__().__();
    }
  }

  @Override
  /** 页面预处理，设置页面标题和表格样式 */
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    String title = "Node labels of the cluster";
    setTitle(title);
    set(DATATABLES_ID, "nodelabels");
    setTableStyles(html, "nodelabels", ".healthStatus {width:10em}",
                   ".healthReport {width:10em}");
  }

  @Override
  /** 获取页面内容渲染块类 */
  protected Class<? extends SubView> content() {
    return NodeLabelsBlock.class;
  }
}