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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import java.util.Map.Entry;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.BODY;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * NodeManager Web UI 所有应用页面，展示当前节点上运行的所有应用列表
 */
public class AllApplicationsPage extends NMView {

  @Override protected void preHead(Page.HTML<__> html) {
    // 执行通用预处理
    commonPreHead(html);
    // 设置页面标题
    setTitle("Applications running on this node");
    // 设置DataTable组件ID
    set(DATATABLES_ID, "applications");
    // 初始化应用列表DataTable
    set(initID(DATATABLES, "applications"), appsTableInit());
    // 设置表格样式
    setTableStyles(html, "applications");
  }

  /**
   * 生成应用列表表格的DataTable初始化配置
   * @return DataTable初始化配置JSON字符串
   */
  private String appsTableInit() {
    return tableInit().
        // 页面加载后按应用ID升序排序
        append(", aaSorting: [[0, 'asc']]").
        // 定义列配置：应用ID列、应用状态列
        append(", aoColumns:[").append(getApplicationsIdColumnDefs())
        .append(", null]} ").toString();
  }

  /**
   * 生成应用ID列的DataTable列定义
   * @return 应用ID列配置JSON字符串
   */
  private String getApplicationsIdColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("{'sType':'natural', 'aTargets': [0]")
        .append(", 'mRender': parseHadoopID }").toString();
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回应用列表内容块类
    return AllApplicationsBlock.class;
  }

  /**
   * 所有应用列表内容块，负责渲染当前节点应用列表HTML
   */
  public static class AllApplicationsBlock extends HtmlBlock implements
      YarnWebParams {

    // NodeManager上下文，保存节点全局信息
    private final Context nmContext;

    @Inject
    public AllApplicationsBlock(Context nmContext) {
      this.nmContext = nmContext;
    }

    @Override
    protected void render(Block html) {
      // 构建应用列表表格框架和表头
      TBODY<TABLE<BODY<Hamlet>>> tableBody =
        html
          .body()
            .table("#applications")
              .thead()
                .tr()
                  .td().__("ApplicationId").__()
                  .td().__("ApplicationState").__()
                .__()
               .__()
               .tbody();
      // 遍历当前节点所有应用，逐行渲染
      for (Entry<ApplicationId, Application> entry : this.nmContext
          .getApplications().entrySet()) {
        // 构造应用信息视图对象
        AppInfo info = new AppInfo(entry.getValue());
        // 添加表格行
        tableBody
          .tr()
            // 应用ID列，添加应用详情链接
            .td().a(url("application", info.getId()), info.getId()).__()
            // 应用状态列
            .td().__(info.getState())
            .__()
          .__();
      }
      // 闭合表格标签
      tableBody.__().__().__();
    }
  }
}