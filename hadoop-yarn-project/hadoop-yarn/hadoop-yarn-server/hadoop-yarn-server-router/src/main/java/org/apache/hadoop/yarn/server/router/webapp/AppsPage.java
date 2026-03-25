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

import static org.apache.hadoop.yarn.util.StringHelper.sjoin;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_SC;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.webapp.SubView;

/**
 * YARN Router 联邦集群应用列表页面视图类，负责渲染应用列表页面的头部配置与内容布局。
 */
class AppsPage extends RouterView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行通用头部预处理
    commonPreHead(html);
    // 设置DataTable组件ID
    set(DATATABLES_ID, "apps");
    // 初始化应用列表表格
    set(initID(DATATABLES, "apps"), appsTableInit());
    // 设置表格样式
    setTableStyles(html, "apps", ".queue {width:6em}", ".ui {width:8em}");

    // 获取子集群名称参数
    String subClusterName = $(APP_SC);
    // 获取应用状态过滤参数
    String reqState = $(APP_STATE);

    // 子集群名称为空时默认显示联邦标识
    if(StringUtils.isBlank(subClusterName)){
      subClusterName = "Federation ";
    }
    // 应用状态为空时默认显示所有应用
    reqState = (StringUtils.isBlank(reqState) ? "All" : reqState);
    // 设置页面标题
    setTitle(sjoin(subClusterName, reqState,  "Applications"));
  }

  /**
   * 生成应用列表表格的DataTables初始化配置。
   * @return DataTables初始化JSON配置字符串
   */
  private String appsTableInit() {
    // id, user, name, queue, starttime, finishtime, state, status, progress, ui
    return tableInit()
      .append(", 'aaData': appsTableData")
      .append(", bDeferRender: true")
      .append(", bProcessing: true")

      .append("\n, aoColumnDefs: ")
      .append(getAppsTableColumnDefs())

      // 页面加载后默认按应用ID降序排序
      .append(", aaSorting: [[0, 'desc']]}").toString();
  }

  /**
   * 生成应用列表表格列定义配置。
   * @return DataTables列定义JSON配置字符串
   */
  protected String getAppsTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb
      .append("[\n")
      // 第一列：应用ID，使用自定义解析渲染
      .append("{'sType':'string', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")

      // 第六、七列：日期时间列，使用自定义日期渲染
      .append("\n, {'sType':'numeric', 'aTargets': [6, 7]")
      .append(", 'mRender': renderHadoopDate }")

      // 第十列：应用进度列，不参与搜索，使用自定义进度渲染
      .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets': [10]")
      .append(", 'mRender': parseHadoopProgress }]").toString();
  }

  @Override
  protected Class<? extends SubView> content() {
    // 内容区域使用AppsBlock子视图渲染
    return AppsBlock.class;
  }
}