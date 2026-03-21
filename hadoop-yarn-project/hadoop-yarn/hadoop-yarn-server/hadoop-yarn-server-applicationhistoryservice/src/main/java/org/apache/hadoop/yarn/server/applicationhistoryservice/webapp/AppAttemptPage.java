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
package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import org.apache.hadoop.yarn.server.webapp.AppAttemptBlock;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

/**
 * 应用程序尝试详情页面，应用历史服务Web UI中展示单个应用尝试的详细信息
 */
public class AppAttemptPage extends AHSView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行通用页面预处理
    commonPreHead(html);

    // 获取请求参数中的应用尝试ID
    String appAttemptId = $(YarnWebParams.APPLICATION_ATTEMPT_ID);
    // 设置页面标题，参数缺失返回错误提示
    set(
      TITLE,
      appAttemptId.isEmpty() ? "Bad request: missing application attempt ID"
          : join("Application Attempt ",
            $(YarnWebParams.APPLICATION_ATTEMPT_ID)));

    // 设置容器列表表格ID
    set(DATATABLES_ID, "containers");
    // 初始化容器列表DataTables组件
    set(initID(DATATABLES, "containers"), WebPageUtils.containersTableInit());
    // 设置容器列表表格样式
    setTableStyles(html, "containers", ".queue {width:6em}", ".ui {width:8em}");

    // 标记当前页面属于应用历史Web UI
    set(YarnWebParams.WEB_UI_TYPE, YarnWebParams.APP_HISTORY_WEB_UI);
  }

  @Override
  /** 获取页面内容区块类 */
  protected Class<? extends SubView> content() {
    return AppAttemptBlock.class;
  }

  /** 获取容器列表表格列定义JSON */
  protected String getContainersTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'natural', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }]").toString();
  }

}