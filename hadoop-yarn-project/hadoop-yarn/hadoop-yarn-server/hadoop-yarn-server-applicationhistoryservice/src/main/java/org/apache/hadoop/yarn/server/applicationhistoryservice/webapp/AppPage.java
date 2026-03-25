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
import org.apache.hadoop.yarn.server.webapp.AppBlock;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

/**
 * 应用历史服务单个应用详情页面，负责渲染应用详情页面的基础配置和布局。
 */
public class AppPage extends AHSView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行公共预处理逻辑
    commonPreHead(html);

    // 获取请求参数中的应用ID
    String appId = $(YarnWebParams.APPLICATION_ID);
    // 设置页面标题，缺少应用ID时显示错误信息
    set(
      TITLE,
      appId.isEmpty() ? "Bad request: missing application ID" : join(
        "Application ", $(YarnWebParams.APPLICATION_ID)));

    // 设置需要初始化的数据表ID列表
    set(DATATABLES_ID, "attempts ResourceRequests");
    // 初始化尝试列表数据表配置
    set(initID(DATATABLES, "attempts"), WebPageUtils.attemptsTableInit());
    // 设置尝试列表表格样式
    setTableStyles(html, "attempts", ".queue {width:6em}", ".ui {width:8em}");

    // 设置资源请求列表表格样式
    setTableStyles(html, "ResourceRequests");

    // 标记当前为应用历史WebUI类型
    set(YarnWebParams.WEB_UI_TYPE, YarnWebParams.APP_HISTORY_WEB_UI);
  }

  @Override
  protected Class<? extends SubView> content() {
    // 使用通用应用信息块作为页面内容主体
    return AppBlock.class;
  }

  /**
   * 获取尝试列表表格的列定义配置JSON。
   * @return 列定义配置字符串
   */
  protected String getAttemptsTableColumnDefs() {
    StringBuilder sb = new StringBuilder();
    return sb.append("[\n").append("{'sType':'natural', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")

      .append("\n, {'sType':'numeric', 'aTargets': [1]")
      .append(", 'mRender': renderHadoopDate }]").toString();
  }
}