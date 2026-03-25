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

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

/**
 * RM Web UI 应用尝试信息页面，负责渲染应用尝试详情页面的整体结构
 */
public class AppAttemptPage extends RmView {

  /**
   * 在HTML head部分渲染前预处理，设置页面标题、DataTable配置
   */
  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);

    // 从请求参数获取应用尝试ID
    String appAttemptId = $(YarnWebParams.APPLICATION_ATTEMPT_ID);
    // 设置页面标题，缺失ID时显示错误信息
    set(
      TITLE,
      appAttemptId.isEmpty() ? "Bad request: missing application attempt ID"
          : join("Application Attempt ",
            $(YarnWebParams.APPLICATION_ATTEMPT_ID)));

    // 设置两个DataTable表格ID：容器列表和资源请求列表
    set(DATATABLES_ID, "containers resourceRequests");
    // 初始化容器列表表格配置
    set(initID(DATATABLES, "containers"), WebPageUtils.containersTableInit());
    // 初始化资源请求列表表格配置
    set(initID(DATATABLES, "resourceRequests"),
        WebPageUtils.resourceRequestsTableInit());
    // 设置容器列表表格列宽样式
    setTableStyles(html, "containers", ".queue {width:6em}", ".ui {width:8em}");

    // 标记当前页面属于RM Web UI类型
    set(YarnWebParams.WEB_UI_TYPE, YarnWebParams.RM_WEB_UI);
  }

  /**
   * 获取页面内容块类
   * @return 应用尝试详情内容块类
   */
  @Override
  protected Class<? extends SubView> content() {
    return RMAppAttemptBlock.class;
  }

}