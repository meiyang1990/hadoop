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
 * YARN RM Web UI 应用详情页面处理器，负责渲染单个应用的详情页面。
 */
public class AppPage extends RmView {

  /**
   * 在HTML头部渲染前进行页面预处理，设置页面标题、表格样式和初始化参数。
   */
  @Override 
  protected void preHead(Page.HTML<__> html) {
    // 调用通用预处理逻辑
    commonPreHead(html);
    // 获取请求参数中的应用ID
    String appId = $(YarnWebParams.APPLICATION_ID);
    // 设置页面标题，缺少应用ID时显示错误信息
    set(
      TITLE,
      appId.isEmpty() ? "Bad request: missing application ID" : join(
        "Application ", $(YarnWebParams.APPLICATION_ID)));

    // 设置DataTables表格ID
    set(DATATABLES_ID, "attempts ResourceRequests");
    // 初始化尝试列表表格配置
    set(initID(DATATABLES, "attempts"), WebPageUtils.attemptsTableInit());
    // 设置尝试列表表格列宽度样式
    setTableStyles(html, "attempts", ".queue {width:6em}", ".ui {width:8em}");

    // 设置资源请求表格样式
    setTableStyles(html, "ResourceRequests");

    // 标记当前UI类型为RM Web UI
    set(YarnWebParams.WEB_UI_TYPE, YarnWebParams.RM_WEB_UI);
  }

  /**
   * 获取页面内容区域对应的子视图类。
   * @return 应用详情区块类
   */
  @Override 
  protected Class<? extends SubView> content() {
    return RMAppBlock.class;
  }
}