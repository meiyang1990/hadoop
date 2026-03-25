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

import static org.apache.hadoop.yarn.server.router.webapp.RouterWebServiceUtil.generateWebTitle;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_SC;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * Router联邦路由节点列表页面渲染类，负责构建节点列表页面的头部配置和内容绑定。
 */
class NodesPage extends RouterView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行通用头部预处理
    commonPreHead(html);
    // 获取请求参数中的节点类型
    String type = $(NODE_SC);
    // 获取请求参数中的节点状态
    String state = $(NODE_STATE);
    // 初始化默认页面标题
    String title = "Nodes of the cluster";
    // 如果指定了节点状态，生成带状态过滤的标题
    if (state != null && !state.isEmpty()) {
      title = generateWebTitle(title, state);
    } else if (type != null && !type.isEmpty()) {
      // 如果指定了节点类型，生成带类型过滤的标题
      title = generateWebTitle(title, type);
    }
    // 设置页面标题
    setTitle(title);
    // 设置DataTable表格ID
    set(DATATABLES_ID, "nodes");
    // 初始化节点表格配置
    set(initID(DATATABLES, "nodes"), nodesTableInit());
    // 设置表格CSS样式
    setTableStyles(html, "nodes", ".healthStatus {width:10em}",
        ".healthReport {width:10em}");
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回节点列表内容块渲染类
    return NodesBlock.class;
  }

  /**
   * 生成节点列表DataTable的初始化配置JSON。
   * @return 表格初始化配置JSON字符串
   */
  private String nodesTableInit() {
    StringBuilder b = tableInit().append(", aoColumnDefs: [");
    b.append("{'bSearchable': false, 'aTargets': [ 7 ]}")
        .append(", {'sType': 'title-numeric', 'bSearchable': false, "
            + "'aTargets': [ 2, 3, 4, 5, 6 ] }")
        .append(", {'sType': 'title-numeric', 'aTargets': [ 5 ]}")
        .append("]}");
    return b.toString();
  }
}