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

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import static org.apache.hadoop.yarn.server.router.webapp.RouterWebServiceUtil.generateWebTitle;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_SC;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NODE_LABEL;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;

/**
 * Router联邦集群Web UI的节点标签页面渲染类，负责渲染包含指标信息的节点标签页面。
 */
public class NodeLabelsPage extends RouterView {

  @Override
  protected void preHead(Hamlet.HTML<__> html) {
    // 调用公共预初始化逻辑
    commonPreHead(html);
    // 获取请求参数中的节点状态过滤条件
    String type = $(NODE_SC);
    // 获取请求参数中的节点标签过滤条件
    String nodeLabel = $(NODE_LABEL);
    // 默认页面标题
    String title = "Node labels of the cluster";

    // 如果指定了节点标签，生成带标签名的页面标题
    if (nodeLabel != null && !nodeLabel.isEmpty()) {
      title = generateWebTitle(title, nodeLabel);
    } else if (type != null && !type.isEmpty()) {
      // 如果指定了节点状态，生成带状态名的页面标题
      title = generateWebTitle(title, type);
    }

    // 设置页面标题
    setTitle(title);
    // 设置当前表格ID为nodelabels
    set(DATATABLES_ID, "nodelabels");
    // 设置表格样式
    setTableStyles(html, "nodelabels", ".healthStatus {width:10em}", ".healthReport {width:10em}");
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回节点标签内容块渲染类
    return NodeLabelsBlock.class;
  }
}