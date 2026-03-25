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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * YARN联邦路由页面视图，渲染YARN联邦信息页面，包含所有子集群ResourceManager监控信息。
 */
class FederationPage extends RouterView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行通用页面头部预处理
    commonPreHead(html);
    // 设置页面标题
    setTitle("About The YARN Federation");
    // 设置ResourceManager表格ID
    set(DATATABLES_ID, "rms");
    // 设置div容器ID
    set("ui.div.id", "div_id");
    // 初始化ResourceManager表格配置
    set(initID(DATATABLES, "rms"), rmsTableInit());
    // 设置表格样式，指定各列宽度
    setTableStyles(html, "rms", ".healthStatus {width:10em}",
        ".healthReport {width:10em}");
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回页面内容区块类
    return FederationBlock.class;
  }

  /**
   * 生成ResourceManager列表表格的初始化配置。
   * @return DataTables表格初始化JSON字符串
   */
  private String rmsTableInit() {
    StringBuilder builder = tableInit().append(", aoColumnDefs: [");
    builder
        // 状态列配置：不可搜索
        .append("{'sName':'State', 'sType':'string', 'bSearchable':false, 'aTargets':[1]},")
        // 启动时间列配置：不可搜索
        .append("{'sName':'LastStartTime', 'sType':'string', 'bSearchable':false, 'aTargets':[2]},")
        // 上次心跳列配置：不可搜索
        .append("{'sName':'lastHeartBeat', 'sType':'string', 'bSearchable':false, 'aTargets':[3]},")
        // 资源信息列配置：不可搜索
        .append("{'sName':'resource', 'sType':'string', 'bSearchable':false, 'aTargets':[4]},")
        // 节点信息列配置：不可搜索
        .append("{'sName':'nodes', 'sType':'string', 'bSearchable':false, 'aTargets':[5]}")
        .append("]}");
    return builder.toString();
  }
}