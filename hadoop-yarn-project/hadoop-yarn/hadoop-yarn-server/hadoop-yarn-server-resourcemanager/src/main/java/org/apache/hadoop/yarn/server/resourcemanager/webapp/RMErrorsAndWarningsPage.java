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

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.server.webapp.ErrorsAndWarningsBlock;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * ResourceManager 错误与警告信息页面，用于展示RM运行过程中产生的异常和告警信息
 */
public class RMErrorsAndWarningsPage extends RmView {

  @Override
  protected Class<? extends SubView> content() {
    // 返回错误警告块视图类，由ErrorsAndWarningsBlock负责渲染具体内容
    return ErrorsAndWarningsBlock.class;
  }

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行通用页面预处理逻辑
    commonPreHead(html);
    // 设置页面标题
    String title = "Errors and Warnings in the ResourceManager";
    setTitle(title);
    // 定义消息表格ID
    String tableId = "messages";
    // 设置表格ID配置项
    set(DATATABLES_ID, tableId);
    // 设置表格初始化JS配置
    set(initID(DATATABLES, tableId), tablesInit());
    // 设置表格各列样式
    setTableStyles(html, tableId, ".message {width:50em}",
      ".count {width:8em}", ".lasttime {width:16em}");
  }

  /**
   * 生成DataTable表格初始化配置JSON
   * @return 表格初始化配置字符串
   */
  private String tablesInit() {
    StringBuilder b = tableInit().append(", aoColumnDefs: [");
    b.append("{'sType': 'string', 'aTargets': [ 0 ]}")
        .append(", {'sType': 'string', 'bSearchable': true, 'aTargets': [ 1 ]}")
        .append(
            ", {'sType': 'numeric', 'bSearchable': false, 'aTargets': [ 2 ]}")
        .append(", {'sType': 'date', 'aTargets': [ 3 ] }]")
        .append(", aaSorting: [[3, 'desc']]}");
    return b.toString();
  }
}