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

import org.apache.hadoop.yarn.server.webapp.ErrorsAndWarningsBlock;
import org.apache.hadoop.yarn.webapp.SubView;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * 应用历史服务器(AHS)错误和警告页面，用于展示应用运行过程中产生的错误与警告信息
 */
public class AHSErrorsAndWarningsPage extends AHSView {

  /**
   * 获取页面内容区块类
   * @return 错误警告信息区块类
   */
  @Override
  protected Class<? extends SubView> content() {
    return ErrorsAndWarningsBlock.class;
  }

  /**
   * 在HTML head渲染前执行初始化配置
   * @param html HTML页面构建器
   */
  @Override
  protected void preHead(Page.HTML<__> html) {
    // 调用通用初始化逻辑
    commonPreHead(html);
    // 设置页面标题
    String title = "Errors and Warnings in the Application History Server";
    setTitle(title);
    // 设置表格ID
    String tableId = "messages";
    set(DATATABLES_ID, tableId);
    // 初始化表格
    set(initID(DATATABLES, tableId), tablesInit());
    // 设置表格列样式
    setTableStyles(html, tableId, ".message {width:50em}",
        ".count {width:8em}", ".lasttime {width:16em}");
  }

  /**
   * 生成DataTables表格初始化配置JSON
   * @return 表格初始化参数字符串
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