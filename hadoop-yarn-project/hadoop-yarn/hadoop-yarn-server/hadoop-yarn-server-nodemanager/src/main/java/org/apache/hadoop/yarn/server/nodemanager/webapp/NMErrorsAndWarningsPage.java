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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.server.webapp.ErrorsAndWarningsBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * NodeManager错误与警告信息页面，用于在Web UI展示NM产生的各类错误和告警信息
 */
public class NMErrorsAndWarningsPage extends NMView {

  /**
   * 获取页面内容区块类
   * @return 错误警告区块类
   */
  @Override
  protected Class<? extends SubView> content() {
    return ErrorsAndWarningsBlock.class;
  }

  /**
   * HTML页面Head预处理，配置页面样式和表格初始化参数
   * @param html HTML页面对象
   */
  @Override
  protected void preHead(HtmlPage.Page.HTML<__> html) {
    // 调用通用预处理逻辑
    commonPreHead(html);
    // 设置页面标题
    String title = "Errors and Warnings in the NodeManager";
    setTitle(title);
    // 初始化DataTable表格配置
    String tableId = "messages";
    set(DATATABLES_ID, tableId);
    set(initID(DATATABLES, tableId), tablesInit());
    // 设置表格列宽样式
    setTableStyles(html, tableId, ".message {width:50em}",
      ".count {width:8em}", ".lasttime {width:16em}");
  }

  /**
   * 生成DataTables表格初始化配置JSON
   * @return 表格初始化参数字符串
   */
  private String tablesInit() {
    StringBuilder b = tableInit().append(", aoColumnDefs: [");
    // 第一列：字符串类型
    b.append("{'sType': 'string', 'aTargets': [ 0 ]}");
    // 第二列：可搜索字符串类型
    b.append(", {'sType': 'string', 'bSearchable': true, 'aTargets': [ 1 ]}");
    // 第三列：不可搜索数值类型
    b.append(", {'sType': 'numeric', 'bSearchable': false, 'aTargets': [ 2 ]}");
    // 第四列：日期类型
    b.append(", {'sType': 'date', 'aTargets': [ 3 ] }]");
    // 默认按第四列降序排序（最新消息在前）
    b.append(", aaSorting: [[3, 'desc']]}");
    return b.toString();
  }
}