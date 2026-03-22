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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.mapreduce.v2.app.webapp.ConfBlock;
import org.apache.hadoop.yarn.webapp.SubView;

/**
 * 历史服务器端作业配置页面，用于展示指定已完成作业的配置信息
 */
public class HsConfPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 页面头部预处理，设置页面标题、初始化前端组件配置
   * @param html html页面根节点
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 获取请求中的作业ID参数
    String jobID = $(JOB_ID);
    // 设置页面标题，处理缺少作业ID的错误情况
    set(TITLE, jobID.isEmpty() ? "Bad request: missing job ID"
        : join("Configuration for MapReduce Job ", $(JOB_ID)));
    // 执行通用头部预处理逻辑
    commonPreHead(html);
    // 设置配置表格ID
    set(DATATABLES_ID, "conf");
    // 初始化配置表格的DataTables设置
    set(initID(DATATABLES, "conf"), confTableInit());
    // 设置配置表格初始化后执行的脚本
    set(postInitID(DATATABLES, "conf"), confPostTableInit());
    // 设置配置表格样式
    setTableStyles(html, "conf");

    // 覆盖默认导航手风琴组件设置，设置第二个导航项默认激活
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
  }

  /**
   * 获取页面主体内容区块类型
   * @return 配置区块类，用于渲染作业配置内容
   */
  @Override protected Class<? extends SubView> content() {
    return ConfBlock.class;
  }

  /**
   * 生成配置表格DataTables初始化配置JSON
   * @return 表格初始化配置字符串
   */
  private String confTableInit() {
    return tableInit().append("}").toString();
  }

  /**
   * 生成配置表格列过滤功能初始化JavaScript代码
   * @return 实现表格按列过滤的JS代码
   */
  private String confPostTableInit() {
    return "var confInitVals = new Array();\n" +
    "$('tfoot input').keyup( function () \n{"+
    "  confDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n"+
    "} );\n"+
    "$('tfoot input').each( function (i) {\n"+
    "  confInitVals[i] = this.value;\n"+
    "} );\n"+
    "$('tfoot input').focus( function () {\n"+
    "  if ( this.className == 'search_init' )\n"+
    "  {\n"+
    "    this.className = '';\n"+
    "    this.value = '';\n"+
    "  }\n"+
    "} );\n"+
    "$('tfoot input').blur( function (i) {\n"+
    "  if ( this.value == '' )\n"+
    "  {\n"+
    "    this.className = 'search_init';\n"+
    "    this.value = confInitVals[$('tfoot input').index(this)];\n"+
    "  }\n"+
    "} );\n";
  }
}