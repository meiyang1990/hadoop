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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * 文件所属模块：MapReduce 应用客户端 Web UI
 * 核心职责：渲染指定 MapReduce 作业的配置信息页面，展示作业的所有配置参数
 */
public class JobConfPage extends AppView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 页面头部预处理方法，设置页面标题、初始化前端UI组件配置
   * @param html HTML页面对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 从请求中获取作业ID参数
    String jobID = $(JOB_ID);
    // 设置页面标题，处理作业ID缺失的异常情况
    set(TITLE, jobID.isEmpty() ? "Bad request: missing job ID"
        : join("Configuration for MapReduce Job ", $(JOB_ID)));
    // 执行公共头部预处理逻辑
    commonPreHead(html);
    // 初始化导航手风琴组件，激活第三个菜单项（对应作业配置）
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:2}");
    // 设置数据表ID为conf
    set(DATATABLES_ID, "conf");
    // 初始化配置数据表
    set(initID(DATATABLES, "conf"), confTableInit());
    // 执行配置数据表后初始化逻辑，添加筛选功能
    set(postInitID(DATATABLES, "conf"), confPostTableInit());
    // 设置数据表样式
    setTableStyles(html, "conf");
  }

  /**
   * 获取页面主体内容区块，返回配置参数区块类
   * @return 配置区块类型
   */
  @Override protected Class<? extends SubView> content() {
    return ConfBlock.class;
  }

  /**
   * 生成配置数据表的初始化JS配置
   * @return 数据表初始化配置JSON字符串
   */
  private String confTableInit() {
    return tableInit().append("}").toString();
  }

  /**
   * 生成配置数据表的筛选功能初始化JS代码，实现按列过滤配置参数
   * @return 初始化筛选功能的JavaScript代码
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