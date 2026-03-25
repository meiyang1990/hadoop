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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_TYPE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_SELECTOR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initSelector;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.webapp.SubView;

/**
 * 历史服务器中展示指定应用所有任务列表的页面
 * 用于在历史服务器Web UI中呈现已完成应用的任务信息
 */
public class HsTasksPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 页面头渲染前的初始化配置，设置页面所需的JS组件参数和样式
   * @param html 页面HTML对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    set(DATATABLES_ID, "tasks");
    set(DATATABLES_SELECTOR, ".dt-tasks" );
    set(initSelector(DATATABLES), tasksTableInit());
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
    set(initID(DATATABLES, "tasks"), tasksTableInit());
    set(postInitID(DATATABLES, "tasks"), jobsPostTableInit());
    setTableStyles(html, "tasks");
  }
  
  /**
   * 获取页面内容区块，本页内容为任务列表区块
   * @return 任务列表区块类对象
   */
  @Override protected Class<? extends SubView> content() {
    return HsTasksBlock.class;
  }

  /**
   * 生成任务列表DataTable jQuery组件的初始化配置JSON
   * 根据任务类型动态调整表格列配置，适配Map和Reduce任务不同的列结构
   * @return 表格初始化配置的JSON字符串
   */
  private String tasksTableInit() {
    TaskType type = null;
    String symbol = $(TASK_TYPE);
    if (!symbol.isEmpty()) {
      type = MRApps.taskType(symbol);
    }
    StringBuilder b = tableInit().
    append(", 'aaData': tasksTableData")
    .append(", bDeferRender: true")
    .append(", bProcessing: true")

    .append("\n, aoColumnDefs: [\n")
    .append("{'sType':'natural', 'aTargets': [ 0 ]")
    .append(", 'mRender': parseHadoopID }")

    .append(", {'sType':'numeric', 'aTargets': [ 4")
    // 根据任务类型调整耗时列位置
    .append(type == TaskType.REDUCE ? ", 9, 10, 11, 12" : ", 7")
    .append(" ], 'mRender': renderHadoopElapsedTime }")

    .append("\n, {'sType':'numeric', 'aTargets': [ 2, 3, 5")
    // 根据任务类型调整日期列位置
    .append(type == TaskType.REDUCE ? ", 6, 7, 8" : ", 6")
    .append(" ], 'mRender': renderHadoopDate }]")

    // 页面加载后默认按任务ID升序排序
    .append("\n, aaSorting: [[0, 'asc']]")
    .append("}");
    return b.toString();
  }
  
  /**
   * 生成任务列表表格初始化后，用于添加列搜索功能的JS代码
   * @return 包含表格搜索事件绑定逻辑的JS代码字符串
   */
  private String jobsPostTableInit() {
    return "var asInitVals = new Array();\n" +
           "$('tfoot input').keyup( function () \n{"+
           "  $('.dt-tasks').dataTable().fnFilter("+
           " this.value, $('tfoot input').index(this) );\n"+
           "} );\n"+
           "$('tfoot input').each( function (i) {\n"+
           "  asInitVals[i] = this.value;\n"+
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
           "    this.value = asInitVals[$('tfoot input').index(this)];\n"+
           "  }\n"+
           "} );\n";
  }
}