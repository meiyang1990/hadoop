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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.postInitID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.tableInit;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

/**
 * 历史服务器Web页面的基类视图，所有历史服务器页面都继承此类。
 * 提供统一的页面布局初始化、表格配置和导航处理，为所有HS页面提供公共基础能力。
 */
public class HsView extends TwoColumnLayout {
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 在HTML页面head部分之前进行页面初始化，配置公共JS和页面样式。
   * @param html HTML页面对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 执行所有子类通用的预初始化逻辑
    commonPreHead(html);
    // 设置作业表格的ID
    set(DATATABLES_ID, "jobs");
    // 初始化作业表格的DataTables配置
    set(initID(DATATABLES, "jobs"), jobsTableInit());
    // 设置表格初始化后的后置处理脚本
    set(postInitID(DATATABLES, "jobs"), jobsPostTableInit());
    // 设置作业表格的样式
    setTableStyles(html, "jobs");
  }

  /**
   * 所有子类通用的预初始化方法，初始化左侧导航手风琴组件。
   * @param html HTML页面对象，用于渲染
   */
  protected void commonPreHead(Page.HTML<__> html) {
    // 设置导航手风琴组件ID
    set(ACCORDION_ID, "nav");
    // 初始化手风琴配置，默认展开第一个导航项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  /**
   * 根据是否有任务ID参数，设置左侧导航栏哪一项默认激活。
   * 当查看任务详情时激活任务相关导航，否则激活作业相关导航。
   */
  protected void setActiveNavColumnForTask() {
    // 从请求参数获取任务ID
    String tid = $(TASK_ID);
    // 默认激活第二项（任务导航）
    String activeNav = "2";
    // 如果没有任务ID参数，则激活第一项（作业导航）
    if((tid == null || tid.isEmpty())) {
      activeNav = "1";
    }
    // 更新手风琴初始化配置，设置正确的激活项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:"+activeNav+"}");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#nav()
   */
  /**
   * 获取左侧导航栏区块类。
   * @return 导航栏区块类
   */
  @Override
  protected Class<? extends SubView> nav() {
    return HsNavBlock.class;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#content()
   */
  /**
   * 获取主内容区默认区块类，默认显示作业列表。
   * @return 内容区块类
   */
  @Override
  protected Class<? extends SubView> content() {
    return HsJobsBlock.class;
  }
  
  //TODO We need a way to move all of the javascript/CSS that is for a subview
  // into that subview.
  /**
   * 生成作业列表表格的DataTables初始化配置JavaScript。
   * @return DataTables配置字符串，插入到页面初始化脚本中。
   */
  private String jobsTableInit() {
    return tableInit().
        append(", 'aaData': jobsTableData").
        append(", bDeferRender: true").
        append(", bProcessing: true").

        // Sort by id upon page load
        append(", aaSorting: [[3, 'desc']]").
        append(", aoColumnDefs:[").
        // Maps Total, Maps Completed, Reduces Total and Reduces Completed
        append("{'sType':'numeric', 'bSearchable': false" +
            ", 'aTargets': [ 8, 9, 10, 11 ] }").
        append("]}").
        toString();
  }
  
  /**
   * 生成作业列表表格初始化完成后的后置处理JavaScript。
   * 为表格每一列添加表头搜索过滤功能。
   * @return 初始化后的JavaScript代码字符串
   */
  private String jobsPostTableInit() {
    return "var asInitVals = new Array();\n" +
    		   "$('tfoot input').keyup( function () \n{"+
           "  jobsDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n"+
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