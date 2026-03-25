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

import org.apache.hadoop.yarn.webapp.SubView;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * 计数器页面渲染类，属于MapReduce ApplicationMaster Web UI，负责渲染作业/任务计数器页面的整体框架
 * 继承AppView基类，完成页面布局初始化、CSS样式配置和内容块绑定
 */
public class CountersPage extends AppView {

  /**
   * 在HTML HEAD部分生成之前执行初始化，配置JQuery UI组件参数
   * @param html HTML页面构建器
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 执行通用前置初始化逻辑
    commonPreHead(html);

    // 获取请求中的任务ID参数
    String tid = $(TASK_ID);
    // 默认激活导航栏第3项（任务计数器）
    String activeNav = "3";
    // 如果没有任务ID，说明查看的是作业总计数器，激活导航栏第2项
    if(tid == null || tid.isEmpty()) {
      activeNav = "2";
    }
    // 配置手风琴导航组件：关闭自动高度，设置默认激活项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:"+activeNav+"}");
    // 设置计数器表格的DataTable选择器
    set(DATATABLES_SELECTOR, "#counters .dt-counters");
    // 初始化DataTable配置：启用JQuery UI样式、只显示表格、显示所有行
    set(initSelector(DATATABLES),
        "{bJQueryUI:true, sDom:'t', iDisplayLength:-1}");
  }

  /**
   * 在HTML HEAD部分生成之后注入自定义CSS样式
   * @param html HTML页面构建器
   */
  @Override protected void postHead(Page.HTML<__> html) {
    // 注入计数器表格相关的CSS样式，固定表格布局和列宽
    html.
      style("#counters, .dt-counters { table-layout: fixed }",
            "#counters th { overflow: hidden; vertical-align: middle }",
            "#counters .dataTables_wrapper { min-height: 1em }",
            "#counters .group { width: 15em }",
            "#counters .name { width: 30em }");
  }

  /**
   * 获取页面内容块对应的类，渲染计数器表格具体内容
   * @return 计数器内容块类对象
   */
  @Override protected Class<? extends SubView> content() {
    return CountersBlock.class;
  }
}