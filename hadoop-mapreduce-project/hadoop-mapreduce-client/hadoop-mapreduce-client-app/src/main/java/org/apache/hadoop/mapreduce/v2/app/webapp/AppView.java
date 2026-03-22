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
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * MapReduce应用Master Web页面主视图，采用双栏布局实现页面整体框架
 * 负责整合导航栏和作业列表内容区，初始化页面所需的前端UI组件
 */
public class AppView extends TwoColumnLayout {

  /**
   * 在HTML头部渲染前执行初始化配置，设置DataTables表格组件参数
   * @param html HTML页面构建对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    // 设置DataTables表格组件ID为jobs
    set(DATATABLES_ID, "jobs");
    // 初始化jobs表格的配置项
    set(initID(DATATABLES, "jobs"), jobsTableInit());
    // 设置jobs表格的CSS样式
    setTableStyles(html, "jobs");
  }

  /**
   * 通用头部初始化配置，设置导航栏手风琴组件参数
   * @param html HTML页面构建对象
   */
  protected void commonPreHead(Page.HTML<__> html) {
    // 设置导航手风琴组件ID为nav
    set(ACCORDION_ID, "nav");
    // 初始化手风琴组件：禁用自动高度，默认展开第二个菜单项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
  }

  /**
   * 获取左侧导航栏子视图类
   * @return 导航块视图类
   */
  @Override
  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }

  /**
   * 获取右侧内容区子视图类
   * @return 作业列表内容块视图类
   */
  @Override
  protected Class<? extends SubView> content() {
    return JobsBlock.class;
  }

  /**
   * 生成jobs表格DataTables组件初始化配置JSON
   * @return 表格初始化配置字符串
   */
  private String jobsTableInit() {
    return tableInit().
        // 页面加载后默认按第一列（作业ID）升序排序
        append(", aaSorting: [[0, 'asc']]").
        append(",aoColumns:[{sType:'title-numeric'},").
        append("null,null,{sType:'title-numeric', bSearchable:false},null,").
        append("null,{sType:'title-numeric',bSearchable:false}, null, null]}").
        toString();
  }
}