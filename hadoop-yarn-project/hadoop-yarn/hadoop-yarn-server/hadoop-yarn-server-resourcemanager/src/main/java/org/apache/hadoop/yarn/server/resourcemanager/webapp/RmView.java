// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this except in compliance
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

import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.util.StringHelper.sjoin;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

// Do NOT rename/refactor this to RMView as it will wreak havoc
// on macOS HFS
/**
 * YARN ResourceManager Web UI 主页面布局类，实现两栏布局框架，
 * 负责渲染应用列表页面整体结构，整合导航栏和应用列表内容块。
 */
public class RmView extends TwoColumnLayout {
  // 直接表格渲染的最大显示行数
  static final int MAX_DISPLAY_ROWS = 100;  // direct table rendering
  // 前端JS数组渲染的最大行数阈值
  static final int MAX_FAST_ROWS = 1000;    // inline js array

  @Override
  /**
   * 在HTML头部加载前进行页面初始化配置，设置页面资源和标题
   */
  protected void preHead(Page.HTML<__> html) {
    // 执行公共头部初始化
    commonPreHead(html);
    // 设置应用列表表格ID
    set(DATATABLES_ID, "apps");
    // 绑定应用列表表格初始化脚本
    set(initID(DATATABLES, "apps"), initAppsTable());
    // 设置表格样式，指定队列列和UI列宽度
    setTableStyles(html, "apps", ".queue {width:6em}", ".ui {width:8em}");

    // Set the correct title.
    // 获取请求中指定的应用状态过滤条件
    String reqState = $(APP_STATE);
    // 处理空状态参数，默认显示全部应用
    reqState = (reqState == null || reqState.isEmpty() ? "All" : reqState);
    // 设置页面标题为[状态] Applications
    setTitle(sjoin(reqState, "Applications"));
  }

  /**
   * 公共头部初始化逻辑，设置导航栏手风琴组件配置
   */
  protected void commonPreHead(Page.HTML<__> html) {
    // 设置导航手风琴组件ID
    set(ACCORDION_ID, "nav");
    // 初始化手风琴组件，禁用自动高度，默认展开第一个菜单项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override
  /**
   * 获取左侧导航栏子视图类
   * @return 导航栏块类
   */
  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }

  @Override
  /**
   * 获取右侧内容区域子视图类
   * @return 带 metrics 的应用列表块类
   */
  protected Class<? extends SubView> content() {
    return AppsBlockWithMetrics.class;
  }

  /**
   * 生成应用列表表格的初始化配置脚本
   * @return DataTables 初始化JSON配置
   */
  protected String initAppsTable() {
    return WebPageUtils.appsTableInit();
  }
}