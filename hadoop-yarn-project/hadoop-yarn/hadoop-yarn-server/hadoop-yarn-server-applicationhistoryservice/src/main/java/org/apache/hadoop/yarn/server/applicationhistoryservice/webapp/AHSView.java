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

import static org.apache.hadoop.yarn.util.StringHelper.sjoin;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;
import org.apache.hadoop.yarn.server.webapp.AppsBlock;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

/**
 * 应用历史服务(AHS)应用列表页面主视图
 * 采用双栏布局，左侧导航+右侧应用列表内容
 * 注意：不可重命名为AHSView以外的名称，会导致Mac OS HFS文件系统兼容性问题
 */
// Do NOT rename/refactor this to AHSView as it will wreak havoc
// on Mac OS HFS
public class AHSView extends TwoColumnLayout {
  // 直接表格渲染的最大显示行数
  static final int MAX_DISPLAY_ROWS = 100; // direct table rendering
  // 内联JS数组渲染的最大行数
  static final int MAX_FAST_ROWS = 1000; // inline js array

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行公共头部预处理
    commonPreHead(html);
    // 设置应用列表表格ID
    set(DATATABLES_ID, "apps");
    // 初始化应用列表DataTables配置
    set(initID(DATATABLES, "apps"), WebPageUtils.appsTableInit(false));
    // 设置表格列样式
    setTableStyles(html, "apps", ".queue {width:6em}", ".ui {width:8em}");

    // 设置页面标题，根据请求的应用状态过滤
    String reqState = $(APP_STATE);
    reqState = (reqState == null || reqState.isEmpty() ? "All" : reqState);
    setTitle(sjoin(reqState, "Applications"));
  }

  /**
   * 公共头部预处理，初始化导航手风琴组件
   */
  protected void commonPreHead(Page.HTML<__> html) {
    // 设置导航手风琴组件ID
    set(ACCORDION_ID, "nav");
    // 初始化导航手风琴配置，关闭自动高度，默认展开第一个
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override
  protected Class<? extends SubView> nav() {
    // 返回左侧导航块实现类
    return NavBlock.class;
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回右侧应用列表内容块实现类
    return AppsBlock.class;
  }
}