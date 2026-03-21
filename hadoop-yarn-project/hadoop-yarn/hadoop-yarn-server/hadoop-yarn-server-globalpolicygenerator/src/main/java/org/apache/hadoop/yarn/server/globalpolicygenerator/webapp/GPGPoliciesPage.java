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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.DATATABLES_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

/**
 * 全局策略生成器Web UI的策略列表页面，继承双栏布局实现页面框架。
 */
public class GPGPoliciesPage extends TwoColumnLayout {

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
  }

  /**
   * 页面HTML head区域预处理，初始化页面标题、UI组件配置。
   * @param html HTML页面构建器
   */
  protected void commonPreHead(Page.HTML<__> html) {
    // 设置页面标题
    setTitle("Global Policy Generator Policies");
    // 配置导航折叠面板ID
    set(ACCORDION_ID, "nav");
    // 初始化折叠面板，禁用自动高度，默认展开第一个导航项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
    // 配置策略表格ID，用于初始化DataTables
    set(DATATABLES_ID, "policies");
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回主内容区子视图类型
    return GPGPoliciesBlock.class;
  }

  @Override
  protected Class<? extends SubView> nav() {
    // 返回左侧导航栏子视图类型
    return NavBlock.class;
  }

}