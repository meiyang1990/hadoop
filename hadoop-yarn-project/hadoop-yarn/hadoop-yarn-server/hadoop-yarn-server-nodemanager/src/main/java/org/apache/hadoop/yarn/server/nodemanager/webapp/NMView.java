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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

/**
 * NodeManager Web UI 基础视图类，提供双栏布局基础框架
 */
public class NMView extends TwoColumnLayout {

  /**
   * HTML头渲染前的预处理，注入页面公共配置
   */
  @Override protected void preHead(Page.HTML<__> html) {
      commonPreHead(html);
    }

  /**
   * 公共预处理逻辑，配置左侧导航手风琴菜单
   */
  protected void commonPreHead(Page.HTML<__> html) {
    // 设置导航手风琴组件ID
    set(ACCORDION_ID, "nav");
    // 初始化手风琴组件，禁用自动高度，默认展开第一个菜单项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  /**
   * 获取左侧导航栏子视图类
   */
  @Override
  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }
}