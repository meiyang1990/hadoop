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

package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * YARN Router Web UI 主视图类，采用两栏布局实现Router Web界面。
 */
public class RouterView extends TwoColumnLayout {

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 调用通用头部预处理逻辑
    commonPreHead(html);

    // 设置页面标题
    setTitle("Router");
  }

  /**
   * 通用头部预处理，配置jQuery UI组件参数。
   */
  protected void commonPreHead(Page.HTML<__> html) {
    // 设置导航手风琴组件ID
    set(ACCORDION_ID, "nav");
    // 初始化导航手风琴，禁用自动高度，默认展开第一个菜单
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override
  protected Class<? extends SubView> nav() {
    // 返回左侧导航栏子视图
    return NavBlock.class;
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回右侧内容区默认显示的关于页面子视图
    return AboutBlock.class;
  }
}