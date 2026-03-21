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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;

/**
 * 全局策略生成器(GPG)Web UI的概览页面，提供GPG服务状态的可视化展示入口
 * 继承双栏布局，左侧导航栏右侧内容区的标准UI结构
 */
public class GPGOverviewPage extends TwoColumnLayout {

  @Override
  protected void preHead(Page.HTML<__> html) {
    commonPreHead(html);
    // 设置页面标题
    setTitle("GPG");
  }

  /**
   * 初始化前端JS组件配置，设置导航手风琴效果
   * @param html HTML页面对象
   */
  protected void commonPreHead(Page.HTML<__> html) {
    set(ACCORDION_ID, "nav");
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:0}");
  }

  @Override
  protected Class<? extends SubView> nav() {
    // 返回左侧导航栏子视图
    return NavBlock.class;
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回右侧GPG概览内容子视图
    return GPGOverviewBlock.class;
  }
}