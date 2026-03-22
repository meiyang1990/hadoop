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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

import org.apache.hadoop.mapreduce.v2.app.webapp.CountersBlock;
import org.apache.hadoop.yarn.webapp.SubView;

/**
 * 历史服务器任务计数器页面，负责渲染已完成任务的计数器信息页面
 * 继承HsView基类，实现历史服务器Web UI的计数器页面布局与样式
 */
public class HsCountersPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 在HTML head部分渲染前进行页面初始化配置
   * @param html HTML页面根元素对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 执行通用页面头部初始化
    commonPreHead(html);
    // 设置任务导航列为当前激活状态
    setActiveNavColumnForTask();
    // 配置DataTables插件选择器，绑定到计数器表格
    set(DATATABLES_SELECTOR, "#counters .dt-counters");
    // 初始化DataTables插件，启用jQuery UI样式，仅显示表格，关闭分页
    set(initSelector(DATATABLES),
        "{bJQueryUI:true, sDom:'t', iDisplayLength:-1}");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#postHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 在HTML head部分渲染完成后添加自定义CSS样式
   * @param html HTML页面根元素对象
   */
  @Override protected void postHead(Page.HTML<__> html) {
    // 添加计数器表格的样式定义，固定表格布局，设置列宽与溢出处理
    html.
      style("#counters, .dt-counters { table-layout: fixed }",
            "#counters th { overflow: hidden; vertical-align: middle }",
            "#counters .dataTables_wrapper { min-height: 1em }",
            "#counters .group { width: 15em }",
            "#counters .name { width: 30em }");
  }

  /**
   * 获取页面内容区域对应的子视图类，计数器页面内容由CountersBlock渲染
   * @return 计数器块视图类
   */
  @Override protected Class<? extends SubView> content() {
    return CountersBlock.class;
  }
}