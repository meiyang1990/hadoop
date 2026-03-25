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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.TASK_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * 单个计数器页面视图，负责渲染MapReduce应用程序/任务级计数器Web页面
 * 继承自AppView基类，整合JQuery UI实现页面交互与表格展示
 */
public class SingleCounterPage extends AppView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 页面HEAD区域预处理，配置页面UI组件与导航状态
   * @param html HTML页面根节点对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 调用通用预处理逻辑
    commonPreHead(html);
    // 获取请求参数中的任务ID
    String tid = $(TASK_ID);
    // 默认选中第二个导航项（作业级计数器）
    String activeNav = "3";
    // 如果没有任务ID，选中第一个导航项
    if(tid == null || tid.isEmpty()) {
      activeNav = "2";
    }
    // 初始化折叠导航栏，设置默认激活项
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:"+activeNav+"}");
    // 设置数据表格ID
    set(DATATABLES_ID, "singleCounter");
    // 初始化计数器表格配置
    set(initID(DATATABLES, "singleCounter"), counterTableInit());
    // 设置计数器表格样式
    setTableStyles(html, "singleCounter");
  }

  /**
   * 生成计数器表格的jQuery DataTables初始化配置
   * @return 表格初始化JSON配置字符串
   */
  private String counterTableInit() {
    return tableInit().
        append(",aoColumnDefs:[").
        append("{'sType':'title-numeric', 'aTargets': [ 1 ] }").
        append("]}").
        toString();
  }
  
  /**
   * 获取页面内容区块对应的SubView类
   * @return 单个计数器区块渲染类
   */
  @Override protected Class<? extends SubView> content() {
    return SingleCounterBlock.class;
  }
}