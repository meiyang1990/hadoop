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

import static org.apache.hadoop.yarn.webapp.view.JQueryUI.*;

/**
 * MapReduce应用任务列表页面，负责渲染任务列表Web页面的整体框架和基础配置
 * 继承自AppView，集成MapReduce应用Web页面通用能力
 */
public class TasksPage extends AppView {

  /**
   * 在HTML head部分渲染前完成页面配置，设置UI组件初始化参数
   * @param html HTML页面构建器
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 执行通用预配置逻辑
    commonPreHead(html);
    // 设置DataTable表格ID为tasks
    set(DATATABLES_ID, "tasks");
    // 初始化导航手风琴组件，设置第三个导航项默认激活
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:2}");
    // 设置tasks表格的初始化配置
    set(initID(DATATABLES, "tasks"), tasksTableInit());
    // 设置tasks表格的样式
    setTableStyles(html, "tasks");
  }

  /**
   * 获取页面内容区块对应的视图类
   * @return TasksBlock内容区块类
   */
  @Override protected Class<? extends SubView> content() {
    return TasksBlock.class;
  }

  /**
   * 生成任务列表DataTable表格的初始化JSON配置
   * @return 表格初始化JSON字符串
   */
  private String tasksTableInit() {
    return tableInit()
      .append(", 'aaData': tasksTableData")
      .append(", bDeferRender: true")
      .append(", bProcessing: true")

      .append("\n, aoColumnDefs: [\n")
      // 第一列：任务ID列，使用自然排序，自定义Hadoop ID渲染
      .append("{'sType':'natural', 'aTargets': [0]")
      .append(", 'mRender': parseHadoopID }")

      // 第二列：进度列，数值类型，不支持搜索，自定义进度渲染
      .append("\n, {'sType':'numeric', bSearchable:false, 'aTargets': [1]")
      .append(", 'mRender': parseHadoopProgress }")


      // 第5、6列：日期列，数值类型，自定义日期渲染
      .append("\n, {'sType':'numeric', 'aTargets': [4, 5]")
      .append(", 'mRender': renderHadoopDate }")

      // 第7列：耗时列，数值类型，自定义耗时渲染
      .append("\n, {'sType':'numeric', 'aTargets': [6]")
      .append(", 'mRender': renderHadoopElapsedTime }]")

      // 页面加载完成后默认按第一列升序排序
      .append(", aaSorting: [[0, 'asc']] }").toString();
  }
}