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

import org.apache.hadoop.mapreduce.v2.app.webapp.SingleCounterBlock;
import org.apache.hadoop.yarn.webapp.SubView;

/**
 * 历史服务器单任务计数器页面渲染类，负责构建显示单个任务计数器信息的Web页面
 * 继承自HsView，复用历史服务器通用页面视图能力
 */
public class HsSingleCounterPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 页面HEAD区域预处理，注入必要的CSS、JS和页面配置
   * @param html HTML页面根节点对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    commonPreHead(html); // 执行通用预处理逻辑
    setActiveNavColumnForTask(); // 设置任务导航栏为激活状态
    set(DATATABLES_ID, "singleCounter"); // 设置DataTable表格ID
    set(initID(DATATABLES, "singleCounter"), counterTableInit()); // 初始化DataTable配置
    setTableStyles(html, "singleCounter"); // 设置表格样式
  }

  /**
   * 生成计数器表格的jQuery DataTables初始化配置
   * @return 格式化后的DataTables初始化JSON字符串
   */
  private String counterTableInit() {
    return tableInit().
        append(", aoColumnDefs:[").
        append("{'sType':'title-numeric', 'aTargets': [ 1 ] }").
        append("]}").
        toString();
  }
  
  /**
   * 获取页面内容区块渲染类，负责渲染核心的计数器内容
   * @return 内容区块对应的SubView实现类
   */
  @Override protected Class<? extends SubView> content() {
    return SingleCounterBlock.class;
  }
}