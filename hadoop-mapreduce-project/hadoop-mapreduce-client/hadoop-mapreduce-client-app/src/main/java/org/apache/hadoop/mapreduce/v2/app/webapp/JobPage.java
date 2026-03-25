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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * MapReduce ApplicationMaster Web UI 作业详情页面实现类
 * 负责渲染单个MapReduce作业的详情页面，配置页面标题和导航，指定内容区块
 */
public class JobPage extends AppView {

  /**
   * 在HTML head部分渲染前执行预处理，配置页面标题、导航样式
   * @param html HTML页面构建器
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 从请求中获取作业ID参数
    String jobID = $(JOB_ID);
    // 设置页面标题，缺少作业ID时显示错误提示
    set(TITLE, jobID.isEmpty() ? "Bad request: missing job ID"
               : join("MapReduce Job ", $(JOB_ID)));
    // 执行通用预处理逻辑
    commonPreHead(html);

    // 初始化导航折叠面板，设置默认激活第二个标签（作业配置标签）
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:2}");
  }

  /**
   * 获取页面内容区块的实现类
   * @return 作业详情内容区块类
   */
  @Override protected Class<? extends SubView> content() {
    return JobBlock.class;
  }
}