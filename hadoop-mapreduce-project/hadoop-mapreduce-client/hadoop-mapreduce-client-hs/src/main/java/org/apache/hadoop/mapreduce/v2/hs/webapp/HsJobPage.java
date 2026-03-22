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

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * 历史服务器端单个MapReduce作业详情页面渲染类
 * 负责生成作业详情页面的整体框架，并指定作业详情内容区块的渲染实现
 */
public class HsJobPage extends HsView {

  /**
   * 页面head标签渲染前的初始化处理，设置页面标题和导航栏配置
   * @param html 页面HTML对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 从请求中获取作业ID参数
    String jobID = $(JOB_ID);
    // 设置页面标题，处理作业ID缺失的错误情况
    set(TITLE, jobID.isEmpty() ? "Bad request: missing job ID"
               : join("MapReduce Job ", $(JOB_ID)));
    // 执行通用页面头部初始化
    commonPreHead(html);
    // 覆盖通用配置，设置导航手风琴菜单默认展开第二个菜单项（作业信息）
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
  }

  /**
   * 获取页面主体内容区块的渲染类
   * @return 作业详情内容区块渲染类
   */
  @Override protected Class<? extends SubView> content() {
    return HsJobBlock.class;
  }
}