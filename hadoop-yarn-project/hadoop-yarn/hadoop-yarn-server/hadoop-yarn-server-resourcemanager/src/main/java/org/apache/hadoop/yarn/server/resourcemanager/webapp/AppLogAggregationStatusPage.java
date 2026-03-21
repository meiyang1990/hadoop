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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

/**
 * YARN ResourceManager WebUI 应用日志聚合状态页面，展示指定应用的日志聚合进度与状态信息。
 */
public class AppLogAggregationStatusPage extends RmView{

  /**
   * 在HTML head部分渲染前执行，设置页面标题等公共头部信息。
   */
  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行公共头部预处理逻辑
    commonPreHead(html);
    // 从请求参数获取应用ID
    String appId = $(YarnWebParams.APPLICATION_ID);
    // 设置页面标题，缺少应用ID时返回错误提示
    set(
      TITLE,
      appId.isEmpty() ? "Bad request: missing application ID" : join(
        "Application ", $(YarnWebParams.APPLICATION_ID)));
  }

  /**
   * 获取页面内容区块类，渲染日志聚合状态内容。
   * @return 日志聚合状态区块类对象
   */
  @Override
  protected Class<? extends SubView> content() {
    return RMAppLogAggregationStatusBlock.class;
  }
}