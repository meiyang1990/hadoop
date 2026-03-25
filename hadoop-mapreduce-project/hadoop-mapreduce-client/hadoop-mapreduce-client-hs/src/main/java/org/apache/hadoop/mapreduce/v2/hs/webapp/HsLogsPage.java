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

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.log.AggregatedLogsBlock;

/**
 * 历史服务器聚合日志页面类，定义历史服务器Web UI中查看任务聚合日志的页面结构
 * 负责渲染任务日志页面的整体框架，指定页面内容区域使用的视图块
 */
public class HsLogsPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  /**
   * 在HTML页面head区域渲染前执行预处理，完成公共初始化与导航栏状态设置
   * @param html HTML页面根对象
   */
  @Override protected void preHead(Page.HTML<__> html) {
    // 执行通用preHead预处理逻辑
    commonPreHead(html);
    // 设置任务导航栏为激活状态
    setActiveNavColumnForTask();
  }

  /**
   * 获取页面内容区域对应的子视图类，指定聚合日志块作为页面内容
   * @return 聚合日志块视图类
   */
  @Override protected Class<? extends SubView> content() {
    return AggregatedLogsBlock.class;
  }
}