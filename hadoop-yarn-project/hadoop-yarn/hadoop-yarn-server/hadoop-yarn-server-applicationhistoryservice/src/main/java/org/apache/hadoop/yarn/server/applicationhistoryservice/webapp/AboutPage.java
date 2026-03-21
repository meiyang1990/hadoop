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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;


import org.apache.hadoop.yarn.webapp.SubView;

/**
 * 应用历史服务Web界面的关于页面
 * 负责渲染Timeline Server/通用历史服务的关于页面整体框架
 */
public class AboutPage extends AHSView {
  @Override
  protected void preHead(Page.HTML<__> html) {
    // 执行通用页面头部预处理
    commonPreHead(html);
    // 设置页面标题
    set(TITLE, "Timeline Server - Generic History Service");
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回关于页面内容块对应的类
    return AboutBlock.class;
  }
}