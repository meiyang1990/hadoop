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

package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * YARN Router Web UI 的关于页面控制器，负责渲染关于页面整体结构。
 */
public class AboutPage extends RouterView {

  @Override
  protected void preHead(Page.HTML<__> html) {
    // 调用父类通用的preHead处理，注入公共页面元数据和资源引用
    commonPreHead(html);
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回关于页面内容区块类，渲染页面主体内容
    return AboutBlock.class;
  }
}