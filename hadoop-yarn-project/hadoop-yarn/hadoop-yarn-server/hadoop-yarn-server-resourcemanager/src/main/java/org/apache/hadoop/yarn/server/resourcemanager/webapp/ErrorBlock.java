// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this use this file except in compliance
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

import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.ERROR_MESSAGE;

/**
 * 该类用于在YARN ResourceManager WebUI中向用户展示错误信息
 */
public class ErrorBlock extends HtmlBlock {
  @Inject
  ErrorBlock(ViewContext ctx) {
    super(ctx);
  }

  /**
   * 渲染错误信息HTML块
   * @param html HTML块构建对象
   */
  @Override
  protected void render(Block html) {
    // 将错误信息渲染为HTML段落
    html.p().__($(ERROR_MESSAGE)).__();
  }
}