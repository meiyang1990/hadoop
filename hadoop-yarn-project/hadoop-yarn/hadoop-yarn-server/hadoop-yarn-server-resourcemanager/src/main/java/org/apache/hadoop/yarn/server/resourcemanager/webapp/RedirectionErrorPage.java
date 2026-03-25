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

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

/**
 * YARN RM Web UI 重定向错误页面
 * 当RM Web代理访问ApplicationMaster UI发生重定向循环时，展示错误提示信息
 */
public class RedirectionErrorPage extends RmView {
  @Override
  protected void preHead(Page.HTML<__> html) {
    // 获取请求中的应用ID
    String aid = $(YarnWebParams.APPLICATION_ID);

    // 执行通用头部预处理
    commonPreHead(html);
    // 设置错误提示信息，说明重定向循环原因和解决方法
    set(YarnWebParams.ERROR_MESSAGE,
        "The application master for " + aid + " redirected the "
        + "resource manager's web proxy's request back to the web proxy, "
        + "which means your request to view the application master's web UI "
        + "cannot be fulfilled. The typical cause for this error is a "
        + "network misconfiguration that causes the resource manager's web "
        + "proxy host to resolve to an unexpected IP address on the "
        + "application master host. Please contact your cluster "
        + "administrator to resolve the issue.");
  }

  @Override
  protected Class<? extends SubView> content() {
    // 返回错误块视图类，展示错误信息
    return ErrorBlock.class;
  }
}