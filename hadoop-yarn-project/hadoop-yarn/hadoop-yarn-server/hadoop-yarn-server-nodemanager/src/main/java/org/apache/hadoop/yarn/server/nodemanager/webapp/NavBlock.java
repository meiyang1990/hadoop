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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.util.Log4jWarningErrorMetricsAppender;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

import static org.apache.hadoop.util.GenericsUtil.isLog4jLogger;

/**
 * NodeManager Web UI导航栏块，负责生成页面顶部导航菜单
 */
public class NavBlock extends HtmlBlock implements YarnWebParams {

  private Configuration conf;

  @Inject
  public NavBlock(Configuration conf) {
	 this.conf = conf;
  }
  
  @Override
  protected void render(Block html) {

    boolean addErrorsAndWarningsLink = false;
    // 检查是否使用Log4j日志框架
    if (isLog4jLogger(NMErrorsAndWarningsPage.class)) {
      // 查找错误警告日志收集Appender
      Log4jWarningErrorMetricsAppender appender = Log4jWarningErrorMetricsAppender.findAppender();
      // 存在Appender则添加错误警告导航链接
      if (appender != null) {
        addErrorsAndWarningsLink = true;
      }
    }
	
    // 获取ResourceManager Web UI完整地址
    String RMWebAppURL =
        WebAppUtils.getResolvedRMWebAppURLWithScheme(this.conf);
    // 开始构建导航栏DOM结构
    Hamlet.DIV<Hamlet> ul = html
      .div("#nav")
      .h3().__("ResourceManager").__()
        .ul()
          // 添加跳转到RM首页的链接
          .li().a(RMWebAppURL, "RM Home").__().__()
      .h3().__("NodeManager").__() // TODO: Problem if no header like this
        .ul()
          // 添加节点信息页面链接
          .li()
            .a(url("node"), "Node Information").__()
          // 添加应用列表页面链接
          .li()
            .a(url("allApplications"), "List of Applications")
            .__()
          // 添加容器列表页面链接
          .li()
            .a(url("allContainers"), "List of Containers").__()
        .__();

    // 添加工具区域导航节
    Hamlet.UL<Hamlet.DIV<Hamlet>> tools = WebPageUtils.appendToolSection(ul, conf);

    // 没有工具区域则结束渲染
    if (tools == null) {
      return;
    }
    // 满足条件添加错误警告页面导航链接
    if (addErrorsAndWarningsLink) {
      tools.li().a(url("errors-and-warnings"), "Errors/Warnings").__();
    }
    // 闭合DOM标签
    tools.__().__();
  }

}