// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain copy of the License at
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

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.util.Log4jWarningErrorMetricsAppender;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import static org.apache.hadoop.util.GenericsUtil.isLog4jLogger;

/**
 * 应用历史服务Web界面导航栏渲染块
 * 负责生成页面顶部导航菜单，包括应用历史主入口和按状态筛选的应用列表链接
 */
public class NavBlock extends HtmlBlock {

  private Configuration conf;

  @Inject
  public NavBlock(Configuration conf) {
    this.conf = conf;
  }

  /**
   * 渲染导航栏HTML内容
   * @param html HTML块输出对象
   */
  @Override
  public void render(Block html) {
    // 标记是否需要添加错误警告日志链接
    boolean addErrorsAndWarningsLink = false;
    // 检查当前类使用的是Log4j日志
    if (isLog4jLogger(NavBlock.class)) {
      // 查找错误警告日志统计appender
      Log4jWarningErrorMetricsAppender appender =
          Log4jWarningErrorMetricsAppender.findAppender();
      // 如果appender存在，显示错误警告链接
      if (appender != null) {
        addErrorsAndWarningsLink = true;
      }
    }
    // 构建导航栏主结构，添加应用历史主标题
    Hamlet.DIV<Hamlet> nav = html.
        div("#nav").
            h3("Application History").
                ul().
                    li().a(url("about"), "About").
        __().
                    li().a(url("apps"), "Applications").
                        ul().
                            // 添加已完成状态应用筛选链接
                            li().a(url("apps",
                                YarnApplicationState.FINISHED.toString()),
                                YarnApplicationState.FINISHED.toString()).
        __().
                            // 添加失败状态应用筛选链接
                            li().a(url("apps",
                                YarnApplicationState.FAILED.toString()),
                                YarnApplicationState.FAILED.toString()).
        __().
                            // 添加 killed 状态应用筛选链接
                            li().a(url("apps",
                                YarnApplicationState.KILLED.toString()),
                                YarnApplicationState.KILLED.toString()).
        __().
        __().
        __().
        __();

    // 添加工具区域到导航栏
    Hamlet.UL<Hamlet.DIV<Hamlet>> tools = WebPageUtils.appendToolSection(nav, conf);

    // 如果工具区域创建失败，提前结束渲染
    if (tools == null) {
      return;
    }

    // 需要显示错误警告链接时，添加到工具区域
    if (addErrorsAndWarningsLink) {
      tools.li().a(url("errors-and-warnings"), "Errors/Warnings").__();
    }
    // 闭合工具区域和导航栏HTML标签
    tools.__().__();
  }
}