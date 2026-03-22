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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.server.webapp.WebPageUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * 文件说明：MapReduce历史服务器Web UI导航栏块
 * 功能：为历史服务器各个页面生成侧边导航菜单，根据当前浏览上下文动态显示不同层级的导航链接
 */
public class HsNavBlock extends HtmlBlock {
  final App app;
  private Configuration conf;

  /**
   * 构造方法，通过依赖注入初始化导航块
   * @param app MapReduce应用上下文对象，包含当前浏览的作业、任务信息
   * @param conf Hadoop配置对象，用于获取页面配置信息
   */
  @Inject HsNavBlock(App app, Configuration conf) {
    this.app = app;
    this.conf = conf;
  }

  /**
   * 渲染导航栏HTML内容，根据当前浏览上下文动态生成导航链接
   * @param html HTML块输出对象
   */
  @Override protected void render(Block html) {
    // 构建应用顶级导航
    DIV<Hamlet> nav = html.
      div("#nav").
      h3("Application").
        ul().
          li().a(url("about"), "About").__().
          li().a(url("app"), "Jobs").__().__();
    // 如果当前浏览某个作业，添加作业级导航
    if (app.getJob() != null) {
      String jobid = MRApps.toString(app.getJob().getID());
      nav.
        h3("Job").
        ul().
          li().a(url("job", jobid), "Overview").__().
          li().a(url("jobcounters", jobid), "Counters").__().
          li().a(url("conf", jobid), "Configuration").__().
          li().a(url("tasks", jobid, "m"), "Map tasks").__().
          li().a(url("tasks", jobid, "r"), "Reduce tasks").__().__();
      // 如果当前浏览某个任务，添加任务级导航
      if (app.getTask() != null) {
        String taskid = MRApps.toString(app.getTask().getID());
        nav.
          h3("Task").
          ul().
            li().a(url("task", taskid), "Task Overview").__().
            li().a(url("taskcounters", taskid), "Counters").__().__();
      }
    }

    // 添加工具 section，支持扩展自定义工具链接
    Hamlet.UL<DIV<Hamlet>> tools = WebPageUtils.appendToolSection(nav, conf);

    if (tools != null) {
      tools.__().__();
    }
  }
}