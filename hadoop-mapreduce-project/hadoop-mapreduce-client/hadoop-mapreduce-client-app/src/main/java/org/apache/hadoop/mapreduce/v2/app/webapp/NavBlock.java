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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.RM_WEB;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.mapreduce.v2.util.MRWebAppUtil;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * MapReduce ApplicationMaster Web UI 导航栏块，负责渲染页面顶部导航菜单
 * 根据当前页面层级（应用/作业/任务）动态生成对应导航链接
 */
public class NavBlock extends HtmlBlock {
  final App app;

  /**
   * 构造方法，注入App上下文对象
   * @param app 当前MR应用Web上下文实例
   */
  @Inject NavBlock(App app) { this.app = app; }

  /**
   * 渲染导航栏HTML内容，根据当前访问层级动态生成导航菜单
   * @param html HTML块输出对象
   */
  @Override protected void render(Block html) {
    // 获取ResourceManager Web服务地址
    String rmweb = $(RM_WEB);
    // 开始构建导航栏容器，先添加集群级导航链接
    DIV<Hamlet> nav = html.
      div("#nav").
        h3("Cluster").
        ul().
          li().a(url(rmweb, "cluster", "cluster"), "About").__().
          li().a(url(rmweb, "cluster", "apps"), "Applications").__().
          li().a(url(rmweb, "cluster", "scheduler"), "Scheduler").__().__().
        // 添加应用级导航链接
        h3("Application").
        ul().
          li().a(url("app/info"), "About").__().
          li().a(url("app"), "Jobs").__().__();
    // 当前已选中具体作业，添加作业级导航菜单
    if (app.getJob() != null) {
      String jobid = MRApps.toString(app.getJob().getID());
      List<AMInfo> amInfos = app.getJob().getAMInfos();
      // 获取当前运行的AM信息（取最后一次尝试的AM）
      AMInfo thisAmInfo = amInfos.get(amInfos.size()-1);
      // 拼接NodeManager的HTTP访问地址
      String nodeHttpAddress = thisAmInfo.getNodeManagerHost() + ":" 
          + thisAmInfo.getNodeManagerHttpPort();
      // 添加作业导航菜单项
      nav.
        h3("Job").
        ul().
          li().a(url("job", jobid), "Overview").__().
          li().a(url("jobcounters", jobid), "Counters").__().
          li().a(url("conf", jobid), "Configuration").__().
          li().a(url("tasks", jobid, "m"), "Map tasks").__().
          li().a(url("tasks", jobid, "r"), "Reduce tasks").__().
          // 添加跳转到NM查看AM日志的链接
          li().a(".logslink", url(MRWebAppUtil.getYARNWebappScheme(),
              nodeHttpAddress, "node",
              "containerlogs", thisAmInfo.getContainerId().toString(), 
              app.getJob().getUserName()), 
              "AM Logs").__().__();
      // 当前已选中具体任务，添加任务级导航菜单
      if (app.getTask() != null) {
        String taskid = MRApps.toString(app.getTask().getID());
        nav.
          h3("Task").
          ul().
            li().a(url("task", taskid), "Task Overview").__().
            li().a(url("taskcounters", taskid), "Counters").__().__();
      }
    }
    // 添加工具类导航链接，结束导航栏构建
    nav.
      h3("Tools").
      ul().
        li().a("/conf", "Configuration").__().
        li().a("/logs", "Local logs").__().
        li().a("/stacks", "Server stacks").__().
        li().a("/jmx?qry=Hadoop:*", "Server metrics").__().__().__();
  }
}