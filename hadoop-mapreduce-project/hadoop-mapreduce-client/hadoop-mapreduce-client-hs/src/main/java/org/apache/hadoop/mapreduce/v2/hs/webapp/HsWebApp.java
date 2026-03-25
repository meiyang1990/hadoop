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

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_OWNER;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_LOG_TYPE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.ENTITY_STRING;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NM_NODENAME;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMParams;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.yarn.webapp.WebApp;

import com.google.inject.Singleton;

/**
 * MapReduce 历史服务器Web应用入口类
 * 负责历史服务器Web界面的依赖注入配置与URL路由注册，提供已完成MapReduce作业的历史信息查询服务
 */
public class HsWebApp extends WebApp implements AMParams {

  private HistoryContext history;

  /**
   * 构造历史服务器Web应用实例
   * @param history 历史上下文对象，提供已完成作业历史数据的访问能力
   */
  public HsWebApp(HistoryContext history) {
    this.history = history;
  }

  /**
   * 初始化Web应用，配置依赖注入并注册所有URL路由规则
   */
  @Override
  public void setup() {
    // 绑定App类为单例
    bind(App.class).in(Singleton.class);
    // 将历史上下文实例注入到AppContext接口
    bind(AppContext.class).toInstance(history);
    // 将历史上下文实例注入到HistoryContext接口
    bind(HistoryContext.class).toInstance(history);
    // 注册根路径路由
    route("/", HsController.class);
    // 注册应用页面路由
    route("/app", HsController.class);
    // 注册作业历史首页路由
    route("/jobhistory", HsController.class);
    // 注册单个作业详情页面路由
    route(pajoin("/job", JOB_ID), HsController.class, "job");
    // 注册作业配置页面路由
    route(pajoin("/conf", JOB_ID), HsController.class, "conf");
    // 注册作业配置下载路由，不使用默认视图
    routeWithoutDefaultView(pajoin("/downloadconf", JOB_ID),
        HsController.class, "downloadConf");
    // 注册作业计数器页面路由
    route(pajoin("/jobcounters", JOB_ID), HsController.class, "jobCounters");
    // 注册单个作业计数器详情路由
    route(pajoin("/singlejobcounter",JOB_ID, COUNTER_GROUP, COUNTER_NAME),
        HsController.class, "singleJobCounter");
    // 注册任务列表页面路由
    route(pajoin("/tasks", JOB_ID, TASK_TYPE), HsController.class, "tasks");
    // 注册尝试列表页面路由
    route(pajoin("/attempts", JOB_ID, TASK_TYPE, ATTEMPT_STATE),
        HsController.class, "attempts");
    // 注册单个任务详情页面路由
    route(pajoin("/task", TASK_ID), HsController.class, "task");
    // 注册任务计数器页面路由
    route(pajoin("/taskcounters", TASK_ID), HsController.class, "taskCounters");
    // 注册单个任务计数器详情路由
    route(pajoin("/singletaskcounter",TASK_ID, COUNTER_GROUP, COUNTER_NAME),
        HsController.class, "singleTaskCounter");
    // 注册关于页面路由
    route("/about", HsController.class, "about");
    // 注册容器日志查看路由
    route(pajoin("/logs", NM_NODENAME, CONTAINER_ID, ENTITY_STRING, APP_OWNER,
        CONTAINER_LOG_TYPE), HsController.class, "logs");
    // 注册NodeManager容器日志获取路由
    route(pajoin("/nmlogs", NM_NODENAME, CONTAINER_ID, ENTITY_STRING, APP_OWNER,
        CONTAINER_LOG_TYPE), HsController.class, "nmlogs");
  }
}