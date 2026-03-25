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

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.yarn.webapp.WebApp;

import javax.servlet.Filter;

/**
 * MapReduce ApplicationMaster Web应用入口类
 * 负责初始化AM Web界面、配置路由规则，对外提供作业运行状态的Web监控能力
 */
public class AMWebApp extends WebApp implements AMParams {

  private AppContext appContext;

  /**
   * 构造AM Web应用实例
   * @param appContext ApplicationMaster上下文，包含当前作业的所有运行状态信息
   */
  public AMWebApp(AppContext appContext) {
    this.appContext = appContext;
  }

  /**
   * 初始化Web应用，完成依赖注入绑定并配置所有页面路由规则
   */
  @Override
  public void setup() {
    // 将应用上下文实例绑定到依赖注入容器
    bind(AppContext.class).toInstance(appContext);
    // 根路径路由到应用首页控制器
    route("/", AppController.class);
    // 应用概要页面路由
    route("/app", AppController.class);
    // mapreduce作业根路径路由
    route("/mapreduce", AppController.class);
    // 单个作业详情页面路由
    route(pajoin("/job", JOB_ID), AppController.class, "job");
    // 作业配置信息页面路由
    route(pajoin("/conf", JOB_ID), AppController.class, "conf");
    // 作业计数器汇总页面路由
    route(pajoin("/jobcounters", JOB_ID), AppController.class, "jobCounters");
    // 单个作业计数器详情页面路由
    route(pajoin("/singlejobcounter",JOB_ID, COUNTER_GROUP, COUNTER_NAME),
        AppController.class, "singleJobCounter");
    // 任务列表页面路由（按任务类型和状态过滤）
    route(pajoin("/tasks", JOB_ID, TASK_TYPE, TASK_STATE), AppController.class, "tasks");
    // 任务尝试列表页面路由（按任务类型和尝试状态过滤）
    route(pajoin("/attempts", JOB_ID, TASK_TYPE, ATTEMPT_STATE),
        AppController.class, "attempts");
    // 单个任务详情页面路由
    route(pajoin("/task", TASK_ID), AppController.class, "task");
    // 单个任务计数器页面路由
    route(pajoin("/taskcounters", TASK_ID), AppController.class, "taskCounters");
    // 单个任务计数器详情页面路由
    route(pajoin("/singletaskcounter",TASK_ID, COUNTER_GROUP, COUNTER_NAME),
        AppController.class, "singleTaskCounter");
  }

  @Override
  protected Class<? extends Filter> getWebAppFilterClass() {
    return null;
  }
}