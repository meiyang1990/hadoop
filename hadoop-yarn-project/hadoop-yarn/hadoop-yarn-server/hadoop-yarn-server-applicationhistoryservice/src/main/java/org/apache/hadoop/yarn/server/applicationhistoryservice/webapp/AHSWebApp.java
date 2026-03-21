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

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import org.apache.hadoop.yarn.api.ApplicationBaseProtocol;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryClientService;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import javax.servlet.Filter;

/**
 * 应用历史服务(AHS)Web应用主类，负责配置和注册AHS Web界面的路由与服务依赖
 */
public class AHSWebApp extends WebApp implements YarnWebParams {

  private final ApplicationHistoryClientService historyClientService;
  private TimelineDataManager timelineDataManager;

  /**
   * 构造AHSWeb应用实例，初始化依赖服务
   * @param timelineDataManager 时间轴数据管理器，负责存储和查询应用事件时间轴数据
   * @param historyClientService 应用历史客户端服务，提供应用历史数据查询接口
   */
  public AHSWebApp(TimelineDataManager timelineDataManager,
      ApplicationHistoryClientService historyClientService) {
    this.timelineDataManager = timelineDataManager;
    this.historyClientService = historyClientService;
  }

  /**
   * 获取应用历史客户端服务实例
   * @return 应用历史客户端服务实例
   */
  public ApplicationHistoryClientService getApplicationHistoryClientService() {
    return historyClientService;
  }

  /**
   * 获取时间轴数据管理器实例
   * @return 时间轴数据管理器实例
   */
  public TimelineDataManager getTimelineDataManager() {
    return timelineDataManager;
  }

  /**
   * 配置Web应用依赖绑定和路由规则
   */
  @Override
  public void setup() {
    // 绑定全局异常处理器
    bind(GenericExceptionHandler.class);
    // 将应用基础协议绑定到历史客户端服务实例
    bind(ApplicationBaseProtocol.class).toInstance(historyClientService);
    // 将时间轴数据管理器绑定到已有实例
    bind(TimelineDataManager.class).toInstance(timelineDataManager);
    // 首页路由
    route("/", AHSController.class);
    // 关于页面路由
    route("/about", AHSController.class, "about");
    // 按应用状态过滤的应用列表路由
    route(pajoin("/apps", APP_STATE), AHSController.class);
    // 指定应用详情路由
    route(pajoin("/app", APPLICATION_ID), AHSController.class, "app");
    // 指定应用尝试详情路由
    route(pajoin("/appattempt", APPLICATION_ATTEMPT_ID), AHSController.class,
      "appattempt");
    // 指定容器详情路由
    route(pajoin("/container", CONTAINER_ID), AHSController.class, "container");
    // 容器日志路由
    route(
      pajoin("/logs", NM_NODENAME, CONTAINER_ID, ENTITY_STRING, APP_OWNER,
        CONTAINER_LOG_TYPE), AHSController.class, "logs");
    // 应用错误警告信息页面路由
    route("/errors-and-warnings", AHSController.class, "errorsAndWarnings");
  }

  @Override
  protected Class<? extends Filter> getWebAppFilterClass() {
    return null;
  }
}