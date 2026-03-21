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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebServiceProtocol;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.webapp.WebServices;
import org.apache.hadoop.yarn.server.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainerInfo;
import org.apache.hadoop.yarn.server.webapp.dao.ContainersInfo;

/**
 * YARN Router REST请求拦截器接口，定义了拦截器需要实现的契约，用于拦截检查客户端发往资源管理器的REST请求。
 * 该接口定义了几个与应用尝试、容器信息查询相关的REST方法，确保RouterWebServices实现与RMWebServices一致的接口。
 */
public interface RESTRequestInterceptor
    extends RMWebServiceProtocol, Configurable {

  /**
   * 初始化拦截器，该方法在拦截器实例生命周期中保证只被调用一次。
   *
   * @param user 客户端用户名
   */
  void init(String user);

  /**
   * 关闭拦截器，释放拦截器持有的资源，在应用销毁时调用。
   * 具体实现需要释放资源并将请求转发给下一个拦截器（如果存在）。
   */
  void shutdown();

  /**
   * 设置拦截器责任链中的下一个拦截器。
   * 实现类需要在处理完请求后将请求转发给下一个拦截器。
   * 链中最后一个拦截器负责将请求发送给实际的资源管理器服务，因此最后一个拦截器不会调用此方法。
   *
   * @param nextInterceptor 责任链中的下一个拦截器
   */
  void setNextInterceptor(RESTRequestInterceptor nextInterceptor);

  /**
   * 获取责任链中的下一个拦截器。
   *
   * @return 责任链中的下一个拦截器
   */
  RESTRequestInterceptor getNextInterceptor();

  /**
   * 获取指定应用尝试的信息
   *
   * @see WebServices#getAppAttempt(HttpServletRequest, HttpServletResponse,
   *      String, String)
   * @param req servlet请求对象
   * @param res servlet响应对象
   * @param appId 应用ID，URL路径参数
   * @param appAttemptId 应用尝试ID，URL路径参数
   * @return 指定应用尝试的信息对象
   */
  AppAttemptInfo getAppAttempt(HttpServletRequest req, HttpServletResponse res,
      String appId, String appAttemptId);

  /**
   * 获取指定应用尝试下的所有容器信息
   *
   * @see WebServices#getContainers(HttpServletRequest, HttpServletResponse,
   *      String, String)
   * @param req servlet请求对象
   * @param res servlet响应对象
   * @param appId 应用ID，URL路径参数
   * @param appAttemptId 应用尝试ID，URL路径参数
   * @return 指定应用尝试下所有容器的信息对象
   */
  ContainersInfo getContainers(HttpServletRequest req, HttpServletResponse res,
      String appId, String appAttemptId);

  /**
   * 获取指定容器的信息
   *
   * @see WebServices#getContainer(HttpServletRequest, HttpServletResponse,
   *      String, String, String)
   * @param req servlet请求对象
   * @param res servlet响应对象
   * @param appId 应用ID，URL路径参数
   * @param appAttemptId 应用尝试ID，URL路径参数
   * @param containerId 容器ID，URL路径参数
   * @return 指定容器的信息对象
   */
  ContainerInfo getContainer(HttpServletRequest req, HttpServletResponse res,
      String appId, String appAttemptId, String containerId);

  /**
   * 设置Router客户端RM服务实例
   *
   * @param routerClientRMService Router客户端RM服务实例
   */
  void setRouterClientRMService(RouterClientRMService routerClientRMService);

  /**
   * 获取Router客户端RM服务实例
   *
   * @return Router客户端RM服务实例
   */
  RouterClientRMService getRouterClientRMService();
}