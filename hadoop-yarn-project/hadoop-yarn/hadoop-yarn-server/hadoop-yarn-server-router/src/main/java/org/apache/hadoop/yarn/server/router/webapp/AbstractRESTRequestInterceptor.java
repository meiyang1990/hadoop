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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;

import org.apache.hadoop.yarn.server.router.RouterServerUtil;

/**
 * YARN Router REST请求拦截器抽象基类，提供责任链模式的基础实现，
 * 具体拦截器可以继承此类扩展，复用通用的链路处理逻辑。
 */
public abstract class AbstractRESTRequestInterceptor
    implements RESTRequestInterceptor {

  // Hadoop配置对象
  private Configuration conf;
  // 责任链中下一个拦截器
  private RESTRequestInterceptor nextInterceptor;
  // 当前请求操作用户信息
  private UserGroupInformation user = null;
  // Router客户端RM服务引用
  private RouterClientRMService routerClientRMService = null;

  /**
   * 设置责任链中的下一个拦截器。
   */
  @Override
  public void setNextInterceptor(RESTRequestInterceptor nextInterceptor) {
    this.nextInterceptor = nextInterceptor;
  }

  /**
   * 设置配置对象，并传递给下一个拦截器。
   */

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (this.nextInterceptor != null) {
      this.nextInterceptor.setConf(conf);
    }
  }

  /**
   * 获取当前配置对象。
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * 初始化拦截器，根据用户名创建用户信息，并初始化下一个拦截器。
   */
  @Override
  public void init(String userName) {
    this.user = RouterServerUtil.setupUser(userName);
    if (this.nextInterceptor != null) {
      this.nextInterceptor.init(userName);
    }
  }

  /**
   * 关闭拦截器，级联关闭责任链上后续拦截器。
   */
  @Override
  public void shutdown() {
    if (this.nextInterceptor != null) {
      this.nextInterceptor.shutdown();
    }
  }

  /**
   * 获取责任链中的下一个拦截器。
   */
  @Override
  public RESTRequestInterceptor getNextInterceptor() {
    return this.nextInterceptor;
  }

  /**
   * 获取当前请求操作用户信息。
   */
  public UserGroupInformation getUser() {
    return user;
  }

  @Override
  public RouterClientRMService getRouterClientRMService() {
    return routerClientRMService;
  }

  @Override
  public void setRouterClientRMService(RouterClientRMService routerClientRMService) {
    this.routerClientRMService = routerClientRMService;
  }
}