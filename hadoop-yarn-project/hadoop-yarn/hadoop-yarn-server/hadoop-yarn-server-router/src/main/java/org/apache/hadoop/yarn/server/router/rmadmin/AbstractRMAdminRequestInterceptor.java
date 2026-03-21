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

package org.apache.hadoop.yarn.server.router.rmadmin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;

/**
 * YARN Router资源管理器管理请求拦截器抽象基类，实现了RMAdminRequestInterceptor接口，
 * 提供责任链模式的基础公共能力，可被具体拦截器扩展实现。
 * 属于YARN Router服务端，处理联邦场景下跨RM的RMAdmin请求拦截。
 *
 */
public abstract class AbstractRMAdminRequestInterceptor
    implements RMAdminRequestInterceptor {
  private Configuration conf;
  private RMAdminRequestInterceptor nextInterceptor;

  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected UserGroupInformation user = null;

  /**
   * 设置责任链中的下一个拦截器。
   */
  @Override
  public void setNextInterceptor(RMAdminRequestInterceptor nextInterceptor) {
    this.nextInterceptor = nextInterceptor;
  }

  /**
   * 设置配置对象，并将配置传递给责任链中下一个拦截器。
   */

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (this.nextInterceptor != null) {
      this.nextInterceptor.setConf(conf);
    }
  }

  /**
   * 获取当前拦截器的配置对象。
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * 初始化拦截器，根据用户名创建用户凭证，并初始化责任链中下一个拦截器。
   */
  @Override
  public void init(String userName) {
    this.user = RouterServerUtil.setupUser(userName);
    if (this.nextInterceptor != null) {
      this.nextInterceptor.init(userName);
    }
  }

  /**
   * 关闭拦截器，关闭责任链中下一个拦截器，释放资源。
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
  public RMAdminRequestInterceptor getNextInterceptor() {
    return this.nextInterceptor;
  }

}