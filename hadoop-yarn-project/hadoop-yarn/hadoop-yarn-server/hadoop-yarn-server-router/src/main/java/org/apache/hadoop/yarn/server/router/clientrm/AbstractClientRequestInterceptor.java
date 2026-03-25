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

package org.apache.hadoop.yarn.server.router.clientrm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN Router客户端请求拦截器抽象基类，实现ClientRequestInterceptor接口，提供责任链模式下拦截器的通用基础功能，
 * 具体拦截器可继承此类复用通用逻辑，专注实现自身业务拦截逻辑。
 */
public abstract class AbstractClientRequestInterceptor
    implements ClientRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractClientRequestInterceptor.class);

  private Configuration conf;
  private ClientRequestInterceptor nextInterceptor;

  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected UserGroupInformation user = null;

  private RouterDelegationTokenSecretManager tokenSecretManager = null;

  /**
   * 设置责任链中的下一个拦截器。
   */
  @Override
  public void setNextInterceptor(ClientRequestInterceptor nextInterceptor) {
    this.nextInterceptor = nextInterceptor;
  }

  /**
   * 设置拦截器配置对象，并传递给下一个拦截器。
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
   * 初始化拦截器，根据用户名创建用户凭证，并传递给下一个拦截器。
   */
  @Override
  public void init(String userName) {
    this.user = RouterServerUtil.setupUser(userName);
    if (this.nextInterceptor != null) {
      this.nextInterceptor.init(userName);
    }
  }

  /**
   * 关闭拦截器，传递关闭请求给下一个拦截器。
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
  public ClientRequestInterceptor getNextInterceptor() {
    return this.nextInterceptor;
  }

  /**
   * 获取Router代理令牌密钥管理器实例。
   */
  @Override
  public RouterDelegationTokenSecretManager getTokenSecretManager() {
    return tokenSecretManager;
  }

  /**
   * 设置Router代理令牌密钥管理器实例。
   */
  @Override
  public void setTokenSecretManager(RouterDelegationTokenSecretManager tokenSecretManager) {
    this.tokenSecretManager = tokenSecretManager;
  }
}