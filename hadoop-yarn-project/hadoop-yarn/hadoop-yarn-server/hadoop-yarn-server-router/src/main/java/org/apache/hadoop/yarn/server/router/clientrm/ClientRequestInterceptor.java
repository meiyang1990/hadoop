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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenSecretManager;

/**
 * YARN Router客户端到ResourceManager请求拦截器接口，定义了拦截器需要实现的契约。
 * 用于拦截和检查客户端发送到资源管理器的请求消息，可实现自定义处理逻辑如认证、日志、限流等。
 */
public interface ClientRequestInterceptor
    extends ApplicationClientProtocol, Configurable {
  /**
   * 初始化拦截器，在拦截器实例生命周期内保证只被调用一次。
   *
   * @param user 客户端用户名
   */
  void init(String user);

  /**
   * 关闭拦截器，释放拦截器持有的资源。
   * 在应用拦截器管道销毁时调用，实现类需要释放资源并将关闭请求转发给下一个拦截器。
   */
  void shutdown();

  /**
   * 设置管道中的下一个拦截器。
   * 接口实现类在检查完请求消息后，需要将请求转发给下一个拦截器处理。
   * 链中的最后一个拦截器负责将请求发送给ResourceManager服务，因此不会收到该方法调用。
   *
   * @param nextInterceptor 管道中下一个要执行的请求拦截器
   */
  void setNextInterceptor(ClientRequestInterceptor nextInterceptor);

  /**
   * 获取拦截器链中的下一个拦截器。
   *
   * @return 拦截器链中的下一个拦截器
   */
  ClientRequestInterceptor getNextInterceptor();

  /**
   * 为当前拦截器设置路由器委托令牌密钥管理器，支持令牌相关操作，
   * 包括创建令牌、更新令牌和删除令牌。
   *
   * @param tokenSecretManager 路由器委托令牌密钥管理器
   */
  void setTokenSecretManager(RouterDelegationTokenSecretManager tokenSecretManager);

  /**
   * 获取路由器委托令牌密钥管理器。
   *
   * @return 路由器委托令牌密钥管理器
   */
  RouterDelegationTokenSecretManager getTokenSecretManager();
}