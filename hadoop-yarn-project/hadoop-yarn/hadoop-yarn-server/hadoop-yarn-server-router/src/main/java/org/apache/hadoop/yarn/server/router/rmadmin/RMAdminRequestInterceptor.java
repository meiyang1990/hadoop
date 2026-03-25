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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;

/**
 * 文件功能：定义了ResourceManager管理请求拦截器需要实现的接口规范，
 * 用于YARN Router中拦截检查客户端发往ResourceManager的管理请求，
 * 支持责任链模式组织多个拦截器处理请求。
 */
public interface RMAdminRequestInterceptor
    extends ResourceManagerAdministrationProtocol, Configurable {
  /**
   * 初始化拦截器，在拦截器实例生命周期内仅会被调用一次。
   *
   * @param user 发起请求的客户端用户名
   */
  void init(String user);

  /**
   * 关闭拦截器并释放持有的资源，在拦截器管道销毁时调用。
   * 具体实现需要释放自身资源，并将关闭请求转发给下一个拦截器。
   */
  void shutdown();

  /**
   * 设置责任链中的下一个拦截器。拦截器处理完请求后需要将请求转发给下一个拦截器。
   * 责任链的最后一个拦截器直接将请求发往ResourceManager服务，不会调用此方法。
   *
   * @param nextInterceptor 责任链中的下一个请求拦截器
   */
  void setNextInterceptor(RMAdminRequestInterceptor nextInterceptor);

  /**
   * 获取责任链中的下一个拦截器。
   *
   * @return 责任链中的下一个请求拦截器
   */
  RMAdminRequestInterceptor getNextInterceptor();

}