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

package org.apache.hadoop.yarn.server.timeline.security;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

/**
 * 时间线服务认证过滤器，为ATSv1和ATSv2提供代理令牌认证支持
 * 用于对时间线服务的REST API请求进行身份认证验证
 */
@Private
@Unstable
public class TimelineAuthenticationFilter
    extends DelegationTokenAuthenticationFilter {

  // 时间线服务代理令牌密钥管理器实例
  private static AbstractDelegationTokenSecretManager
      <TimelineDelegationTokenIdentifier> secretManager;

  /**
   * 初始化过滤器，注入时间线服务的代理令牌密钥管理器
   */
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // 将密钥管理器存入Servlet上下文供父过滤器使用
    filterConfig.getServletContext().setAttribute(
        DelegationTokenAuthenticationFilter.
            DELEGATION_TOKEN_SECRET_MANAGER_ATTR, secretManager);
    super.init(filterConfig);
  }

  /**
   * 设置时间线服务代理令牌密钥管理器
   * @param secretMgr 时间线代理令牌密钥管理器实例
   */
  public static void setTimelineDelegationTokenSecretManager(
      AbstractDelegationTokenSecretManager
          <TimelineDelegationTokenIdentifier> secretMgr) {
    TimelineAuthenticationFilter.secretManager = secretMgr;
  }
}