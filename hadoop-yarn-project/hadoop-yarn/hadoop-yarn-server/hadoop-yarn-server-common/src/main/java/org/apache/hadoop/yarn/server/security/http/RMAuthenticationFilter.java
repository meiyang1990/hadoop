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

/**
 * YARN ResourceManager HTTP认证过滤器，扩展DelegationToken认证，兼容旧版协议头
 * 用于对RM的Web服务请求进行委托令牌认证
 */
package org.apache.hadoop.yarn.server.security.http;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;

@Private
@Unstable
public class RMAuthenticationFilter extends
    DelegationTokenAuthenticationFilter {

  // 委托令牌密钥管理器实例，由RM设置
  static private AbstractDelegationTokenSecretManager<?> manager;
  // 旧版YARN认证委托令牌协议头名称，兼容旧客户端
  private static final String OLD_HEADER = "Hadoop-YARN-Auth-Delegation-Token";

  /**
   * 默认构造函数
   */
  public RMAuthenticationFilter() {
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // 将全局密钥管理器放入Servlet上下文，供父过滤器使用
    filterConfig.getServletContext().setAttribute(
      DelegationTokenAuthenticationFilter.DELEGATION_TOKEN_SECRET_MANAGER_ATTR,
      manager);
    super.init(filterConfig);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws IOException, ServletException {
    // 转换为HTTP请求
    HttpServletRequest req = (HttpServletRequest) request;
    // 获取新版标准委托令牌协议头
    String newHeader =
        req.getHeader(DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER);
    // 新版协议头不存在时，兼容旧版协议头
    if (newHeader == null || newHeader.isEmpty()) {
      // 向后兼容：仅当新版头不存在时，才允许使用旧版头字段
      final String oldHeader = req.getHeader(OLD_HEADER);
      if (oldHeader != null && !oldHeader.isEmpty()) {
        // 包装请求，将旧版头的值映射到新版头字段
        request = new HttpServletRequestWrapper(req) {
          @Override
          public String getHeader(String name) {
            if (name
                .equals(DelegationTokenAuthenticator.DELEGATION_TOKEN_HEADER)) {
              return oldHeader;
            }
            return super.getHeader(name);
          }
        };
      }
    }
    // 调用父过滤器继续认证流程
    super.doFilter(request, response, filterChain);
  }

  /**
   * 设置全局委托令牌密钥管理器，供过滤器使用
   * @param manager 密钥管理器实例
   */
  public static void setDelegationTokenSecretManager(
      AbstractDelegationTokenSecretManager<?> manager) {
    RMAuthenticationFilter.manager = manager;
  }
}