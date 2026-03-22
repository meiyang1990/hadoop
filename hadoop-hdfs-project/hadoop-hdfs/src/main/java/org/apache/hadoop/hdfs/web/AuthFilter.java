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
package org.apache.hadoop.hdfs.web;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilter;

/**
 * WebHDFS 认证过滤器，继承自 ProxyUserAuthenticationFilter
 * 针对 WebHDFS 场景处理委托令牌认证，支持携带令牌的请求绕过Kerberos认证
 */
public class AuthFilter extends ProxyUserAuthenticationFilter {

  /**
   * 处理HTTP请求认证逻辑，检测到携带委托令牌的WebHDFS请求直接放行，走令牌认证，绕过Kerberos
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws IOException, ServletException {
    // 将请求转换为全小写路径格式的HttpServletRequest
    final HttpServletRequest httpRequest = ProxyUserAuthenticationFilter.
        toLowerCase((HttpServletRequest)request);
    // 从请求参数获取委托令牌字符串
    final String tokenString = httpRequest.getParameter(DelegationParam.NAME);
    // 如果请求携带了委托令牌，且访问路径为WebHDFS路径
    if (tokenString != null && httpRequest.getServletPath().startsWith(
        WebHdfsFileSystem.PATH_PREFIX)) {
      //请求URL已经携带委托令牌，将使用令牌认证，绕过Kerberos认证
      filterChain.doFilter(httpRequest, response);
      return;
    }
    // 没有携带令牌，走父类默认认证流程
    super.doFilter(request, response, filterChain);
  }

}