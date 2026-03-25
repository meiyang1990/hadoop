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

package org.apache.hadoop.yarn.server.webproxy.amfilter;

import java.security.Principal;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 * YARN Web代理AM过滤器请求包装器，封装已认证的用户Principal信息
 * 用于在Web代理请求中注入ApplicationMaster认证后的用户信息
 */
public class AmIpServletRequestWrapper extends HttpServletRequestWrapper {
  private final AmIpPrincipal principal;

  /**
   * 构造请求包装器，注入自定义Principal
   * @param request 原始Http请求
   * @param principal 已认证的AM请求Principal
   */
  public AmIpServletRequestWrapper(HttpServletRequest request, 
      AmIpPrincipal principal) {
    super(request);
    this.principal = principal;
  }

  @Override
  public Principal getUserPrincipal() {
    return principal;
  }

  @Override
  public String getRemoteUser() {
    return principal.getName();
  }

  @Override
  public boolean isUserInRole(String role) {
    //No role info so far
    return false;
  }

}