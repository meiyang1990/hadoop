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

package org.apache.hadoop.yarn.server.timelineservice.reader.security;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderWebServicesUtils;

/**
 * 时间线服务V2读取权限白名单认证过滤器，用于检查用户是否有权限读取ATSv2数据。
 */

public class TimelineReaderWhitelistAuthorizationFilter implements Filter {

  public static final String EMPTY_STRING = "";

  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineReaderWhitelistAuthorizationFilter.class);

  // 是否开启白名单读权限认证
  private boolean isWhitelistReadAuthEnabled = false;

  // 允许读取时间线数据的用户访问控制列表
  private AccessControlList allowedUsersAclList;
  // YARN管理员访问控制列表，管理员默认拥有读取权限
  private AccessControlList adminAclList;

  @Override
  public void destroy() {
    // NOTHING
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    // 转换为HTTP请求响应对象
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;

    // 如果开启了白名单认证则执行权限检查
    if (isWhitelistReadAuthEnabled) {
      // 从请求中获取当前用户信息
      UserGroupInformation callerUGI = TimelineReaderWebServicesUtils
          .getUser(httpRequest);
      // 未获取到用户信息，说明未认证，抛出异常
      if (callerUGI == null) {
        String msg = "Unable to obtain user name, user not authenticated";
        throw new AuthorizationException(msg);
      }
      // 检查用户是否在管理员或白名单用户列表中，都不在则拒绝访问
      if (!(adminAclList.isUserAllowed(callerUGI)
          || allowedUsersAclList.isUserAllowed(callerUGI))) {
        String userName = callerUGI.getShortUserName();
        String msg = "User " + userName
            + " is not allowed to read TimelineService V2 data.";
        // 返回403禁止访问错误
        httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, msg);
        return;
      }
    }
    // 权限检查通过，继续执行后续过滤器链
    if (chain != null) {
      chain.doFilter(request, response);
    }
  }

  @Override
  public void init(FilterConfig conf) throws ServletException {
    // 从过滤器初始化参数读取是否开启读权限认证配置
    String isWhitelistReadAuthEnabledStr = conf
        .getInitParameter(YarnConfiguration.TIMELINE_SERVICE_READ_AUTH_ENABLED);
    // 配置未设置则使用默认值
    if (isWhitelistReadAuthEnabledStr == null) {
      isWhitelistReadAuthEnabled =
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READ_AUTH_ENABLED;
    } else {
      // 解析配置值
      isWhitelistReadAuthEnabled =
          Boolean.valueOf(isWhitelistReadAuthEnabledStr);
    }

    // 如果开启了认证，初始化访问控制列表
    if (isWhitelistReadAuthEnabled) {
      // 获取允许访问的用户配置
      String listAllowedUsers = conf.getInitParameter(
          YarnConfiguration.TIMELINE_SERVICE_READ_ALLOWED_USERS);
      // 配置为空则使用默认值
      if (StringUtils.isEmpty(listAllowedUsers)) {
        listAllowedUsers =
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READ_ALLOWED_USERS;
      }
      LOG.info("listAllowedUsers={}", listAllowedUsers);
      // 构造允许访问用户的ACL
      allowedUsersAclList = new AccessControlList(listAllowedUsers);
      LOG.info("allowedUsersAclList={}", allowedUsersAclList.getUsers());
      // 加载管理员ACL配置，管理员默认允许访问
      String adminAclListStr =
          conf.getInitParameter(YarnConfiguration.YARN_ADMIN_ACL);
      // 管理员ACL未配置则设置为空字符串
      if (StringUtils.isEmpty(adminAclListStr)) {
        adminAclListStr =
            TimelineReaderWhitelistAuthorizationFilter.EMPTY_STRING;
        LOG.info("adminAclList not set, hence setting it to \"\"");
      }
      // 构造管理员ACL
      adminAclList = new AccessControlList(adminAclListStr);
      LOG.info("adminAclList={}", adminAclList.getUsers());
    }
  }
}