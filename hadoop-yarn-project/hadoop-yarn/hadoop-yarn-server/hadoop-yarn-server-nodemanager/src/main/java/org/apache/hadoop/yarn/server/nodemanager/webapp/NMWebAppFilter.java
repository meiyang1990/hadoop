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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterConfig;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.webapp.Controller.RequestContext;
import com.google.inject.Injector;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.http.NameValuePair;

/**
 * NodeManager Web UI过滤器，处理容器日志页面的重定向逻辑。
 * 当日志聚合开启且需要获取聚合日志时，将请求重定向到独立的日志服务器。
 */
@Singleton
public class NMWebAppFilter implements Filter {

  private Injector injector;
  private Context nmContext;

  private static final long serialVersionUID = 1L;

  /**
   * 构造函数，注入依赖。
   * @param injector Guice注入器
   * @param nmContext NodeManager上下文对象
   */
  @Inject
  public NMWebAppFilter(Injector injector, Context nmContext) {
    this.injector = injector;
    this.nmContext = nmContext;
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
      FilterChain filterChain) throws IOException, ServletException {
    // 转换为HTTP请求响应对象
    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;
    // 检查是否需要重定向到日志服务器
    String redirectPath = containerLogPageRedirectPath(request);
    if (redirectPath != null) {
      // 返回重定向响应
      String redirectMsg = "Redirecting to log server" + " : " + redirectPath;
      PrintWriter out = response.getWriter();
      out.println(redirectMsg);
      response.setHeader("Location", redirectPath);
      response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
      return;
    }
    // 无需重定向，继续处理请求
    filterChain.doFilter(request, response);
  }

  /**
   * 计算容器日志页面需要重定向到日志服务器的目标路径。
   * 满足条件则返回重定向路径，否则返回null。
   * @param request HTTP请求对象
   * @return 重定向路径或null
   */
  private String containerLogPageRedirectPath(HttpServletRequest request) {
    // 对请求URI进行HTML转义，避免XSS攻击
    String uri = HtmlQuoting.quoteHtmlChars(request.getRequestURI());
    String redirectPath = null;
    // 仅处理非REST API的容器日志请求
    if (!uri.contains("/ws/v1/node") && uri.contains("/containerlogs")) {
      // 拆分URI路径获取参数
      String[] parts = uri.split("/");
      String containerIdStr = parts[3];
      String appOwner = parts[4];
      String logType = null;
      if (parts.length > 5) {
        logType = parts[5];
      }
      if (containerIdStr != null && !containerIdStr.isEmpty()) {
        ContainerId containerId;
        try {
          // 解析容器ID
          containerId = ContainerId.fromString(containerIdStr);
        } catch (IllegalArgumentException ex) {
          // 容器ID非法，不重定向
          return redirectPath;
        }
        // 获取对应的应用ID
        ApplicationId appId =
            containerId.getApplicationAttemptId().getApplicationId();
        // 从NodeManager上下文获取应用信息
        Application app = nmContext.getApplications().get(appId);

        boolean fetchAggregatedLog = false;
        // 获取URL编码后的查询参数
        List<NameValuePair> params = WebAppUtils.getURLEncodedQueryParam(request);
        if (params != null) {
          // 检查是否请求获取远程聚合日志
          for (NameValuePair param : params) {
            if (param.getName().equals(ContainerLogsPage
                .LOG_AGGREGATION_TYPE)) {
              if (param.getValue().equals(ContainerLogsPage
                  .LOG_AGGREGATION_REMOTE_TYPE)) {
                fetchAggregatedLog = true;
              }
            }
          }
        }

        // 获取NodeManager配置
        Configuration nmConf = nmContext.getLocalDirsHandler().getConfig();
        // 判断是否需要重定向：应用不存在（已清理）或明确请求聚合日志，且日志聚合已开启
        if ((app == null || fetchAggregatedLog)
            && nmConf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
              YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
          // 获取日志服务器地址配置
          String logServerUrl =
              nmConf.get(YarnConfiguration.YARN_LOG_SERVER_URL);
          if (logServerUrl != null && !logServerUrl.isEmpty()) {
            // 构造日志服务器的目标URL路径
            StringBuilder sb = new StringBuilder();
            sb.append(logServerUrl);
            sb.append("/");
            sb.append(nmContext.getNodeId().toString());
            sb.append("/");
            sb.append(containerIdStr);
            sb.append("/");
            sb.append(containerIdStr);
            sb.append("/");
            sb.append(appOwner);
            if (logType != null && !logType.isEmpty()) {
              sb.append("/");
              sb.append(logType);
            }
            // 追加原请求的查询参数
            redirectPath =
                WebAppUtils.appendQueryParams(request, sb.toString());
          } else {
            // 未配置日志服务器地址，标记不进行重定向
            injector.getInstance(RequestContext.class).set(
              ContainerLogsPage.REDIRECT_URL, "false");
          }
        }
      }
    }
    return redirectPath;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void destroy() {
  }
}