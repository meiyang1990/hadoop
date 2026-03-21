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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.pjoin;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.http.IsActiveServlet;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;

/**
 * RM Web 应用过滤器，负责处理高可用场景下的重定向以及已完成应用跳转到应用历史服务的逻辑
 */
@Singleton
public class RMWebAppFilter implements Filter {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMWebAppFilter.class);

  private Injector injector;
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  // 定义无需重定向的URI集合
  private static final Set<String> NON_REDIRECTED_URIS = Sets.newHashSet(
      "/conf", "/stacks", "/logLevel", "/logs", IsActiveServlet.PATH_SPEC,
      "/jmx", "/prom");
  private String path;
  private boolean ahsEnabled;
  private String ahsPageURLPrefix;
  private static final int BASIC_SLEEP_TIME = 5;
  private static final int MAX_SLEEP_TIME = 5 * 60;
  private static final Random randnum = new Random();

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  /**
   * 构造函数，初始化过滤器配置
   * @param injector Guice注入器
   * @param conf YARN配置对象
   */
  @Inject
  public RMWebAppFilter(Injector injector, Configuration conf) {
    this.injector = injector;
    // 根据HTTPS配置获取对应的RM Web服务地址
    InetSocketAddress sock = YarnConfiguration.useHttps(conf)
        ? conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
            YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS,
            YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT)
        : conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS,
            YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS,
            YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);

    // 拼接完整的RM Web服务基础URL
    path = sock.getHostName() + ":" + sock.getPort();
    path = YarnConfiguration.useHttps(conf)
        ? "https://" + path
        : "http://" + path;
    // 获取应用历史服务是否启用的配置
    ahsEnabled = conf.getBoolean(
        YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED);
    // 构建应用历史服务页面URL前缀
    ahsPageURLPrefix = pjoin(
        WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(
            conf), "applicationhistory");
  }

  @Override
  public void doFilter(ServletRequest servletRequest,
      ServletResponse servletResponse, FilterChain chain) throws IOException,
      ServletException {
    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;

    // 设置字符编码为UTF-8
    response.setCharacterEncoding("UTF-8");
    // 对请求URI进行HTML转义，防止XSS攻击
    String htmlEscapedUri = HtmlQuoting.quoteHtmlChars(request.getRequestURI());

    if (htmlEscapedUri == null) {
      htmlEscapedUri = "/";
    }

    // 拼接URI和查询参数
    String uriWithQueryString =
        WebAppUtils.appendQueryParams(request, htmlEscapedUri);
    // 获取转义后的完整URI（含查询参数）
    String htmlEscapedUriWithQueryString =
        WebAppUtils.getHtmlEscapedURIWithQueryString(request);

    // 获取RM Web应用实例
    RMWebApp rmWebApp = injector.getInstance(RMWebApp.class);
    // 检查当前RM是否为 standby 节点
    rmWebApp.checkIfStandbyRM();
    // 如果当前是 standby 节点且需要重定向，则执行重定向逻辑
    if (rmWebApp.isStandby()
        && shouldRedirect(rmWebApp, htmlEscapedUri)) {

      // 获取目标重定向地址
      String redirectPath = rmWebApp.getRedirectPath();

      if (redirectPath != null && !redirectPath.isEmpty()) {
        // 拼接请求路径到重定向地址
        redirectPath += uriWithQueryString;
        String redirectMsg = "This is standby RM. The redirect url is: "
            + htmlEscapedUriWithQueryString;
        PrintWriter out = response.getWriter();
        out.println(redirectMsg);
        // 设置重定向头和状态码
        response.setHeader("Location", redirectPath);
        response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        return;
      } else {
        // 没有可用活跃RM，准备自动重试
        boolean doRetry = true;
        String retryIntervalStr =
            request.getParameter(YarnWebParams.NEXT_REFRESH_INTERVAL);
        int retryInterval = 0;
        if (retryIntervalStr != null) {
          try {
            retryInterval = Integer.parseInt(retryIntervalStr.trim());
          } catch (NumberFormatException ex) {
            doRetry = false;
          }
        }
        // 计算下一次重试的等待时间（指数退避）
        int next = calculateExponentialTime(retryInterval);

        // 构建带重试参数的重定向URL
        String redirectUrl =
            appendOrReplaceParamter(path + uriWithQueryString,
              YarnWebParams.NEXT_REFRESH_INTERVAL + "=" + (retryInterval + 1));
        if (redirectUrl == null || next > MAX_SLEEP_TIME) {
          doRetry = false;
        }
        // 构建响应提示信息
        String redirectMsg =
            doRetry ? "Can not find any active RM. Will retry in next " + next
                + " seconds." : "There is no active RM right now.";
        redirectMsg += "\nHA Zookeeper Connection State: "
            + rmWebApp.getHAZookeeperConnectionState();
        PrintWriter out = response.getWriter();
        out.println(redirectMsg);
        if (doRetry) {
          // 设置Refresh头实现自动重试
          response.setHeader("Refresh", next + ";url=" + redirectUrl);
          response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        }
      }
      return;
    } else if (ahsEnabled) {
      // 如果启用了应用历史服务，检查是否需要跳转到AHS
      String ahsRedirectUrl = ahsRedirectPath(uriWithQueryString, rmWebApp);
      if(ahsRedirectUrl != null) {
        // 重定向到应用历史服务
        response.setHeader("Location", ahsRedirectUrl);
        response.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        return;
      }
    }

    // 放行请求，继续处理
    chain.doFilter(request, response);
  }

  /**
   * 生成应用历史服务重定向路径，当RM中不存在对应实体时返回重定向地址
   * @param uri 请求URI
   * @param rmWebApp RM Web应用实例
   * @return 重定向地址，如果不需要重定向返回null
   */
  private String ahsRedirectPath(String uri, RMWebApp rmWebApp) {
    // TODO: Commonize URL parsing code. Will be done in YARN-4642.
    String redirectPath = null;
    if(uri.contains("/cluster/")) {
      String[] parts = uri.split("/");
      if(parts.length > 3) {
        RMContext context = rmWebApp.getRMContext();
        String type = parts[2];
        ApplicationId appId = null;
        ApplicationAttemptId appAttemptId = null;
        ContainerId containerId = null;
        switch(type){
        case "app":
          try {
            // 解析应用ID
            appId = Apps.toAppID(parts[3]);
          } catch (YarnRuntimeException | NumberFormatException e) {
            LOG.debug("Error parsing {} as an ApplicationId",
                parts[3], e);
            return redirectPath;
          }
          // 如果当前RM中不存在该应用，重定向到AHS
          if(!context.getRMApps().containsKey(appId)) {
            redirectPath = pjoin(ahsPageURLPrefix, "app", appId);
          }
          break;
        case "appattempt":
          try{
            // 解析应用尝试ID
            appAttemptId = ApplicationAttemptId.fromString(parts[3]);
          } catch (IllegalArgumentException e) {
            LOG.debug("Error parsing {} as an ApplicationAttemptId",
                parts[3], e);
            return redirectPath;
          }
          // 如果当前RM中不存在该应用，重定向到AHS
          if(!context.getRMApps().containsKey(
              appAttemptId.getApplicationId())) {
            redirectPath = pjoin(ahsPageURLPrefix,
                "appattempt", appAttemptId);
          }
          break;
        case "container":
          try {
            // 解析容器ID
            containerId = ContainerId.fromString(parts[3]);
          } catch (IllegalArgumentException e) {
            LOG.debug("Error parsing {} as an ContainerId",
                parts[3], e);
            return redirectPath;
          }
          // 如果当前RM中不存在该应用，重定向到AHS
          if(!context.getRMApps().containsKey(
              containerId.getApplicationAttemptId().getApplicationId())) {
            redirectPath = pjoin(ahsPageURLPrefix,
                "container", containerId);
          }
          break;
        default:
          break;
        }
      }
    }
    return redirectPath;
  }

  /**
   * 判断当前URI是否需要重定向
   * @param rmWebApp RM Web应用实例
   * @param uri 请求URI
   * @return 是否需要重定向
   */
  private boolean shouldRedirect(RMWebApp rmWebApp, String uri) {
    return !uri.equals("/" + rmWebApp.wsName() + "/v1/cluster/info")
        && !uri.equals("/ws/v1/cluster/info")
        && !uri.equals("/" + rmWebApp.name() + "/cluster")
        && !uri.startsWith(ProxyUriUtils.PROXY_BASE)
        && !NON_REDIRECTED_URIS.contains(uri);
  }

  /**
   * 添加或替换URI中的查询参数
   * @param uri 原始URI
   * @param newQuery 新的查询参数项
   * @return 修改后的URI字符串，出错返回null
   */
  private String appendOrReplaceParamter(String uri, String newQuery) {
    if (uri.contains(YarnWebParams.NEXT_REFRESH_INTERVAL + "=")) {
      // 替换已存在的重试间隔参数
      return uri.replaceAll(YarnWebParams.NEXT_REFRESH_INTERVAL + "=[^&]+",
        newQuery);
    }
    try {
      URI oldUri = new URI(uri);
      String appendQuery = oldUri.getQuery();
      if (appendQuery == null) {
        appendQuery = newQuery;
      } else {
        appendQuery += "&" + newQuery;
      }

      // 构建新的URI对象
      URI newUri =
          new URI(oldUri.getScheme(), oldUri.getAuthority(), oldUri.getPath(),
            appendQuery, oldUri.getFragment());

      return newUri.toString();
    } catch (URISyntaxException e) {
      return null;
    }
  }

  /**
   * 使用指数退避算法计算重试等待时间，添加随机因子避免 thundering herd
   * @param retries 已重试次数
   * @return 下一次等待时间（秒）
   */
  private static int calculateExponentialTime(int retries) {
    long baseTime = BASIC_SLEEP_TIME * (1L << retries);
    return (int) (baseTime * (randnum.nextDouble() + 0.5));
  }

  @Override
  public void destroy() {
  }
}