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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.server.webproxy.ProxyUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * YARN Web 代理应用Master访问IP过滤器，用于验证请求来源是否合法，并注入用户身份信息。
 * 核心作用是只允许来自代理服务器的请求转发请求，同时从Cookie中提取用户信息设置到请求中，
 * 保障YARN HA场景下的代理访问安全。
 */
@Public
public class AmIpFilter implements Filter {
  private static final Logger LOG = LoggerFactory.getLogger(AmIpFilter.class);
  
  @Deprecated
  public static final String PROXY_HOST = "PROXY_HOST";
  @Deprecated
  public static final String PROXY_URI_BASE = "PROXY_URI_BASE";
  public static final String PROXY_HOSTS = "PROXY_HOSTS";
  public static final String PROXY_HOSTS_DELIMITER = ",";
  public static final String PROXY_URI_BASES = "PROXY_URI_BASES";
  public static final String PROXY_URI_BASES_DELIMITER = ",";
  private static final String PROXY_PATH = "/proxy";
  // 代理IP列表更新间隔，默认每5分钟更新一次
  private static long updateInterval = TimeUnit.MINUTES.toMillis(5);

  // 允许转发请求的代理主机名数组
  private String[] proxyHosts;
  // 允许访问的代理IP地址集合，缓存解析后的结果
  private Set<String> proxyAddresses = null;
  // 上次更新代理IP列表的时间戳
  private long lastUpdate;
  @VisibleForTesting
  // 代理URI基础路径映射：key为主机端口，value为完整URI基础路径
  Map<String, String> proxyUriBases;
  // RM HA模式下的RM地址数组
  String rmUrls[] = null;

  @Override
  public void init(FilterConfig conf) throws ServletException {
    // 向后兼容旧版单代理配置
    if (conf.getInitParameter(PROXY_HOST) != null
        && conf.getInitParameter(PROXY_URI_BASE) != null) {
      proxyHosts = new String[]{conf.getInitParameter(PROXY_HOST)};
      proxyUriBases = new HashMap<>(1);
      proxyUriBases.put("dummy", conf.getInitParameter(PROXY_URI_BASE));
    } else {
      // 解析多代理主机配置
      proxyHosts = conf.getInitParameter(PROXY_HOSTS)
          .split(PROXY_HOSTS_DELIMITER);

      // 解析多代理URI基础路径配置
      String[] proxyUriBasesArr = conf.getInitParameter(PROXY_URI_BASES)
          .split(PROXY_URI_BASES_DELIMITER);
      proxyUriBases = new HashMap<>(proxyUriBasesArr.length);
      for (String proxyUriBase : proxyUriBasesArr) {
        try {
          URL url = new URL(proxyUriBase);
          // 按主机端口作为key存储URI基础路径
          proxyUriBases.put(url.getHost() + ":" + url.getPort(), proxyUriBase);
        } catch(MalformedURLException e) {
          LOG.warn("{} does not appear to be a valid URL", proxyUriBase, e);
        }
      }
    }

    // 解析RM HA配置
    if (conf.getInitParameter(AmFilterInitializer.RM_HA_URLS) != null) {
      rmUrls = conf.getInitParameter(AmFilterInitializer.RM_HA_URLS).split(",");
    }
  }

  /**
   * 获取允许访问的代理IP地址集合，定期自动更新解析结果。
   * @return 合法代理IP地址集合
   * @throws ServletException 如果所有代理主机都解析失败
   */
  protected Set<String> getProxyAddresses() throws ServletException {
    long now = Time.monotonicNow();
    synchronized(this) {
      // 如果缓存为空或已过期，重新解析更新
      if (proxyAddresses == null || (lastUpdate + updateInterval) <= now) {
        proxyAddresses = new HashSet<>();
        for (String proxyHost : proxyHosts) {
          try {
            // DNS解析代理主机所有IP地址
            for (InetAddress add : InetAddress.getAllByName(proxyHost)) {
              LOG.debug("proxy address is: {}", add.getHostAddress());
              proxyAddresses.add(add.getHostAddress());
            }
            lastUpdate = now;
          } catch (UnknownHostException e) {
            LOG.warn("Could not locate {} - skipping", proxyHost, e);
          }
        }
        if (proxyAddresses.isEmpty()) {
          throw new ServletException("Could not locate any of the proxy hosts");
        }
      }
      return proxyAddresses;
    }
  }

  @Override
  public void destroy() {
    //Empty
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse resp,
      FilterChain chain) throws IOException, ServletException {
    // 拒绝非HTTP请求
    ProxyUtils.rejectNonHttpRequests(req);

    HttpServletRequest httpReq = (HttpServletRequest)req;
    HttpServletResponse httpResp = (HttpServletResponse)resp;

    String method = httpReq.getMethod();
    // 禁止TRACE/TRACK方法，防止跨站脚本攻击
    if (method != null && (method.equalsIgnoreCase("TRACE") ||
        method.equalsIgnoreCase("TRACK"))) {
      httpResp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
      return;
    }

    LOG.debug("Remote address for request is: {}", httpReq.getRemoteAddr());

    // 请求来源IP不在合法代理IP列表，重定向到代理服务器
    if (!getProxyAddresses().contains(httpReq.getRemoteAddr())) {
      StringBuilder redirect = new StringBuilder(findRedirectUrl());

      redirect.append(httpReq.getRequestURI());

      int insertPoint = redirect.indexOf(PROXY_PATH);

      if (insertPoint >= 0) {
        // 在路径中插入/redirect标识，告知RM这是一次重定向请求
        insertPoint += PROXY_PATH.length();
        redirect.insert(insertPoint, "/redirect");
      }
      // 追加原始查询参数到重定向URL
      String queryString = httpReq.getQueryString();
      if (queryString != null && !queryString.isEmpty()) {
        redirect.append("?");
        redirect.append(queryString);
      }

      ProxyUtils.sendRedirect(httpReq, httpResp, redirect.toString());
    } else {
      String user = null;

      // 从Cookie中提取代理用户信息
      if (httpReq.getCookies() != null) {
        for(Cookie c: httpReq.getCookies()) {
          if(WebAppProxyServlet.PROXY_USER_COOKIE_NAME.equals(c.getName())){
            user = c.getValue();
            break;
          }
        }
      }
      // 未找到用户Cookie，直接放行，不设置用户身份
      if (user == null) {
        LOG.debug("Could not find {} cookie, so user will not be set",
            WebAppProxyServlet.PROXY_USER_COOKIE_NAME);

        chain.doFilter(req, resp);
      } else {
        // 包装请求，注入用户身份Principal，供后续权限校验使用
        AmIpPrincipal principal = new AmIpPrincipal(user);
        ServletRequest requestWrapper = new AmIpServletRequestWrapper(httpReq,
            principal);

        chain.doFilter(requestWrapper, resp);
      }
    }
  }

  /**
   * 查找可用的重定向代理URL，支持RM HA场景自动选可用RM。
   * @return 可用的代理URL基础路径
   * @throws ServletException 找不到可用代理时抛出异常
   */
  @VisibleForTesting
  public String findRedirectUrl() throws ServletException {
    String addr = null;
    if (proxyUriBases.size() == 1) {
      // 非HA场景或外部代理，直接返回唯一配置
      addr = proxyUriBases.values().iterator().next();
    } else if (rmUrls != null) {
      // RM HA场景遍历RM地址，找一个可用的代理URL
      for (String url : rmUrls) {
        String host = proxyUriBases.get(url);
        if (isValidUrl(host)) {
          addr = host;
          break;
        }
      }
    }

    if (addr == null) {
      throw new ServletException(
          "Could not determine the proxy server for redirection");
    }
    return addr;
  }

  /**
   * 验证指定URL是否可用，通过发送连接请求判断服务是否存活。
   * @param url 待验证的代理URL
   * @return 该URL是否可用
   */
  @VisibleForTesting
  public boolean isValidUrl(String url) {
    boolean isValid = false;
    try {
      // 打开连接测试可用性
      HttpURLConnection conn = (HttpURLConnection) new URL(url)
          .openConnection();
      conn.connect();
      // 返回200 OK则认为可用
      isValid = conn.getResponseCode() == HttpURLConnection.HTTP_OK;
      // 开启安全认证场景下，401未授权/403禁止访问也认为服务可用，只是当前没有凭证，不影响可用性判断
      if (!isValid && UserGroupInformation.isSecurityEnabled()) {
        isValid = (conn
            .getResponseCode() == HttpURLConnection.HTTP_UNAUTHORIZED)
            || (conn.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN);
        return isValid;
      }
    } catch (Exception e) {
      LOG.warn("Failed to connect to " + url + ": " + e.toString());
    }
    return isValid;
  }

  /**
   * 设置代理IP列表更新间隔，仅用于单元测试。
   * @param updateInterval 新的更新间隔（毫秒）
   */
  @VisibleForTesting
  protected static void setUpdateInterval(long updateInterval) {
    AmIpFilter.updateInterval = updateInterval;
  }
}