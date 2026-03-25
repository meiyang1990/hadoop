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

package org.apache.hadoop.yarn.server.webproxy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriBuilderException;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher.AppReportSource;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher.FetchedAppReport;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.util.TrackingUriPlugin;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN WebApp代理Servlet，负责代理用户访问YARN集群中运行的Application Master Web UI。
 * 核心职责：实现安全访问控制、HTTPS合规检查、请求转发和重定向处理，解决跨域访问和安全风险问题。
 */
public class WebAppProxyServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(
      WebAppProxyServlet.class);
  private static final String REDIRECT = "/redirect";
  /** 需要透传给后端Application Master的请求头列表 */
  private static final Set<String> PASS_THROUGH_HEADERS =
    new HashSet<>(Arrays.asList(
        "User-Agent",
        "Accept",
        "Accept-Encoding",
        "Accept-Language",
        "Accept-Charset",
        "Content-Type",
        "Origin",
        "Access-Control-Request-Method",
        "Access-Control-Request-Headers"));

  public static final String PROXY_USER_COOKIE_NAME = "proxy-user";

  /** 跟踪URI生成插件列表，用于自定义应用跟踪URL生成 */
  private transient List<TrackingUriPlugin> trackingUriPlugins;
  /** 错误页面基础URL路径 */
  private final String failurePageUrlBase;
  /** YARN配置对象 */
  private transient YarnConfiguration conf;

  /**
   * HTTP请求方法枚举。
   */
  private enum HTTP { GET, POST, HEAD, PUT, DELETE }

  /**
   * Empty Hamlet class.
   */
  private static class __ implements Hamlet.__ {
    //Empty
  }
  
  /** HTML页面构建器，基于Hamlet模板框架生成警告/错误页面 */
  private static class Page extends Hamlet {
    Page(PrintWriter out) {
      super(out, 0, false);
    }
  
    public HTML<WebAppProxyServlet.__> html() {
      return new HTML<>("html", null, EnumSet.of(EOpt.ENDTAG));
    }
  }

  protected void setConf(YarnConfiguration conf){
    this.conf = conf;
  }
  /**
   * 默认构造函数，初始化代理Servlet配置和插件
   */
  public WebAppProxyServlet() {
    super();
    conf = new YarnConfiguration();
    this.trackingUriPlugins =
        conf.getInstances(YarnConfiguration.YARN_TRACKING_URL_GENERATOR,
            TrackingUriPlugin.class);
    this.failurePageUrlBase =
        StringHelper.pjoin(WebAppUtils.getResolvedRMWebAppURLWithScheme(conf),
          "cluster", "failure");
  }

  /** 获取ResourceManager上应用页面的基础URL */
  private String getRmAppPageUrlBase(ApplicationId id) throws YarnException, IOException {
    ServletContext context = getServletContext();
    AppReportFetcher af = (AppReportFetcher) context.getAttribute(WebAppProxy.FETCHER_ATTRIBUTE);
    return af.getRmAppPageUrlBase(id);
  }

  /** 获取应用历史服务器上应用页面的基础URL */
  private String getAhsAppPageUrlBase() {
    ServletContext context = getServletContext();
    AppReportFetcher af = (AppReportFetcher) context.getAttribute(WebAppProxy.FETCHER_ATTRIBUTE);
    return af.getAhsAppPageUrlBase();
  }

  /**
   * 返回404错误响应并包含自定义错误信息
   * @param resp HTTP响应
   * @param message 错误信息
   * @throws IOException IO异常
   */
  private static void notFound(HttpServletResponse resp, String message) 
    throws IOException {
    ProxyUtils.notFound(resp, message);
  }
  
  /**
   * 显示安全警告页面，提示用户访问第三方应用UI存在风险
   * @param resp HTTP响应
   * @param link 目标链接
   * @param user 应用运行用户
   * @param id 应用ID
   * @throws IOException IO异常
   */
  private static void warnUserPage(HttpServletResponse resp, String link, 
      String user, ApplicationId id) throws IOException {
    //Set the cookie when we warn which overrides the query parameter
    //This is so that if a user passes in the approved query parameter without
    //having first visited this page then this page will still be displayed 
    resp.addCookie(makeCheckCookie(id, false));
    resp.setContentType(MimeType.HTML);
    Page p = new Page(resp.getWriter());
    p.html().
      h1("WARNING: The following page may not be safe!").
      h3().
        __("click ").a(link, "here").
        __(" to continue to an Application Master web interface owned by ", user).
        __().
        __();
  }

  /**
   * 检查HTTPS严格模式合规性，若要求HTTPS但目标不是HTTPS则返回错误页面
   * @param resp HTTP响应
   * @param link 目标URI
   * @param conf YARN配置
   * @return true表示不符合要求已返回错误页面，false表示合规
   * @throws IOException IO异常
   */
  @VisibleForTesting
  static boolean checkHttpsStrictAndNotProvided(
      HttpServletResponse resp, URI link, YarnConfiguration conf)
      throws IOException {
    String httpsPolicy = conf.get(
        YarnConfiguration.RM_APPLICATION_HTTPS_POLICY,
        YarnConfiguration.DEFAULT_RM_APPLICATION_HTTPS_POLICY);
    boolean required = httpsPolicy.equals("STRICT");
    boolean provided = link.getScheme().equals("https");
    if (required && !provided) {
      resp.setContentType(MimeType.HTML);
      Page p = new Page(resp.getWriter());
      p.html().
          h1("HTTPS must be used").
          h3().
          __(YarnConfiguration.RM_APPLICATION_HTTPS_POLICY,
              "is set to STRICT, which means that the tracking URL ",
              "must be an HTTPS URL, but it is not.").
          __("The tracking URL is: ", link).
          __().
          __();
      return true;
    }
    return false;
  }
  
  /**
   * 代理请求到目标Application Master UI，并将响应返回给客户端
   * @param req 客户端请求
   * @param resp 客户端响应
   * @param link 目标应用URI
   * @param c 确认Cookie，用于记录用户已确认安全风险
   * @param proxyHost 代理主机地址
   * @param method HTTP方法
   * @param appId 应用ID
   * @throws IOException IO异常
   */
  private void proxyLink(final HttpServletRequest req,
      final HttpServletResponse resp, final URI link, final Cookie c,
      final String proxyHost, final HTTP method, final ApplicationId appId)
      throws IOException {
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

    String httpsPolicy = conf.get(YarnConfiguration.RM_APPLICATION_HTTPS_POLICY,
        YarnConfiguration.DEFAULT_RM_APPLICATION_HTTPS_POLICY);

    boolean connectionTimeoutEnabled =
        conf.getBoolean(YarnConfiguration.RM_PROXY_TIMEOUT_ENABLED,
            YarnConfiguration.DEFALUT_RM_PROXY_TIMEOUT_ENABLED);
    int connectionTimeout =
        conf.getInt(YarnConfiguration.RM_PROXY_CONNECTION_TIMEOUT,
            YarnConfiguration.DEFAULT_RM_PROXY_CONNECTION_TIMEOUT);

    // 宽松/严格HTTPS模式下，配置SSL上下文用于验证应用自签名证书
    if (httpsPolicy.equals("LENIENT") || httpsPolicy.equals("STRICT")) {
      ProxyCA proxyCA = getProxyCA();
      // ProxyCA could be null when the Proxy is run outside the RM
      if (proxyCA != null) {
        try {
          httpClientBuilder.setSSLContext(proxyCA.createSSLContext(appId));
          httpClientBuilder.setSSLHostnameVerifier(
              proxyCA.getHostnameVerifier());
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }

    // Make sure we send the request from the proxy address in the config
    // since that is what the AM filter checks against. IP aliasing or
    // similar could cause issues otherwise.
    InetAddress localAddress = InetAddress.getByName(proxyHost);
    LOG.debug("local InetAddress for proxy host: {}", localAddress);
    // 配置请求默认参数：绑定代理出口地址、允许循环重定向、配置超时时间
    httpClientBuilder.setDefaultRequestConfig(
        connectionTimeoutEnabled ?
            RequestConfig.custom()
                .setCircularRedirectsAllowed(true)
                .setLocalAddress(localAddress)
                .setConnectionRequestTimeout(connectionTimeout)
                .setSocketTimeout(connectionTimeout)
                .setConnectTimeout(connectionTimeout)
                .build() :
            RequestConfig.custom()
                .setCircularRedirectsAllowed(true)
                .setLocalAddress(localAddress)
                .build());

    HttpRequestBase base = null;
    // 根据HTTP方法创建对应请求对象
    if (method.equals(HTTP.GET)) {
      base = new HttpGet(link);
    } else if (method.equals(HTTP.PUT)) {
      base = new HttpPut(link);

      // 读取PUT请求体，转发给后端应用
      StringBuilder sb = new StringBuilder();
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(req.getInputStream(), StandardCharsets.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }

      ((HttpPut) base).setEntity(new StringEntity(sb.toString()));
    } else {
      // 不支持的方法返回405
      resp.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
      return;
    }

    @SuppressWarnings("unchecked")
    Enumeration<String> names = req.getHeaderNames();
    // 透传允许的请求头到后端应用
    while (names.hasMoreElements()) {
      String name = names.nextElement();
      if (PASS_THROUGH_HEADERS.contains(name)) {
        String value = req.getHeader(name);
        LOG.debug("REQ HEADER: {} : {}", name, value);
        base.setHeader(name, value);
      }
    }

    // 如果有远程用户信息，通过Cookie透传给后端应用
    String user = req.getRemoteUser();
    if (user != null && !user.isEmpty()) {
      base.setHeader("Cookie",
          PROXY_USER_COOKIE_NAME + "=" + URLEncoder.encode(user, "ASCII"));
    }
    OutputStream out = resp.getOutputStream();
    HttpClient client = httpClientBuilder.build();
    try {
      // 执行请求，复制响应头和内容到客户端响应
      HttpResponse httpResp = client.execute(base);
      resp.setStatus(httpResp.getStatusLine().getStatusCode());
      for (Header header : httpResp.getAllHeaders()) {
        resp.setHeader(header.getName(), header.getValue());
      }
      if (c != null) {
        resp.addCookie(c);
      }
      InputStream in = httpResp.getEntity().getContent();
      if (in != null) {
        IOUtils.copyBytes(in, out, 4096, true);
      }
    } finally {
      base.releaseConnection();
    }
  }
  
  private static String getCheckCookieName(ApplicationId id){
    return "checked_"+id;
  }
  
  private static Cookie makeCheckCookie(ApplicationId id, boolean isSet) {
    Cookie c = new Cookie(getCheckCookieName(id),String.valueOf(isSet));
    c.setHttpOnly(true);
    c.setPath(ProxyUriUtils.getPath(id));
    c.setMaxAge(60 * 60 * 2); //2 hours in seconds
    return c;
  }
  
  /** 检查是否开启安全认证模式 */
  private boolean isSecurityEnabled() {
    Boolean b = (Boolean) getServletContext()
        .getAttribute(WebAppProxy.IS_SECURITY_ENABLED_ATTRIBUTE);
    return b != null ? b : false;
  }
  
  /** 从ServletContext获取应用报告获取器，查询应用报告 */
  private FetchedAppReport getApplicationReport(ApplicationId id)
      throws IOException, YarnException {
    return ((AppReportFetcher) getServletContext()
        .getAttribute(WebAppProxy.FETCHER_ATTRIBUTE)).getApplicationReport(id);
  }

  /** 从ServletContext获取ProxyCA证书管理器 */
  private ProxyCA getProxyCA() {
    return ((ProxyCA) getServletContext().getAttribute(WebAppProxy.PROXY_CA));
  }
  
  /** 从ServletContext获取代理主机地址 */
  private String getProxyHost() throws IOException {
    return ((String) getServletContext()
        .getAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE));
  }
  
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    methodAction(req, resp, HTTP.GET);
  }

  @Override
  protected final void doPut(final HttpServletRequest req,
      final HttpServletResponse resp) throws ServletException, IOException {
    methodAction(req, resp, HTTP.PUT);
  }

  /**
   * 统一处理各类HTTP方法的代理请求，核心业务逻辑入口
   * @param req HTTP请求
   * @param resp HTTP响应
   * @param method HTTP方法
   * @throws ServletException Servlet异常
   * @throws IOException IO异常
   */
  private void methodAction(final HttpServletRequest req,
      final HttpServletResponse resp,
      final HTTP method) throws ServletException, IOException {
    try {
      String userApprovedParamS = 
        req.getParameter(ProxyUriUtils.PROXY_APPROVAL_PARAM);
      boolean userWasWarned = false;
      boolean userApproved = Boolean.parseBoolean(userApprovedParamS);
      boolean securityEnabled = isSecurityEnabled();
      boolean isRedirect = false;
      String pathInfo = req.getPathInfo();
      final String remoteUser = req.getRemoteUser();

      String[] parts = null;

      if (pathInfo != null) {
        // 如果是重定向路径，去掉重定向前缀后解析路径
        if (pathInfo.startsWith(REDIRECT)) {
          pathInfo = pathInfo.substring(REDIRECT.length());
          isRedirect = true;
        }

        parts = pathInfo.split("/", 3);
      }

      // 路径格式不正确，返回404
      if ((parts == null) || (parts.length < 2)) {
        LOG.warn("{} gave an invalid proxy path {}", remoteUser,  pathInfo);
        notFound(resp, "Your path appears to be formatted incorrectly.");
        return