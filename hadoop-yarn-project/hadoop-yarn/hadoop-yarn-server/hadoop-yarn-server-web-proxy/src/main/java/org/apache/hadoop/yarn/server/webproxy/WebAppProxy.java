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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN Web 应用代理服务，负责代理用户访问YARN集群中运行的应用Web UI，
 * 实现跨域访问隔离、权限控制，支持联邦集群环境下的应用路由。
 */
public class WebAppProxy extends AbstractService {
  public static final String FETCHER_ATTRIBUTE= "AppUrlFetcher";
  public static final String IS_SECURITY_ENABLED_ATTRIBUTE = "IsSecurityEnabled";
  public static final String PROXY_HOST_ATTRIBUTE = "proxyHost";
  public static final String PROXY_CA = "ProxyCA";
  private static final Logger LOG = LoggerFactory.getLogger(
      WebAppProxy.class);
  
  private HttpServer2 proxyServer = null;
  private String bindAddress = null;
  private int port = 0;
  private AccessControlList acl = null;
  private AppReportFetcher fetcher = null;
  private boolean isSecurityEnabled = false;
  private String proxyHost = null;
  
  /**
   * 构造WebAppProxy代理服务实例
   */
  public WebAppProxy() {
    super(WebAppProxy.class.getName());
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 获取安全认证配置
    String auth =  conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
    // 判断是否开启安全认证
    if (auth == null || "simple".equals(auth)) {
      isSecurityEnabled = false;
    } else if ("kerberos".equals(auth)) {
      isSecurityEnabled = true;
    } else {
      LOG.warn("Unrecognized attribute value for " +
          CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION +
          " of " + auth);
    }
    // 获取代理服务主机端口配置
    String proxy = WebAppUtils.getProxyHostAndPort(conf);
    String[] proxyParts = proxy.split(":");
    proxyHost = proxyParts[0];

    // 根据是否开启联邦集群选择对应的应用报告获取器
    if (HAUtil.isFederationEnabled(conf)) {
      fetcher = new FedAppReportFetcher(conf);
    } else {
      fetcher = new DefaultAppReportFetcher(conf);
    }
    // 获取代理服务绑定地址配置
    bindAddress = conf.get(YarnConfiguration.PROXY_ADDRESS);
    // 绑定地址未配置则抛出异常终止启动
    if(bindAddress == null || bindAddress.isEmpty()) {
      throw new YarnRuntimeException(YarnConfiguration.PROXY_ADDRESS +
          " is not set so the proxy will not run.");
    }

    // 解析绑定地址中的主机和端口
    String[] parts = StringUtils.split(bindAddress, ':');
    port = 0;
    if (parts.length == 2) {
      bindAddress = parts[0];
      port = Integer.parseInt(parts[1]);
    }

    // 使用单独配置的绑定主机覆盖原地址中的主机
    String bindHost = conf.getTrimmed(YarnConfiguration.PROXY_BIND_HOST, null);
    if (bindHost != null) {
      LOG.debug("{} is set, will be used to run proxy.",
          YarnConfiguration.PROXY_BIND_HOST);
      bindAddress = bindHost;
    }

    LOG.info("Instantiating Proxy at {}:{}", bindAddress, port);

    // 初始化YARN管理员访问控制列表
    acl = new AccessControlList(conf.get(YarnConfiguration.YARN_ADMIN_ACL, 
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    super.serviceInit(conf);
  }
  
  @Override
  protected void serviceStart() throws Exception {
    try {
      Configuration conf = getConfig();
      // 构建HTTP代理服务器实例
      HttpServer2.Builder b = new HttpServer2.Builder()
          .setName("proxy")
          .addEndpoint(
              URI.create(WebAppUtils.getHttpSchemePrefix(conf) + bindAddress
                  + ":" + port)).setFindPort(port == 0).setConf(getConfig())
          .setACL(acl);
      // 如果启用HTTPS则加载SSL配置
      if (YarnConfiguration.useHttps(conf)) {
        WebAppUtils.loadSslConfiguration(b);
      }
      proxyServer = b.build();
      // 注册代理Servlet处理请求
      proxyServer.addServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
          ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
      // 将共享属性设置到Servlet上下文
      proxyServer.setAttribute(FETCHER_ATTRIBUTE, fetcher);
      proxyServer
          .setAttribute(IS_SECURITY_ENABLED_ATTRIBUTE, isSecurityEnabled);
      proxyServer.setAttribute(PROXY_HOST_ATTRIBUTE, proxyHost);
      // 启动HTTP代理服务器
      proxyServer.start();
    } catch (IOException e) {
      LOG.error("Could not start proxy web server",e);
      throw e;
    }
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    // 停止代理HTTP服务器
    if(proxyServer != null) {
      try {
        proxyServer.stop();
      } catch (Exception e) {
        LOG.error("Error stopping proxy web server", e);
        throw new YarnRuntimeException("Error stopping proxy web server",e);
      }
    }
    // 停止应用报告获取器
    if(this.fetcher != null) {
      this.fetcher.stop();
    }
    super.serviceStop();
  }

  /**
   * 等待代理服务器线程终止
   */
  public void join() {
    if(proxyServer != null) {
      try {
        proxyServer.join();
      } catch (InterruptedException e) {
        // ignored
      }
    }
  }

  @VisibleForTesting
  String getBindAddress() {
    return bindAddress + ":" + port;
  }

  @VisibleForTesting
  public AppReportFetcher getFetcher() {
    return fetcher;
  }
}