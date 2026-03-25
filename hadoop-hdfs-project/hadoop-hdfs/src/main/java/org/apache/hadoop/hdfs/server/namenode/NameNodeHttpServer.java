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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_REST_CSRF_ENABLED_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.HashMap;

import javax.servlet.ServletContext;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.TokenVerifier;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.web.ParamFilter;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.AclPermissionParam;
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.hdfs.web.resources.UserProvider;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.http.RestCsrfPreventionFilter;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件: NameNodeHttpServer.java
 * 所属模块: HDFS NameNode 服务端
 * 核心职责: 封装NameNode启动的HTTP服务器，提供WebHDFS REST API和NameNode监控Web服务
 * 主要功能: 管理HTTP/HTTPS服务器生命周期，初始化WebHDFS REST接口，注册各类管理Servlet
 */
/**
 * Encapsulates the HTTP server started by the NameNode. 
 */
@InterfaceAudience.Private
public class NameNodeHttpServer {

  private static final Logger LOG = LoggerFactory.getLogger(NameNodeHttpServer.class);

  private HttpServer2 httpServer;
  private final Configuration conf;
  private final NameNode nn;
  
  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private final InetSocketAddress bindAddress;
  
  // Servlet上下文属性键：NameNode服务地址
  public static final String NAMENODE_ADDRESS_ATTRIBUTE_KEY = "name.node.address";
  // Servlet上下文属性键：FSImage对象
  public static final String FSIMAGE_ATTRIBUTE_KEY = "name.system.image";
  // Servlet上下文属性键：NameNode对象
  protected static final String NAMENODE_ATTRIBUTE_KEY = "name.node";
  // Servlet上下文属性键：启动进度对象
  public static final String STARTUP_PROGRESS_ATTRIBUTE_KEY = "startup.progress";
  // Servlet上下文属性键：别名映射对象
  public static final String ALIASMAP_ATTRIBUTE_KEY = "name.system.aliasmap";

  /**
   * 构造NameNodeHttpServer实例
   * @param conf Hadoop配置对象
   * @param nn 所属NameNode实例
   * @param bindAddress HTTP服务绑定地址
   */
  NameNodeHttpServer(Configuration conf, NameNode nn,
      InetSocketAddress bindAddress) {
    this.conf = conf;
    this.nn = nn;
    this.bindAddress = bindAddress;
  }

  /**
   * 初始化WebHDFS REST接口，配置相关过滤器和Jersey资源
   * @param conf Hadoop配置对象
   * @param httpServer2 HTTP服务器实例
   * @param jerseyResourcePackage Jersey资源包路径
   * @throws IOException 初始化失败抛出IO异常
   */
  public static void initWebHdfs(Configuration conf, HttpServer2 httpServer2,
      String jerseyResourcePackage) throws IOException {
    // 从配置加载用户名正则表达式模式
    UserParam.setUserPattern(conf.get(
        HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_KEY,
        HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT));
    // 从配置加载ACL权限正则表达式模式
    AclPermissionParam.setAclPermissionPattern(conf.get(
        HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY,
        HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT));

    final String pathSpec = WebHdfsFileSystem.PATH_PREFIX + "/*";

    // 添加REST CSRF防护过滤器（如果开启）
    if (conf.getBoolean(DFS_WEBHDFS_REST_CSRF_ENABLED_KEY,
        DFS_WEBHDFS_REST_CSRF_ENABLED_DEFAULT)) {
      Map<String, String> restCsrfParams = RestCsrfPreventionFilter
          .getFilterParams(conf, "dfs.webhdfs.rest-csrf.");
      String restCsrfClassName = RestCsrfPreventionFilter.class.getName();
      HttpServer2.defineFilter(httpServer2.getWebAppContext(),
          restCsrfClassName, restCsrfClassName, restCsrfParams,
          new String[] {pathSpec});
    }

    // 添加参数名称转小写过滤器，兼容大小写不敏感的参数请求
    HttpServer2.defineFilter(httpServer2.getWebAppContext(),
        ParamFilter.class.getName(), ParamFilter.class.getName(), null,
        new String[] {pathSpec});

    // 注册Jersey资源配置
    final Map<String, String> params = new HashMap<>();
    ResourceConfig config = new ResourceConfig();
    config.register(ExceptionHandler.class);
    config.packages(jerseyResourcePackage, Param.class.getPackage().getName());
    // 注册用户信息绑定工厂，为WebHDFS接口注入当前请求用户信息
    config.register(new AbstractBinder() {
      // add a factory to generate UserGroupInformation
      @Override
      protected void configure() {
        bindFactory(UserProvider.class).to(UserGroupInformation.class);
      }
    });
    httpServer2.addJerseyResourceConfig(config, pathSpec, params);
  }

  /**
   * @see DFSUtil#getHttpPolicy(org.apache.hadoop.conf.Configuration)
   * for information related to the different configuration options and
   * Http Policy is decided.
   */
  /**
   * 启动HTTP/HTTPS服务器，完成地址绑定、过滤器和Servlet注册
   * @throws IOException 启动失败抛出IO异常
   */
  void start() throws IOException {
    // 获取HTTP/HTTPS策略配置
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    final String infoHost = bindAddress.getHostName();

    final InetSocketAddress httpAddr = bindAddress;
    // 从配置读取HTTPS地址
    final String httpsAddrString = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    if (httpsAddr != null) {
      // 如果配置了单独的HTTPS绑定主机，覆盖原有主机名
      final String bindHost =
          conf.getTrimmed(DFSConfigKeys.DFS_NAMENODE_HTTPS_BIND_HOST_KEY);
      if (bindHost != null && !bindHost.isEmpty()) {
        httpsAddr = new InetSocketAddress(bindHost, httpsAddr.getPort());
      }
    }

    // 创建HTTP服务器构建器，初始化安全配置
    HttpServer2.Builder builder = DFSUtil.getHttpServerTemplate(conf,
        httpAddr, httpsAddr, "hdfs",
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY);

    // 配置X-Frame-Options防点击劫持
    final boolean xFrameEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    final String xFrameOptionValue = conf.getTrimmed(
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

    // 构建HTTP服务器实例
    httpServer = builder.build();

    // 如果HTTPS开启，设置DataNode HTTPS默认端口到上下文
    if (policy.isHttpsEnabled()) {
      // 假设所有DataNode使用相同的SSL端口
      InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.getTrimmed(
          DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, infoHost + ":"
              + DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT));
      httpServer.setAttribute(DFSConfigKeys.DFS_DATANODE_HTTPS_PORT_KEY,
          datanodeSslPort.getPort());
    }

    // 初始化WebHDFS REST接口
    initWebHdfs(conf, httpServer, NamenodeWebHdfsMethods.class.getPackage().getName());

    // 设置Servlet上下文共享属性
    httpServer.setAttribute(NAMENODE_ATTRIBUTE_KEY, nn);
    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    // 注册管理用Servlet
    setupServlets(httpServer);
    // 启动HTTP服务器
    httpServer.start();

    int connIdx = 0;
    // 保存HTTP服务绑定地址到配置
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      if (httpAddress != null) {
        conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY,
            NetUtils.getHostPortString(httpAddress));
        LOG.info("Listening for HTTP traffic on {}", httpAddress);
      }
    }

    // 保存HTTPS服务绑定地址到配置
    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      if (httpsAddress != null) {
        conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY,
            NetUtils.getHostPortString(httpsAddress));
        LOG.info("Listening for HTTPS traffic on {}", httpsAddress);
      }
    }
  }

  /**
   * 阻塞等待HTTP服务器终止
   * @throws InterruptedException 等待过程被中断抛出异常
   */
  public void join() throws InterruptedException {
    if (httpServer != null) {
      httpServer.join();
    }
  }

  /**
   * 停止HTTP服务器，释放资源
   * @throws Exception 停止过程中发生异常
   */
  void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  /**
   * 获取HTTP服务绑定地址
   * @return HTTP服务地址
   */
  InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  /**
   * 获取HTTPS服务绑定地址
   * @return HTTPS服务地址
   */
  InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  /**
   * 设置FSImage对象到Servlet上下文，供Web界面访问
   * @param fsImage FSImage实例
   */
  void setFSImage(FSImage fsImage) {
    httpServer.setAttribute(FSIMAGE_ATTRIBUTE_KEY, fsImage);
  }

  /**
   * 设置NameNode服务地址到Servlet上下文，供Web界面访问
   * @param nameNodeAddress NameNode服务地址
   */
  void setNameNodeAddress(InetSocketAddress nameNodeAddress) {
    httpServer.setAttribute(NAMENODE_ADDRESS_ATTRIBUTE_KEY,
        NetUtils.getConnectAddress(nameNodeAddress));
  }

  /**
   * 设置NameNode启动进度到Servlet上下文，供启动进度页面展示
   * @param prog 启动进度对象
   */
  void setStartupProgress(StartupProgress prog) {
    httpServer.setAttribute(STARTUP_PROGRESS_ATTRIBUTE_KEY, prog);
  }

  /**
   * 设置别名映射对象到Servlet上下文，供Web界面访问
   * @param aliasMap 内存别名映射实例
   */
  void setAliasMap(InMemoryAliasMap aliasMap) {
    httpServer.setAttribute(ALIASMAP_ATTRIBUTE_KEY, aliasMap);
  }

  /**
   * 注册NameNode管理相关Servlet到HTTP服务器
   * @param httpServer HTTP服务器实例
   */
  private static void setupServlets(HttpServer2 httpServer) {
    httpServer.addInternalServlet("startupProgress",
        StartupProgressServlet.PATH_SPEC, StartupProgressServlet.class);
    httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class,
        true);
    httpServer.addInternalServlet("imagetransfer", ImageServlet.PATH_SPEC,
        ImageServlet.class, true);
    httpServer.addInternalServlet(IsNameNodeActiveServlet.SERVLET_NAME,
        IsNameNodeActiveServlet.PATH_SPEC,
        IsNameNodeActiveServlet.class);
    httpServer.addInternalServlet(NetworkTopologyServlet.SERVLET_NAME,
        NetworkTopologyServlet.PATH_SPEC, NetworkTopologyServlet.class);
  }

  /**
   * 从Servlet上下文获取FSImage实例
   * @param context Servlet上下文对象
   * @return FSImage实例
   */
  static FSImage getFsImageFromContext(ServletContext context) {
    return (FSImage)context.getAttribute(FSIMAGE_ATTRIBUTE_KEY);
  }

  /**
   * 从Servlet上下文获取NameNode实例
   * @param context Servlet上下文对象
   * @return NameNode实例
   */
  public static NameNode getNameNodeFromContext(ServletContext context) {
    return (NameNode)context.getAttribute(NAMENODE_ATTRIBUTE_KEY);
  }

  /**
   * 从Servlet上下文获取Token验证器
   * @param context Servlet上下文对象
   * @return Token验证器实例
   */
  public static TokenVerifier
      getTokenVerifierFromContext(ServletContext context) {
    return (TokenVerifier) context.getAttribute(NAMENODE_ATTRIBUTE_KEY);
  }

  /**
   * 从Servlet上下文获取Hadoop配置对象
   * @param context Servlet上下文对象
   * @return Configuration配置实例
   */
  static Configuration getConfFromContext(ServletContext context) {
    return (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
  }

  /**
   * 从Servlet上下文获取别名映射实例
   * @param context Servlet上下文对象
   * @return 内存别名映射实例
   */
  static InMemoryAliasMap getAliasMapFromContext(ServletContext context) {
    return (InMemoryAliasMap) context.getAttribute(ALIASMAP_ATTRIBUTE_KEY);
  }

  /**
   * 从Servlet上下文获取NameNode服务地址
   * @param context Servlet上下文对象
   * @return NameNode服务地址
   */
  public static InetSocketAddress getNameNodeAddressFromContext(
      ServletContext context) {
    return (InetSocketAddress)context.getAttribute(
        NAMENODE_ADDRESS_ATTRIBUTE_KEY);
  }

  /**
   * 从Servlet上下文获取NameNode启动进度对象
   * @param context Servlet上下文对象
   * @return 启动进度对象
   */
  static StartupProgress getStartupProgressFromContext(
      ServletContext context) {
    return (StartupProgress)context.getAttribute(STARTUP_PROGRESS_ATTRIBUTE_KEY);
  }

  /**
   * 从Servlet上下文获取NameNode HA服务状态
   * @param context Servlet上下文对象
   * @return HA服务状态
   */
  public static HAServiceProtocol.HAServiceState getNameNodeStateFromContext(ServletContext context) {
    return getNameNodeFromContext(context).getServiceState();
  }

  /**
   * 获取底层HTTP服务器实例，仅用于测试
   * @return HttpServer2实例
   */
  @VisibleForTesting
  public HttpServer2 getHttpServer() {
    return httpServer;
  }
}