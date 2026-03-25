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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import javax.servlet.ServletContext;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;

/**
 * @file org/apache/hadoop/hdfs/qjournal/server/JournalNodeHttpServer.java
 * @brief QJM日志节点的HTTP服务封装类，提供外部访问日志节点的HTTP接口能力
 * 
 * 该类属于HDFS QJM（共享编辑日志）模块，封装了JournalNode启动和管理HTTP服务的完整逻辑，
 * 支持NameNode拉取日志元数据等操作，同时支持HTTP/HTTPS双协议配置。
 */
@InterfaceAudience.Private
public class JournalNodeHttpServer {
  /** Servlet上下文属性键，用于存储本地JournalNode实例 */
  public static final String JN_ATTRIBUTE_KEY = "localjournal";

  private HttpServer2 httpServer;
  private final JournalNode localJournalNode;

  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private final InetSocketAddress bindAddress;

  private final Configuration conf;

  /**
   * 构造JournalNode HTTP服务实例
   * @param conf Hadoop配置对象
   * @param jn 所属的JournalNode实例
   * @param bindAddress 服务绑定地址
   */
  JournalNodeHttpServer(Configuration conf, JournalNode jn,
      InetSocketAddress bindAddress) {
    this.conf = conf;
    this.localJournalNode = jn;
    this.bindAddress = bindAddress;
  }

  /**
   * 启动HTTP服务，完成地址绑定、Servlet注册和服务启动
   * @throws IOException 启动失败时抛出IO异常
   */
  void start() throws IOException {
    final InetSocketAddress httpAddr = bindAddress;

    // 从配置中读取HTTPS地址
    final String httpsAddrString = conf.get(
        DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    if (httpsAddr != null) {
      // 如果配置了单独的HTTPS绑定主机，覆盖原有地址的主机部分
      final String bindHost =
          conf.getTrimmed(DFSConfigKeys.DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY);
      if (bindHost != null && !bindHost.isEmpty()) {
        httpsAddr = new InetSocketAddress(bindHost, httpsAddr.getPort());
      }
    }

    // 构建HTTP服务构建器，配置安全认证信息
    HttpServer2.Builder builder = DFSUtil.getHttpServerTemplate(conf,
        httpAddr, httpsAddr, "journal",
        DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_KEYTAB_FILE_KEY);

    // 配置X-Frame-Options防止点击劫持
    final boolean xFrameEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    final String xFrameOptionValue = conf.getTrimmed(
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

    // 构建并启动服务，注册上下文属性和Servlet
    httpServer = builder.build();
    httpServer.setAttribute(JN_ATTRIBUTE_KEY, localJournalNode);
    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    httpServer.addInternalServlet("getJournal", "/getJournal",
        GetJournalEditServlet.class, true);
    httpServer.start();

    // 根据HTTP策略更新配置中的实际绑定地址
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY,
          NetUtils.getHostPortString(httpAddress));
    }

    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY,
          NetUtils.getHostPortString(httpsAddress));
    }
  }

  /**
   * 停止HTTP服务，释放绑定端口
   * @throws IOException 停止过程发生异常时抛出IO异常
   */
  void stop() throws IOException {
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * 获取运行服务绑定的实际地址，优先返回HTTP地址，不存在则返回HTTPS地址
   * @return 绑定的InetSocketAddress实例
   */
  public InetSocketAddress getAddress() {
    assert httpAddress != null || httpsAddress != null;
    return httpAddress != null ? httpAddress : httpsAddress;
  }
  
  /**
   * 获取运行服务绑定的HTTP实际地址
   * @return HTTP绑定地址
   */
  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  /**
   * 获取运行服务绑定的HTTPS实际地址
   * @return HTTPS绑定地址
   */
  public InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  /**
   * 获取当前HTTP服务的访问URI
   * @return 服务访问URI
   */
  URI getServerURI() {
    // 对于HTTPS_ONLY策略，第一个连接器就是HTTPS连接器，因此直接取第一个地址即可
    InetSocketAddress addr = httpServer.getConnectorAddress(0);
    return URI.create(DFSUtil.getHttpClientScheme(conf) + "://"
        + NetUtils.getHostPortString(addr));
  }

  /**
   * 从Servlet上下文中获取指定 journalId对应的Journal实例
   * @param context Servlet上下文对象
   * @param jid 日志ID
   * @return 对应Journal实例
   * @throws IOException 获取失败时抛出IO异常
   */
  public static Journal getJournalFromContext(ServletContext context, String jid)
      throws IOException {
    JournalNode jn = (JournalNode)context.getAttribute(JN_ATTRIBUTE_KEY);
    return jn.getOrCreateJournal(jid);
  }

  /**
   * 从Servlet上下文中获取Hadoop配置对象
   * @param context Servlet上下文对象
   * @return Hadoop配置对象
   */
  public static Configuration getConfFromContext(ServletContext context) {
    return (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
  }
}