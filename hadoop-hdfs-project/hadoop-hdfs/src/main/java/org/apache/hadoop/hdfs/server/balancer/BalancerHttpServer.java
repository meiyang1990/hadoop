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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;

/**
 * HDFS数据均衡Balancer进程的内嵌HTTP服务端，提供Balancer的Web监控端点
 * 用于对外暴露均衡进程状态信息，支持HTTP/HTTPS两种访问方式
 */
public class BalancerHttpServer {

  /** Balancer实例在HttpServer中的属性键，供Web页面获取当前均衡实例 */
  private static final String BALANCER_ATTRIBUTE_KEY = "current.balancer";

  private final Configuration conf;
  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private HttpServer2 httpServer;

  /**
   * 构造BalancerHTTP服务端实例
   * @param conf Hadoop配置对象
   */
  public BalancerHttpServer(Configuration conf) {
    this.conf = conf;
  }

  /**
   * 启动BalancerHTTP服务，完成地址绑定、安全配置、服务初始化
   * @throws IOException 如果服务启动失败抛出异常
   */
  public void start() throws IOException {
    String webApp = "balancer";
    // 从配置中读取HTTP绑定地址
    httpAddress = conf.getSocketAddr(DFSConfigKeys.DFS_BALANCER_HTTP_BIND_HOST_KEY,
        DFSConfigKeys.DFS_BALANCER_HTTP_ADDRESS_KEY,
        DFSConfigKeys.DFS_BALANCER_HTTP_ADDRESS_DEFAULT,
        DFSConfigKeys.DFS_BALANCER_HTTP_PORT_DEFAULT);

    // 从配置中读取HTTPS绑定地址
    httpsAddress = conf.getSocketAddr(DFSConfigKeys.DFS_BALANCER_HTTPS_BIND_HOST_KEY,
        DFSConfigKeys.DFS_BALANCER_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_BALANCER_HTTPS_ADDRESS_DEFAULT,
        DFSConfigKeys.DFS_BALANCER_HTTPS_PORT_DEFAULT);

    // 获取预配置的HTTP服务构建器，包含Kerberos认证配置
    HttpServer2.Builder builder =
        DFSUtil.getHttpServerTemplate(conf, httpAddress, httpsAddress, webApp,
            DFSConfigKeys.DFS_BALANCER_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
            DFSConfigKeys.DFS_BALANCER_KEYTAB_FILE_KEY);

    // 读取X-Frame-Options配置，防止点击劫持攻击
    final boolean xFrameEnabled = conf.getBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    // 获取X-Frame-Options配置值
    final String xFrameOptionValue = conf.getTrimmed(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    // 配置X-Frame-Options安全响应头
    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

    // 构建并启动HTTP服务
    httpServer = builder.build();
    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    httpServer.start();

    // 根据HTTP策略更新实际绑定地址到配置中
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      if (httpAddress != null) {
        conf.set(DFSConfigKeys.DFS_BALANCER_HTTP_ADDRESS_KEY,
            NetUtils.getHostPortString(httpAddress));
      }
    }
    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      if (httpsAddress != null) {
        conf.set(DFSConfigKeys.DFS_BALANCER_HTTPS_ADDRESS_KEY,
            NetUtils.getHostPortString(httpsAddress));
      }
    }
  }

  /**
   * 将当前Balancer实例注入到HTTP服务属性中，供Web页面访问
   * @param balancer Balancer均衡实例对象
   */
  public void setBalancerAttribute(Balancer balancer) {
    httpServer.setAttribute(BALANCER_ATTRIBUTE_KEY, balancer);
  }

  /**
   * 停止HTTP服务，释放端口资源
   * @throws IOException 如果停止过程发生异常抛出
   */
  public void stop() throws IOException {
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * 获取HTTP服务实际绑定地址
   * @return HTTP服务绑定地址
   */
  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  /**
   * 获取HTTPS服务实际绑定地址
   * @return HTTPS服务绑定地址
   */
  public InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }
}