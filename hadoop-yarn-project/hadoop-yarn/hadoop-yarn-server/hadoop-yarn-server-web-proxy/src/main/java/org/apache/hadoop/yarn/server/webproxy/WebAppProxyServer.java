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
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Web应用代理服务端，位于终端用户和YARN ApplicationMaster的Web界面之间，
 * 负责转发用户请求到对应的ApplicationMaster，实现YARN集群Web服务的反向代理。
 */
public class WebAppProxyServer extends CompositeService {

  /**
   * 关闭钩子优先级，与ResourceManager保持一致。
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Logger LOG = LoggerFactory.getLogger(
      WebAppProxyServer.class);

  private WebAppProxy proxy = null;

  private JvmPauseMonitor pauseMonitor;

  /**
   * 构造WebAppProxyServer实例。
   */
  public WebAppProxyServer() {
    super(WebAppProxyServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 使用YarnConfiguration包装传入配置，加载YARN默认配置
    Configuration config = new YarnConfiguration(conf);
    // 安全环境下完成Kerberos登录
    doSecureLogin(conf);
    // 创建Web应用代理实例
    proxy = new WebAppProxy();
    // 将代理服务添加到复合服务管理
    addService(proxy);

    // 初始化指标系统
    DefaultMetricsSystem.initialize("WebAppProxyServer");
    // 初始化JVM指标采集
    JvmMetrics jm = JvmMetrics.initSingleton("WebAppProxyServer", null);
    // 创建JVM暂停监控器
    pauseMonitor = new JvmPauseMonitor();
    // 将监控服务添加到复合服务管理
    addService(pauseMonitor);
    // 关联暂停监控到JVM指标
    jm.setPauseMonitor(pauseMonitor);

    super.serviceInit(config);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    // 关闭指标系统
    DefaultMetricsSystem.shutdown();
  }

  /**
   * 使用配置中指定的Kerberos主体完成代理服务安全登录。
   * @param conf 包含认证信息的配置对象
   * @throws IOException 登录失败时抛出IO异常
   */
  protected void doSecureLogin(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getBindAddress(conf);  
    // 从配置获取keytab和主体信息，完成Kerberos登录
    SecurityUtil.login(conf, YarnConfiguration.PROXY_KEYTAB,
        YarnConfiguration.PROXY_PRINCIPAL, socAddr.getHostName());
  }

  /**
   * 从配置中读取并构造代理服务绑定地址。
   *
   * @param conf 配置对象
   * @return 绑定地址
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(
        YarnConfiguration.PROXY_BIND_HOST,
        YarnConfiguration.PROXY_ADDRESS,
        YarnConfiguration.DEFAULT_PROXY_ADDRESS,
        YarnConfiguration.DEFAULT_PROXY_PORT);
  }

  /**
   * WebAppProxy服务启动入口方法。
   * @param args 启动参数
   */
  public static void main(String[] args) {
    // 设置默认未捕获异常处理器
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    // 打印启动日志信息
    StringUtils.startupShutdownMessage(WebAppProxyServer.class, args, LOG);
    try {
      // 创建YARN配置对象
      YarnConfiguration configuration = new YarnConfiguration();
      // 解析通用命令行参数
      new GenericOptionsParser(configuration, args);
      // 启动代理服务
      WebAppProxyServer proxyServer = startServer(configuration);
      // 阻塞等待代理服务结束
      proxyServer.proxy.join();
    } catch (Throwable t) {
      // 异常情况下退出进程
      ExitUtil.terminate(-1, t);
    }
  }

  /**
   * 初始化并启动Web应用代理服务。
   * 
   * @param configuration 服务配置
   * @return 启动完成的代理服务实例
   */
  protected static WebAppProxyServer startServer(Configuration configuration)
      throws Exception {
    // 创建代理服务实例
    WebAppProxyServer proxy = new WebAppProxyServer();
    // 注册JVM关闭钩子，确保服务正常关闭
    ShutdownHookManager.get().addShutdownHook(
        new CompositeServiceShutdownHook(proxy), SHUTDOWN_HOOK_PRIORITY);
    // 初始化服务
    proxy.init(configuration);
    // 启动服务
    proxy.start();
    return proxy;
  }

}