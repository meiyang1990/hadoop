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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilterInitializer;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.timelineservice.writer.TimelineEntitySetWriter;
import org.apache.hadoop.yarn.api.records.timelineservice.writer.TimelineEntityWriter;
import org.apache.hadoop.yarn.api.records.timelineservice.writer.TimelineHealthWriter;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.timelineservice.reader.security.TimelineReaderAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.timelineservice.reader.security.TimelineReaderWhitelistAuthorizationFilterInitializer;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.server.util.timeline.TimelineServerUtils;
import org.apache.hadoop.yarn.server.webapp.LogWebService;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.classification.VisibleForTesting;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timeline V2 时间线服务读取服务器主类，提供REST API读取存储的时间线数据。
 * 属于YARN TimelineServiceV2模块，负责响应用户/客户端的时间线数据查询请求。
 */
@Private
@Unstable
public class TimelineReaderServer extends CompositeService {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineReaderServer.class);
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;
  static final String TIMELINE_READER_MANAGER_ATTR =
      "timeline.reader.manager";

  private HttpServer2 readerWebServer;
  private TimelineReaderManager timelineReaderManager;
  private String webAppURLWithoutScheme;


  public TimelineReaderServer() {
    super(TimelineReaderServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 检查Timeline V2是否启用，未启用则抛出异常初始化失败
    if (!YarnConfiguration.timelineServiceV2Enabled(conf)) {
      throw new YarnException("timeline service v.2 is not enabled");
    }
    // 获取不包含协议前缀的Timeline Reader Web服务地址
    webAppURLWithoutScheme =
        WebAppUtils.getTimelineReaderWebAppURLWithoutScheme(conf);
    // 解析地址得到绑定Socket地址
    InetSocketAddress bindAddr =
        NetUtils.createSocketAddr(webAppURLWithoutScheme);
    // 安全启用时，通过keytab登录获取凭证
    try {
      SecurityUtil.login(conf, YarnConfiguration.TIMELINE_SERVICE_KEYTAB,
          YarnConfiguration.TIMELINE_SERVICE_PRINCIPAL, bindAddr.getHostName());
    } catch(IOException e) {
      throw new YarnRuntimeException("Failed to login from keytab", e);
    }

    // 创建时间线数据读取存储实例
    TimelineReader timelineReaderStore = createTimelineReaderStore(conf);
    // 初始化存储
    timelineReaderStore.init(conf);
    // 将存储服务添加到复合服务管理
    addService(timelineReaderStore);
    // 创建时间线读取管理器
    timelineReaderManager = createTimelineReaderManager(timelineReaderStore);
    // 将读取管理器添加到复合服务管理
    addService(timelineReaderManager);
    super.serviceInit(conf);
  }

  /**
   * 根据配置反射创建TimelineReader存储实现实例。
   * @param conf 配置对象
   * @return 反射创建的TimelineReader实例
   */
  private TimelineReader createTimelineReaderStore(final Configuration conf) {
    String timelineReaderClassName = conf.get(
        YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_READER_CLASS);
    LOG.info("Using store: {}", timelineReaderClassName);
    try {
      Class<?> timelineReaderClazz = Class.forName(timelineReaderClassName);
      if (TimelineReader.class.isAssignableFrom(timelineReaderClazz)) {
        return (TimelineReader) ReflectionUtils.newInstance(
            timelineReaderClazz, conf);
      } else {
        throw new YarnRuntimeException("Class: " + timelineReaderClassName
            + " not instance of " + TimelineReader.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate TimelineReader: "
          + timelineReaderClassName, e);
    }
  }


  /**
   * 创建TimelineReaderManager实例，封装底层存储。
   * @param timelineReaderStore 时间线读取存储实例
   * @return TimelineReaderManager实例
   */
  private TimelineReaderManager createTimelineReaderManager(
      TimelineReader timelineReaderStore) {
    return new TimelineReaderManager(timelineReaderStore);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    // 启动Timeline Reader Web服务
    startTimelineReaderWebApp();
  }

  /**
   * 阻塞主线程保持服务运行，直到收到停止信号。
   */
  private void join() {
    // keep the main thread that started the server up until it receives a stop
    // signal
    if (readerWebServer != null) {
      try {
        readerWebServer.join();
      } catch (InterruptedException ignore) {}
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (readerWebServer != null) {
      readerWebServer.stop();
    }
    super.serviceStop();
  }

  /**
   * 添加Timeline Reader所需的过滤器：跨域、认证、授权。
   * @param conf 配置对象
   */
  protected void addFilters(Configuration conf) {
    boolean enableCorsFilter = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED,
        YarnConfiguration.TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED_DEFAULT);
    // 如果开启CORS，配置跨域过滤器
    if (enableCorsFilter) {
      conf.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }
    String initializers = conf.get("hadoop.http.filter.initializers", "");
    Set<String> defaultInitializers = new LinkedHashSet<String>();
    // 没有配置代理用户过滤器的情况下，添加Timeline认证过滤器
    if (!initializers.contains(
        ProxyUserAuthenticationFilterInitializer.class.getName())) {
      if (!initializers.contains(
          TimelineReaderAuthenticationFilterInitializer.class.getName())) {
        defaultInitializers.add(
            TimelineReaderAuthenticationFilterInitializer.class.getName());
      }
    } else {
      // 已经配置代理用户过滤器则使用它
      defaultInitializers.add(
          ProxyUserAuthenticationFilterInitializer.class.getName());
    }

    // 添加白名单授权过滤器
    defaultInitializers.add(
        TimelineReaderWhitelistAuthorizationFilterInitializer.class.getName());

    // 将默认过滤器合并到现有过滤器配置中
    TimelineServerUtils.setTimelineFilters(
        conf, initializers, defaultInitializers);
  }

  /**
   * 启动Timeline Reader HTTP/Web服务，绑定端口注册资源。
   */
  private void startTimelineReaderWebApp() {
    Configuration conf = getConfig();
    // 添加过滤器配置
    addFilters(conf);

    // 获取绑定主机配置，未配置Reader绑定时回退到全局Timeline绑定地址
    String hostProperty = YarnConfiguration.TIMELINE_SERVICE_READER_BIND_HOST;
    String host = conf.getTrimmed(hostProperty);
    if (host == null || host.isEmpty()) {
      // if reader bind-host is not set, fall back to timeline-service.bind-host
      // to maintain compatibility
      hostProperty = YarnConfiguration.TIMELINE_SERVICE_BIND_HOST;
    }
    // 获取最终绑定地址
    String bindAddress = WebAppUtils
        .getWebAppBindURL(conf, hostProperty, webAppURLWithoutScheme);

    LOG.info("Instantiating TimelineReaderWebApp at {}", bindAddress);
    try {

      // 获取HTTP协议(http/https)前缀
      String httpScheme = WebAppUtils.getHttpSchemePrefix(conf);

      // 构建HttpServer2实例
      HttpServer2.Builder builder = new HttpServer2.Builder()
            .setName("timeline")
            .setConf(conf)
            .addEndpoint(URI.create(httpScheme + bindAddress));

      // HTTPS协议下加载SSL配置
      if (httpScheme.equals(WebAppUtils.HTTPS_PREFIX)) {
        WebAppUtils.loadSslConfiguration(builder, conf);
      }
      // 构建服务器实例
      readerWebServer = builder.build();
      // 注册Jersey资源配置，处理REST请求
      readerWebServer.addJerseyResourceConfig(configure(), "/*", null);
      // 将读取管理器存入Servlet上下文供REST接口使用
      readerWebServer.setAttribute(TIMELINE_READER_MANAGER_ATTR,
          timelineReaderManager);
      // 启动服务器
      readerWebServer.start();
    } catch (Exception e) {
      String msg = "TimelineReaderWebApp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }

  @VisibleForTesting
  public int getWebServerPort() {
    return readerWebServer.getConnectorAddress(0).getPort();
  }

  /**
   * 启动Timeline Reader服务器，完成初始化和启动流程，注册关闭钩子。
   * @param args 启动参数
   * @param conf 配置对象
   * @return 启动完成的TimelineReaderServer实例
   */
  static TimelineReaderServer startTimelineReaderServer(String[] args,
      Configuration conf) {
    Thread.setDefaultUncaughtExceptionHandler(
        new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(TimelineReaderServer.class,
        args, LOG);
    TimelineReaderServer timelineReaderServer = null;
    try {
      timelineReaderServer = new TimelineReaderServer();
      // 注册JVM关闭钩子，优雅关闭服务
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(timelineReaderServer),
          SHUTDOWN_HOOK_PRIORITY);
      // 初始化服务
      timelineReaderServer.init(conf);
      // 启动服务
      timelineReaderServer.start();
    } catch (Throwable t) {
      LOG.error("Error starting TimelineReaderWebServer", t);
      ExitUtil.terminate(-1, "Error starting TimelineReaderWebServer");
    }
    return timelineReaderServer;
  }

  /**
   * 配置Jersey REST资源，注册所有需要暴露的REST端点和提供者。
   * @return 配置完成的ResourceConfig对象
   */
  protected static ResourceConfig configure() {
    ResourceConfig config = new ResourceConfig();
    // 扫描读取服务包下的资源
    config.packages("org.apache.hadoop.yarn.server.timelineservice.reader");
    // 扫描写入API包下的资源
    config.packages("org.apache.hadoop.yarn.api.records.writer");
    // 注册日志查询Web服务
    config.register(LogWebService.class);
    // 注册通用异常处理器
    config.register(GenericExceptionHandler.class);
    // 注册Timeline读取Web服务
    config.register(TimelineReaderWebServices.class);
    // 注册时间线实体集合写入器
    config.register(TimelineEntitySetWriter.class);
    // 注册时间线实体写入器
    config.register(TimelineEntityWriter.class);
    // 注册健康检查写入器
    config.register(TimelineHealthWriter.class);
    // 注册JSON处理组件
    config.register(new JettisonFeature()).register(YarnJacksonJaxbJsonProvider.class);
    return config;
  }

  /**
   * 主方法，启动Timeline Reader服务器。
   * @param args 启动参数
   */
  public static void main(String[] args) {
    Configuration conf = new YarnConfiguration();
    // 强制开启Timeline服务和V2版本
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSIONS, 2.0f);
    // 启动服务器
    TimelineReaderServer server = startTimelineReaderServer(args, conf);
    // 阻塞主线程保持运行
    server.join();
  }
}